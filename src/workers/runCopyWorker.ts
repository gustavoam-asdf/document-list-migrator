import { Readable, Writable } from "node:stream"
import { finished, pipeline } from "node:stream/promises"

import { LineGrouper } from "../stream/LineGrouper"
import { LineSplitter } from "../stream/LineSplitter"
import { RejectsWriter } from "../rejects/RejectsWriter"
import { TextDecoderStream } from "../stream/polifylls"
import { extractPgErrorDetails } from "../errors/pgErrorDetails"
import { isConnectionError } from "../errors/isConnectionError"
import { rejectsDir } from "../constants"
import { splitInParts } from "../shared/splitInParts"

const MIN_QUARTER_SIZE = 5
const CONNECTION_RETRY_MAX = 3
const CONNECTION_RETRY_DELAY_MS = 500

export type CopyWorkerProgress = {
	type: "progress"
	workerName: string
	rows: number
	rejected: number
	batchMs: number
}

export type CopyWorkerDone = {
	type: "done"
	workerName: string
	count: number
	rejected: number
}

type RunParams = {
	filePath: string
	useSecondaryDb: boolean
	workerName: string
	batchRows: number
	createCopyStream: () => Promise<Writable>
	postMessage: (msg: CopyWorkerProgress | CopyWorkerDone) => void
	// Si devuelve true entre batches, el worker drena el batch actual y sale limpio.
	shouldStop?: () => boolean
}

type QuarterContext = {
	createCopyStream: () => Promise<Writable>
	rejects: RejectsWriter
	workerName: string
	filePath: string
	batchIndex: number
	baseRowIndex: number // índice de la primera fila de `lines` dentro del batch original
}

// Al fondo del cuarteo probamos fila por fila para identificar la fila exacta
// que rompe el COPY. Las inocentes pasan, sólo las verdaderas culpables van a rejects.
async function isolateBadRows(
	lines: string[],
	lastError: Error,
	ctx: QuarterContext,
): Promise<{ inserted: number; rejected: number }> {
	let inserted = 0
	let rejected = 0
	const errorDetails = extractPgErrorDetails(lastError)

	for (let i = 0; i < lines.length; i++) {
		const row = lines[i]!
		const stream = await ctx.createCopyStream()
		try {
			await pipeline(Readable.from([row]), stream)
			inserted++
		} catch (rowError) {
			const rowDetails = extractPgErrorDetails(rowError)
			ctx.rejects.write({
				source: `copy:${ctx.workerName}`,
				rawLine: row.replace(/\n$/, ""),
				error: rowDetails.message,
				errorCode: rowDetails.code,
				errorDetail: rowDetails.detail,
				errorHint: rowDetails.hint,
				errorColumn: rowDetails.column,
				errorWhere: rowDetails.where,
				chunkFile: ctx.filePath,
				batchIndex: ctx.batchIndex,
				rowIndexInBatch: ctx.baseRowIndex + i,
			})
			rejected++
			console.error(
				`[${ctx.workerName}] bad row identified | batch ${ctx.batchIndex} pos ${ctx.baseRowIndex + i} ` +
				`| ${rowDetails.code ?? ""} ${rowDetails.message}`,
			)
		} finally {
			stream.removeAllListeners()
		}
	}

	if (rejected === 0) {
		// Ninguna fila individual falló, pero el batch como conjunto sí. Raro
		// (¿constraint violado por conjunto?). Logueamos todas con el error compartido.
		console.warn(
			`[${ctx.workerName}] all ${lines.length} rows passed individually but batch failed — ` +
			`logging all to rejects with shared error`,
		)
		for (let i = 0; i < lines.length; i++) {
			ctx.rejects.write({
				source: `copy:${ctx.workerName}`,
				rawLine: lines[i]!.replace(/\n$/, ""),
				error: errorDetails.message,
				errorCode: errorDetails.code,
				errorDetail: errorDetails.detail,
				chunkFile: ctx.filePath,
				batchIndex: ctx.batchIndex,
				rowIndexInBatch: ctx.baseRowIndex + i,
				note: "passed individually, failed in batch",
			})
		}
		return { inserted: 0, rejected: lines.length }
	}

	return { inserted, rejected }
}

async function retryWithQuartering(
	lines: string[],
	error: Error,
	ctx: QuarterContext,
): Promise<{ inserted: number; rejected: number }> {
	const partLength = Math.floor(lines.length / 4)

	if (partLength < MIN_QUARTER_SIZE) {
		return isolateBadRows(lines, error, ctx)
	}

	console.warn(`[${ctx.workerName}] quartering ${lines.length} rows after error: ${error.message}`)

	const parts = splitInParts({ values: lines, size: partLength })
	let inserted = 0
	let rejected = 0
	let offset = 0

	for (const part of parts) {
		const readable = Readable.from(part)
		const queryStream = await ctx.createCopyStream()
		try {
			await pipeline(readable, queryStream)
			inserted += part.length
		} catch (subError) {
			const sub = await retryWithQuartering(part, subError as Error, {
				...ctx,
				baseRowIndex: ctx.baseRowIndex + offset,
			})
			inserted += sub.inserted
			rejected += sub.rejected
		} finally {
			queryStream.removeAllListeners()
		}
		offset += part.length
	}

	return { inserted, rejected }
}

export async function runCopyWorker({
	filePath,
	useSecondaryDb,
	workerName,
	batchRows,
	createCopyStream,
	postMessage,
	shouldStop,
}: RunParams): Promise<void> {
	const phase = useSecondaryDb ? "secondary" : "primary"
	console.log(`[${workerName}] start file=${filePath} batchRows=${batchRows} phase=${phase}`)

	const file = Bun.file(filePath)
	const fileStream = file.stream()

	const decoderStream = new TextDecoderStream("utf-8")
	const lineTransformStream = new TransformStream(new LineSplitter())
	const lineGroupTransformStream = new TransformStream(new LineGrouper(batchRows))

	const linesStream = fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)

	const rejects = new RejectsWriter(`${rejectsDir}/copy-${phase}-${workerName}.jsonl`)

	let queryStream = await createCopyStream()
	let count = 0
	let totalRejected = 0
	let batchIndex = 0

	let drainedEarly = false
	for await (const lines of linesStream) {
		// Si nos pidieron drenar, salimos antes de procesar el siguiente batch.
		// El batch en vuelo no se interrumpe, lo cerramos limpio.
		if (shouldStop?.()) {
			console.warn(`[${workerName}] shutdown requested — draining after batch ${batchIndex}`)
			drainedEarly = true
			break
		}
		batchIndex++
		const batchStart = Date.now()
		// LineSplitter ya quitó el `\n` final; lo reagregamos para COPY.
		// Filtramos líneas vacías (pasan si el chunk file tuviera saltos dobles).
		const tsvLines: string[] = []
		for (const line of lines) {
			if (line.length === 0) continue
			tsvLines.push(line + "\n")
		}
		if (tsvLines.length === 0) {
			batchIndex--
			continue
		}

		let inserted = 0
		let rejected = 0

		try {
			await pipeline(Readable.from(tsvLines), queryStream, { end: false })
			inserted = tsvLines.length
		} catch (error) {
			if (isConnectionError(error)) {
				console.warn(`[${workerName}] connection error at batch ${batchIndex} — long-lived COPY died, recreating; data in stream is lost`)

				let attempt = 0
				let recovered = false
				while (attempt < CONNECTION_RETRY_MAX && !recovered) {
					attempt++
					await new Promise(r => setTimeout(r, CONNECTION_RETRY_DELAY_MS * attempt))
					try {
						queryStream = await createCopyStream()
						await pipeline(Readable.from(tsvLines), queryStream, { end: false })
						inserted = tsvLines.length
						recovered = true
					} catch (retryErr) {
						console.warn(`[${workerName}] reconnect attempt ${attempt}/${CONNECTION_RETRY_MAX} failed: ${(retryErr as Error).message}`)
						if (attempt === CONNECTION_RETRY_MAX) {
							const details = extractPgErrorDetails(retryErr)
							for (let i = 0; i < tsvLines.length; i++) {
								rejects.write({
									source: `copy:${workerName}`,
									rawLine: tsvLines[i]!.replace(/\n$/, ""),
									error: `connection retry exhausted: ${details.message}`,
									errorCode: details.code,
									chunkFile: filePath,
									batchIndex,
									rowIndexInBatch: i,
								})
							}
							rejected = tsvLines.length
						}
					}
				}
			} else {
				const result = await retryWithQuartering(tsvLines, error as Error, {
					createCopyStream,
					rejects,
					workerName,
					filePath,
					batchIndex,
					baseRowIndex: 0,
				})
				inserted = result.inserted
				rejected = result.rejected
			}
		}

		count += inserted
		totalRejected += rejected
		const batchMs = Date.now() - batchStart

		const rate = batchMs > 0 ? Math.round(inserted / (batchMs / 1000)) : 0
		console.log(
			`[${workerName}] batch ${batchIndex} | inserted ${inserted.toLocaleString()} ` +
			`| rejected ${rejected} | total ${count.toLocaleString()} | ${rate.toLocaleString()} rps | ${batchMs}ms`
		)

		postMessage({
			type: "progress",
			workerName,
			rows: inserted,
			rejected,
			batchMs,
		})

		// Ver comentario original: hay que quitar listeners para que el COPY
		// pueda commitearse al final manteniendo el stream vivo entre batches.
		queryStream.removeAllListeners("error")
		queryStream.removeAllListeners("close")
		queryStream.removeAllListeners("finish")
		queryStream.removeAllListeners("end")
	}

	queryStream.end()
	try {
		await finished(queryStream)
	} catch (closeErr) {
		console.warn(`[${workerName}] COPY stream finished with error: ${(closeErr as Error).message}`)
	}

	const rejectCount = await rejects.close()

	const tag = drainedEarly ? "done (drained)" : "done"
	console.log(`[${workerName}] ${tag} | total inserted ${count.toLocaleString()} | rejected ${rejectCount}`)
	postMessage({ type: "done", workerName, count, rejected: rejectCount })
}
