import { FILE_LINES_SPLIT, dnisDir, filesDir, localFile, localZipFile, rejectsDir, remoteZipFile, rucsDir } from "../constants";

import { $ } from "bun";
import { LineGrouper } from "../stream/LineGrouper";
import { LineSplitterWithoutHeader } from "../stream/LineSplitterWithoutHeader";
import { Readable } from "node:stream";
import { RejectsWriter } from "../rejects/RejectsWriter";
import { TextDecoderStream } from "../stream/polifylls";
import { createWriteStream, } from "node:fs";
import { escapeCopyField } from "../copy/escapeCopyField";
import fs from "node:fs/promises";
import { pipeline, } from "node:stream/promises";

type ParseResult =
	| { kind: "tsv"; tsv: string }
	| { kind: "reject"; reason: string }
	| { kind: "skip" }

function parseDniLine(line: string): ParseResult {
	// DNI = persona natural; SUNAT encodes natural persons como RUCs que empiezan con "10".
	if (!line.startsWith("10")) {
		return { kind: "skip" }
	}
	if (line.includes("|ANULACION - ERROR SU|")) {
		return { kind: "skip" }
	}

	const firstPipe = line.indexOf("|")
	if (firstPipe === -1) {
		return { kind: "reject", reason: "no pipe separator" }
	}
	const secondPipe = line.indexOf("|", firstPipe + 1)
	if (secondPipe === -1) {
		return { kind: "reject", reason: "only one pipe separator" }
	}

	const ruc = line.slice(0, firstPipe).trim()
	const nombre = line.slice(firstPipe + 1, secondPipe).trim().replace(/\s+/g, " ")

	if (!ruc) {
		return { kind: "reject", reason: "empty ruc" }
	}
	if (!nombre || nombre === "-") {
		return { kind: "reject", reason: "empty nombre" }
	}
	if (ruc.length < 4) {
		return { kind: "reject", reason: "ruc too short to extract dni" }
	}

	const dni = ruc.slice(2, -1)
	const tsv = `${escapeCopyField(dni)}\t${escapeCopyField(nombre)}\n`
	return { kind: "tsv", tsv }
}

function parseRucLine(line: string): ParseResult {
	if (line.includes("|ANULACION - ERROR SU|")) {
		return { kind: "skip" }
	}

	// Mismas transformaciones que el viejo rucParser, fusionadas con el TSV final:
	// 1. trim + colapso de whitespace
	// 2. escapar backslash a \\
	// 3. colapsar segmentos `|-+` a `|` (campos vacíos con sólo guiones)
	// 4. quitar pipes finales
	// 5. revertir `\\|` (era literal `-|` en SUNAT) a `-`
	const cleaned = line
		.trim()
		.replace(/\s+/g, " ")
		.replace(/\\/g, "\\\\")
		.replace(/\|-+/g, "|")
		.replace(/\|+$/, "")
		.replace(/\\\\\|/g, "-")

	const fields = cleaned.split("|").map(v => {
		const t = v.trim()
		return t === "" ? undefined : t
	})

	const [
		ruc,
		razonSocial,
		estado,
		condicionDomicilio,
		ubigeo,
		tipoVia,
		nombreVia,
		codigoZona,
		tipoZona,
		numero,
		interior,
		lote,
		departamento,
		manzana,
		kilometro,
	] = fields

	if (!ruc) {
		return { kind: "reject", reason: "empty ruc" }
	}
	if (!razonSocial) {
		return { kind: "reject", reason: "empty razonSocial" }
	}
	if (!estado) {
		return { kind: "reject", reason: "empty estado" }
	}

	const noIsRUC10 = !ruc.startsWith("10")
	const nCondicionDomicilio = condicionDomicilio ?? "NO HABIDO"

	// `\N` es el marker COPY-text para NULL. Para "10*" (persona natural)
	// todos los campos de dirección van como NULL.
	const addr = (v: string | undefined) =>
		noIsRUC10 && v ? escapeCopyField(v) : "\\N"

	// Orden de columnas del COPY: ruc, razonSocial, estado, condicionDomicilio,
	// tipoVia, nombreVia, codigoZona, tipoZona, numero, interior, lote,
	// departamento, manzana, kilometro, codigoUbigeo
	const tsv = [
		escapeCopyField(ruc),
		escapeCopyField(razonSocial),
		escapeCopyField(estado),
		escapeCopyField(nCondicionDomicilio),
		addr(tipoVia),
		addr(nombreVia),
		addr(codigoZona),
		addr(tipoZona),
		addr(numero),
		addr(interior),
		addr(lote),
		addr(departamento),
		addr(manzana),
		addr(kilometro),
		addr(ubigeo),
	].join("\t") + "\n"

	return { kind: "tsv", tsv }
}

type ChunkWriterState = {
	dir: string
	index: number
	linesInCurrent: number
	writer: ReturnType<ReturnType<typeof Bun.file>["writer"]>
}

function openChunkWriter(dir: string, index: number) {
	return Bun.file(`${dir}/chunk_${index}.txt`).writer({
		highWaterMark: 1024 * 1024 * 50,
	})
}

function createChunkState(dir: string): ChunkWriterState {
	return {
		dir,
		index: 0,
		linesInCurrent: 0,
		writer: openChunkWriter(dir, 0),
	}
}

async function rotateIfNeeded(state: ChunkWriterState, label: string): Promise<void> {
	if (state.linesInCurrent < FILE_LINES_SPLIT) {
		return
	}
	const closedIndex = state.index
	await state.writer.end()
	state.index++
	state.linesInCurrent = 0
	state.writer = openChunkWriter(state.dir, state.index)
	console.log(`[split] ${label} chunk_${closedIndex}.txt full → opening chunk_${state.index}.txt`)
}

export async function updateRucsFile() {
	console.log("[split] preparing directories");
	await fs.mkdir(filesDir, { recursive: true })
	await fs.mkdir(dnisDir, { recursive: true })
	await fs.mkdir(rucsDir, { recursive: true })
	await fs.mkdir(rejectsDir, { recursive: true })

	console.log("[split] downloading zip");
	const dataZipped = await fetch(remoteZipFile)
	const responseStream = Readable.fromWeb(dataZipped.body as any)
	const zipFileStream = createWriteStream(localZipFile, { flags: "w", encoding: "binary" })
	await pipeline(responseStream, zipFileStream)

	console.log("[split] unzipping");
	await $`unzip ${localZipFile} -d ${filesDir} > /dev/null`
	await fs.rm(localZipFile)

	const dniState = createChunkState(dnisDir)
	const rucState = createChunkState(rucsDir)

	const dniRejects = new RejectsWriter(`${rejectsDir}/parse-dni.jsonl`)
	const rucRejects = new RejectsWriter(`${rejectsDir}/parse-ruc.jsonl`)

	let totalSourceLines = 0
	let totalDniEmitted = 0
	let totalRucEmitted = 0

	const classifierStream = new WritableStream<string[]>({
		async write(lines) {
			for (const line of lines) {
				totalSourceLines++
				// +1 porque el header "RUC..." fue saltado y queremos números
				// 1-based para que coincidan con un editor de texto
				const sourceLineNum = totalSourceLines + 1

				const dni = parseDniLine(line)
				if (dni.kind === "tsv") {
					await rotateIfNeeded(dniState, "dni")
					dniState.writer.write(dni.tsv)
					dniState.linesInCurrent++
					totalDniEmitted++
				} else if (dni.kind === "reject") {
					dniRejects.write({
						source: "parse-dni",
						rawLine: line,
						error: dni.reason,
						sourceLine: sourceLineNum,
					})
				}

				const ruc = parseRucLine(line)
				if (ruc.kind === "tsv") {
					await rotateIfNeeded(rucState, "ruc")
					rucState.writer.write(ruc.tsv)
					rucState.linesInCurrent++
					totalRucEmitted++
				} else if (ruc.kind === "reject") {
					rucRejects.write({
						source: "parse-ruc",
						rawLine: line,
						error: ruc.reason,
						sourceLine: sourceLineNum,
					})
				}
			}
		},
		async close() {
			await Promise.all([
				dniState.writer.end(),
				rucState.writer.end(),
			])
		}
	});

	const decoderStream = new TextDecoderStream("latin1")
	const lineTransformStream = new TransformStream(new LineSplitterWithoutHeader("RUC"));
	const lineGroupTransformStream = new TransformStream(new LineGrouper(100000));

	const file = Bun.file(localFile)
	const fileStream = file.stream()

	console.log("[split] processing source file");
	await fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)
		.pipeTo(classifierStream);

	await fs.rm(localFile)

	const dniRejectCount = await dniRejects.close()
	const rucRejectCount = await rucRejects.close()

	console.log(
		`[split] DONE | source=${totalSourceLines.toLocaleString()} | ` +
		`dni emitted=${totalDniEmitted.toLocaleString()} chunks=${dniState.index + 1} rejects=${dniRejectCount} | ` +
		`ruc emitted=${totalRucEmitted.toLocaleString()} chunks=${rucState.index + 1} rejects=${rucRejectCount}`
	)

	const dniChunkFiles = Array.from({ length: dniState.index + 1 }, (_, i) => `${dnisDir}/chunk_${i}.txt`);
	const rucChunkFiles = Array.from({ length: rucState.index + 1 }, (_, i) => `${rucsDir}/chunk_${i}.txt`);

	return {
		dniChunkFiles,
		rucChunkFiles,
		parseStats: {
			totalSourceLines,
			totalDniEmitted,
			totalRucEmitted,
			dniRejectCount,
			rucRejectCount,
			dniRejectsPath: dniRejects.getPath(),
			rucRejectsPath: rucRejects.getPath(),
		},
	}
}
