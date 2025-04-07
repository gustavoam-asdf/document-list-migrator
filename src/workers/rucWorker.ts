import { finished, pipeline } from "node:stream/promises";
import { primarySql, secondarySql, } from "../db";

import { LineGrouper } from "../transformers/LineGrouper";
import { LineSplitterWithoutHeader } from "../transformers/LineSplitterWithoutHeader";
import { Readable, } from "node:stream";
import { TextDecoderStream } from "../polifylls";
import { Writable } from "node:stream";
import { splitInParts } from "../splitInParts";

// prevents TS errors
declare var self: Worker;

async function retryToInsert(lines: string[], error: Error, createCopyQueryStream: () => Promise<Writable>) {
	const partLength = Math.floor(lines.length / 4)

	if (partLength < 5) {
		console.error({
			error,
			lines,
			personas: lines.map(line => {
				const [
					ruc,
					razonSocial,
					estado,
					condicionDomicilio,
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
					codigoUbigeo,
				] = line.trim().split("\t")

				return {
					ruc,
					razonSocial,
					estado,
					condicionDomicilio,
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
					codigoUbigeo,
				}
			}),
		})
		return
	}

	console.error({
		error,
		lines: lines.length,
		message: "Retrying to insert RUCs"
	})

	const parts = splitInParts({ values: lines, size: partLength })

	let index = 0
	for (const part of parts) {
		const readable = Readable.from(part)
		const queryStream = await createCopyQueryStream()

		await pipeline(readable, queryStream)
			.then(() => console.log(`${(new Date).toISOString()}: Inserted ${part.length} RUCs [${index + 1} / ${parts.length}]`))
			.catch(error => retryToInsert(part, error, createCopyQueryStream))
		index++
	}
}

self.onmessage = async (event: MessageEvent<{
	filePath: string;
	useSecondaryDb: boolean;
}>) => {
	console.log("RUC worker started");
	const { filePath: rucFilePath, useSecondaryDb } = event.data

	console.log("Reading RUC file");
	const file = Bun.file(rucFilePath)
	const fileStream = file.stream()

	const decoderStream = new TextDecoderStream("latin1")
	const lineTransformStream = new TransformStream(new LineSplitterWithoutHeader("RUC"));
	const lineGroupTransformStream = new TransformStream(new LineGrouper(50000));

	const rucsStream = fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)

	const sql = useSecondaryDb ? secondarySql : primarySql

	const createCopyQueryStream = () => sql`
		COPY "PersonaJuridica" ("ruc", "razonSocial", "estado", "condicionDomicilio", "tipoVia", "nombreVia", "codigoZona", "tipoZona", "numero", "interior", "lote", "departamento", "manzana", "kilometro", "codigoUbigeo") FROM STDIN
	`.writable()

	const queryStream = await createCopyQueryStream()

	console.log(`Inserting RUCs into ${useSecondaryDb ? "secondary" : "primary"} database`);
	let count = 0
	for await (const lines of rucsStream) {
		const personaLines: string[] = []
		for (const line of lines) {
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
			] = line
				.split("|")
				.map(value => {
					const trimmed = value.trim()
					const spacesCleaned = trimmed.replace(/\s+/g, " ")
					const rareCharsCleaned = spacesCleaned.replace(/\\/g, "\\\\")

					const onlyHyphens = /^-+$/g.test(rareCharsCleaned)

					return (
						spacesCleaned === "" ||
						onlyHyphens
					) ? undefined : rareCharsCleaned
				})

			if (!ruc || !razonSocial || !estado) {
				continue
			}

			const noIsRUC10 = !ruc.startsWith("10")

			const nCondicionDomicilio = condicionDomicilio ?? 'NO HABIDO'
			const nTipoVia = noIsRUC10 ? (tipoVia ?? '\\N') : '\\N'
			const nNombreVia = noIsRUC10 ? (nombreVia ?? '\\N') : '\\N'
			const nCodigoZona = noIsRUC10 ? (codigoZona ?? '\\N') : '\\N'
			const nTipoZona = noIsRUC10 ? (tipoZona ?? '\\N') : '\\N'
			const nNumero = noIsRUC10 ? (numero ?? '\\N') : '\\N'
			const nInterior = noIsRUC10 ? (interior ?? '\\N') : '\\N'
			const nLote = noIsRUC10 ? (lote ?? '\\N') : '\\N'
			const nDepartamento = noIsRUC10 ? (departamento ?? '\\N') : '\\N'
			const nManzana = noIsRUC10 ? (manzana ?? '\\N') : '\\N'
			const nKilometro = noIsRUC10 ? (kilometro ?? '\\N') : '\\N'
			const nCodigoUbigeo = noIsRUC10 ? (ubigeo ?? '\\N') : '\\N'

			personaLines.push(`${ruc}\t${razonSocial}\t${estado}\t${nCondicionDomicilio}\t${nTipoVia}\t${nNombreVia}\t${nCodigoZona}\t${nTipoZona}\t${nNumero}\t${nInterior}\t${nLote}\t${nDepartamento}\t${nManzana}\t${nKilometro}\t${nCodigoUbigeo}\n`)
		}

		const readable = Readable.from(personaLines)

		await pipeline(readable, queryStream, {
			end: false,
		})
			.then(() => {
				count += lines.length
				console.log(`${(new Date).toISOString()}: Inserted ${count.toString().padStart(8, "_")} RUCs to ${useSecondaryDb ? "secondary" : "primary"} database`)
			})
			.catch(async error => retryToInsert(personaLines, error, createCopyQueryStream))

		// ! Ensure that only unnecessary events are supressed due to is needed a transaction to commit the data
		queryStream.removeAllListeners("error");
		queryStream.removeAllListeners("close");
		queryStream.removeAllListeners("finish");
		queryStream.removeAllListeners("end");
	}

	queryStream.end();

	await finished(queryStream)

	self.postMessage("RUC worker done");
	process.exit(0);
};