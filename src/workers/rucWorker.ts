import { sql } from "../db";
import { TextDecoderStream } from "../polifylls";
import { LineGrouper } from "../transformers/LineGrouper";
import { LineSplitter } from "../transformers/LineSplitter";
import { Readable, } from "node:stream";
import { pipeline } from "node:stream/promises";

// prevents TS errors
declare var self: Worker;

self.onmessage = async (event: MessageEvent<string>) => {
	self.postMessage("RUC worker started");
	const rucFilePath = event.data
	const file = Bun.file(rucFilePath)
	const fileStream = file.stream()

	const decoderStream = new TextDecoderStream("latin1")
	const lineTransformStream = new TransformStream(new LineSplitter);
	const lineGroupTransformStream = new TransformStream(new LineGrouper(10000));

	const rucsStream = fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)

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

					return (trimmed === "" || trimmed === "-") ? undefined : trimmed
				})

			if (!ruc || !razonSocial || !estado) {
				continue
			}

			const isRUC10 = ruc.startsWith("10")

			const nTipoVia = isRUC10 ? (tipoVia ?? '\\N') : '\\N'
			const nNombreVia = isRUC10 ? (nombreVia ?? '\\N') : '\\N'
			const nCodigoZona = isRUC10 ? (codigoZona ?? '\\N') : '\\N'
			const nTipoZona = isRUC10 ? (tipoZona ?? '\\N') : '\\N'
			const nNumero = isRUC10 ? (numero ?? '\\N') : '\\N'
			const nInterior = isRUC10 ? (interior ?? '\\N') : '\\N'
			const nLote = isRUC10 ? (lote ?? '\\N') : '\\N'
			const nDepartamento = isRUC10 ? (departamento ?? '\\N') : '\\N'
			const nManzana = isRUC10 ? (manzana ?? '\\N') : '\\N'
			const nKilometro = isRUC10 ? (kilometro ?? '\\N') : '\\N'
			const nCodigoUbigeo = isRUC10 ? (ubigeo ?? '\\N') : '\\N'


			personaLines.push(`${ruc}\t${razonSocial}\t${estado}\t${condicionDomicilio}\t${nTipoVia}\t${nNombreVia}\t${nCodigoZona}\t${nTipoZona}\t${nNumero}\t${nInterior}\t${nLote}\t${nDepartamento}\t${nManzana}\t${nKilometro}\t${nCodigoUbigeo}\n`)
		}

		const readable = Readable.from(personaLines)
		const queryStream = await sql`
			COPY "PersonaJuridica" ("ruc", "razonSocial", "estado", "condicionDomicilio", "tipoVia", "nombreVia", "codigoZona", "tipoZona", "numero", "interior", "lote", "departamento", "manzana", "kilometro", "codigoUbigeo") FROM STDIN
		`.writable()

		await pipeline(readable, queryStream)
			.catch(error => console.error({
				error,
				personas: personaLines.map(line => {
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
				})
			}))
	}

	self.postMessage("RUC worker done");
	process.exit(0);
};