import { TextDecoderStream } from "../polifylls";
import { LineGrouper } from "../transformers/LineGrouper";
import { LineSplitter } from "../transformers/LineSplitter";

// prevents TS errors
declare var self: Worker;

enum EstadoContribuyente {
	ACTIVO = "ACTIVO",
	SUSPENSION_TEMPORAL = "SUSPENSION TEMPORAL",
	BAJA_PROVISIONAL = "BAJA PROVISIONAL",
	BAJA_DEFINITIVA = "BAJA DEFINITIVA",
	BAJA_PROVISIONAL_DE_OFICIO = "BAJA PROV. POR OFICI",
	BAJA_DEFINITIVA_DE_OFICIO = "BAJA DEFINITIVA DE OFICIO"
}

interface PersonaJuridica {
	ruc: string
	razonSocial: string
	estado: EstadoContribuyente
	condicionDomicilio: string
	codigoUbigeo?: string
	direccion: {
		tipoVia?: string
		nombreVia?: string
		codigoZona?: string
		tipoZona?: string
		numero?: string
		interior?: string
		lote?: string
		departamento?: string
		manzana?: string
		kilometro?: string
	}
}

self.onmessage = async (event: MessageEvent<string>) => {
	self.postMessage("RUC worker started");
	const rucFilePath = event.data
	const file = Bun.file(rucFilePath)
	const fileStream = file.stream()

	const decoderStream = new TextDecoderStream("latin1")
	const lineTransformStream = new TransformStream(new LineSplitter);
	const lineGroupTransformStream = new TransformStream(new LineGrouper(100000));

	const rucsStream = fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)

	for await (const lines of rucsStream) {
		for (const line of lines) {
			const [
				ruc,
				nombreRazonSocial,
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

					return trimmed === "" || trimmed === "-" ? undefined : trimmed
				})
		}
	}

	self.postMessage("RUC worker done");
	process.exit(0);
};