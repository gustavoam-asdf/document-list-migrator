import { sql } from "../db";
import { TextDecoderStream } from "../polifylls";
import { LineGrouper } from "../transformers/LineGrouper";
import { LineSplitter } from "../transformers/LineSplitter";
import { Readable, Writable } from "node:stream";
import { pipeline } from "node:stream/promises";

// prevents TS errors
declare var self: Worker;

interface PersonaNatural {
	dni: string
	nombreCompleto: string
}


self.onmessage = async (event: MessageEvent<string>) => {
	self.postMessage("DNI worker started");
	const dniFilePath = event.data
	const file = Bun.file(dniFilePath)
	const fileStream = file.stream()

	const decoderStream = new TextDecoderStream("utf-8")
	const lineTransformStream = new TransformStream(new LineSplitter);
	const lineGroupTransformStream = new TransformStream(new LineGrouper(100000));

	const dnisStream = fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)

	for await (const lines of dnisStream) {
		const personas: PersonaNatural[] = []
		for (const line of lines) {
			const [
				ruc,
				nombreRazonSocial,
			] = line
				.split("|")
				.map(value => {
					const trimmed = value.trim()

					return (trimmed === "" || trimmed === "-") ? undefined : trimmed
				})

			if (!ruc || !nombreRazonSocial) {
				continue
			}

			const dni = ruc.slice(2, -1)

			personas.push({
				dni,
				nombreCompleto: nombreRazonSocial,
			})
		}

		const readable = Readable.from(personas.map(p => `${p.dni}\t${p.nombreCompleto}\n`))
		const queryStream = await sql`COPY "PersonaNatural" ("dni", "nombreCompleto") FROM STDIN`.writable()

		await pipeline(readable, queryStream)
			.catch(console.error)
	}

	self.postMessage("DNI worker done");
	process.exit(0);
};