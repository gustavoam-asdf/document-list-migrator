import { sql } from "../db";
import { TextDecoderStream } from "../polifylls";
import { splitInParts } from "../splitInParts";
import { LineGrouper } from "../transformers/LineGrouper";
import { LineSplitter } from "../transformers/LineSplitter";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

// prevents TS errors
declare var self: Worker;

async function retryToInsert(lines: string[], error: Error) {
	const partLength = Math.floor(lines.length / 4)

	if (partLength < 10) {
		console.error({
			error,
			personas: lines.map(line => {
				const [dni, nombreCompleto] = line.trim().split("\t")

				return {
					dni,
					nombreCompleto,
				}
			}),
		})
		return
	}

	const parts = splitInParts({ values: lines, size: partLength })

	for (const part of parts) {
		const readable = Readable.from(part)
		const queryStream = await sql`COPY "PersonaNatural" ("dni", "nombreCompleto") FROM STDIN`.writable()

		await pipeline(readable, queryStream)
			.catch(error => retryToInsert(part, error))
	}
}

self.onmessage = async (event: MessageEvent<string>) => {
	console.log("DNI worker started");
	const dniFilePath = event.data
	const file = Bun.file(dniFilePath)
	const fileStream = file.stream()

	const decoderStream = new TextDecoderStream("utf-8")
	const lineTransformStream = new TransformStream(new LineSplitter);
	const lineGroupTransformStream = new TransformStream(new LineGrouper(10000));

	const dnisStream = fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)

	for await (const lines of dnisStream) {
		const personaLines: string[] = []
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

			personaLines.push(`${dni}\t${nombreRazonSocial}\n`)
		}

		const readable = Readable.from(personaLines)
		const queryStream = await sql`COPY "PersonaNatural" ("dni", "nombreCompleto") FROM STDIN`.writable()

		await pipeline(readable, queryStream)
			.catch(error => retryToInsert(personaLines, error))
	}

	self.postMessage("DNI worker done");
	process.exit(0);
};