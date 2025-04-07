import { finished, pipeline } from "node:stream/promises";
import { primarySql, secondarySql } from "../db";

import { LineGrouper } from "../transformers/LineGrouper";
import { LineSplitter } from "../transformers/LineSplitter";
import { Readable } from "node:stream";
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
				const [dni, nombreCompleto] = line.trim().split("\t")

				return {
					dni,
					nombreCompleto,
				}
			}),
		})
		return
	}

	console.error({
		error,
		lines: lines.length,
		message: "Retrying to insert DNIs"
	})

	const parts = splitInParts({ values: lines, size: partLength })

	let index = 0
	for (const part of parts) {
		const readable = Readable.from(part)
		const queryStream = await createCopyQueryStream()

		await pipeline(readable, queryStream)
			.then(() => console.log(`${(new Date).toISOString()}: Inserted ${part.length} DNIs [${index + 1} / ${parts.length}]`))
			.catch(error => retryToInsert(part, error, createCopyQueryStream))
		index++
	}
}

self.onmessage = async (event: MessageEvent<{
	filePath: string;
	useSecondaryDb: boolean;
}>) => {
	console.log("DNI worker started");
	const { filePath: dniFilePath, useSecondaryDb } = event.data

	console.log("Reading DNI file");
	const file = Bun.file(dniFilePath)
	const fileStream = file.stream()

	const decoderStream = new TextDecoderStream("utf-8")
	const lineTransformStream = new TransformStream(new LineSplitter);
	const lineGroupTransformStream = new TransformStream(new LineGrouper(50000));

	const dnisStream = fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)

	const sql = useSecondaryDb ? secondarySql : primarySql

	const createCopyQueryStream = () => sql`COPY "PersonaNatural" ("dni", "nombreCompleto") FROM STDIN`.writable()

	const queryStream = await createCopyQueryStream()

	console.log(`Inserting DNIs into ${useSecondaryDb ? "secondary" : "primary"} database`);
	let count = 0
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
					const spacesCleaned = trimmed.replace(/\s+/g, " ")

					return (spacesCleaned === "" || spacesCleaned === "-") ? undefined : spacesCleaned
				})

			if (!ruc || !nombreRazonSocial) {
				continue
			}

			const dni = ruc.slice(2, -1)

			personaLines.push(`${dni}\t${nombreRazonSocial}\n`)
		}

		const readable = Readable.from(personaLines)

		await pipeline(readable, queryStream, {
			end: false,
		})
			.then(() => {
				count += lines.length
				console.log(`${(new Date).toISOString()}: Inserted ${count} DNIs to ${useSecondaryDb ? "secondary" : "primary"} database`)
			})
			.catch(error => retryToInsert(personaLines, error, createCopyQueryStream))

		// const eventNames = queryStream.eventNames()

		// for (const eventName of eventNames) {
		// 	const listenerCount = queryStream.listenerCount(eventName);
		// 	console.log(`Event: ${eventName.toString()}, Listeners: ${listenerCount}`);
		// }

		// ! Ensure that only unnecessary events are supressed due to is needed a transaction to commit the data
		queryStream.removeAllListeners("error");
		queryStream.removeAllListeners("close");
		queryStream.removeAllListeners("finish");
		queryStream.removeAllListeners("end");
	}

	queryStream.end();
	await finished(queryStream)

	self.postMessage("DNI worker done");
	process.exit(0);
};