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

async function retryToInsert(lines: string[], error: Error, queryStream: Writable) {
	const partLength = Math.floor(lines.length / 4)

	if (partLength < 10) {
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

	for (const part of parts) {
		const readable = Readable.from(part)

		await pipeline(readable, queryStream, {
			end: false,
		})
			.catch(error => retryToInsert(part, error, queryStream))
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

	const queryStream = await sql`COPY "PersonaNatural" ("dni", "nombreCompleto") FROM STDIN`.writable()

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
				console.log(`${(new Date).toISOString()}: Inserted ${lines.length} DNIs, ${count} in total`)
			})
			.catch(error => retryToInsert(personaLines, error, queryStream))

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