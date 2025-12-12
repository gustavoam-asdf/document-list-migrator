import { FILE_LINES_SPLIT, dnisDir, filesDir, localFile, localZipFile, remoteZipFile, rucsDir } from "./constants";

import { $ } from "bun";
import { LineGrouper } from "./transformers/LineGrouper";
import { LineSplitterWithoutHeader } from "./transformers/LineSplitterWithoutHeader";
import { Readable } from "node:stream";
import { TextDecoderStream } from "./polifylls";
import { createWriteStream, } from "node:fs";
import fs from "node:fs/promises";
import { pipeline, } from "node:stream/promises";

type LineParser = (line: string) => string | undefined;

const dniParser: LineParser = line => {
	const isRuc10 = line.startsWith('10');
	if (!isRuc10) {
		return "";
	}

	if (line.includes('|ANULACION - ERROR SU|')) {
		return "";
	}

	const firstPipeCharacter = line.indexOf('|');

	if (firstPipeCharacter === -1) {
		return "";
	}

	const secondPipeCharacter = line.indexOf('|', firstPipeCharacter + 1);

	if (secondPipeCharacter === -1) {
		return "";
	}

	const dniLine = line.slice(0, secondPipeCharacter);

	return dniLine;
}

const rucParser: LineParser = line => {
	if (line.includes('|ANULACION - ERROR SU|')) {
		return "";
	}

	const trimmed = line.trim()
	const spacesCleaned = trimmed.replace(/\s+/g, " ")
	const rareCharsCleaned = spacesCleaned.replace(/\\/g, "\\\\")
	const withoutHyphens = rareCharsCleaned.replace(/\|-+/g, '|')

	const rucLine = withoutHyphens.replace(/\\\\\|/g, '-')
	return rucLine;
}

export async function updateRucsFile() {
	console.log("Create directories if not exists...");
	await fs.mkdir(filesDir, { recursive: true })
	await fs.mkdir(dnisDir, { recursive: true })
	await fs.mkdir(rucsDir, { recursive: true })
	console.log("Directories created");

	console.log("Downloading and saving zip file...");
	const dataZipped = await fetch(remoteZipFile)
	const responseStream = Readable.fromWeb(dataZipped.body as any)

	const zipFileStream = createWriteStream(localZipFile, {
		flags: 'w',
		encoding: 'binary',
	})

	await pipeline(responseStream, zipFileStream)
	console.log("Zip file saved");

	console.log("Unzipping file...");
	await $`unzip ${localZipFile} -d ${filesDir} > /dev/null`
	console.log("File unzipped");

	console.log("Removing zip file...");
	await fs.rm(localZipFile)
	console.log("Zip file removed");

	// Chunk writers management
	let dniChunkIndex = 0;
	let dniLinesInCurrentChunk = 0;
	let currentDniWriter = Bun.file(`${dnisDir}/chunk_${dniChunkIndex}.txt`).writer({
		highWaterMark: 1024 * 1024 * 50,
	});

	let rucChunkIndex = 0;
	let rucLinesInCurrentChunk = 0;
	let currentRucWriter = Bun.file(`${rucsDir}/chunk_${rucChunkIndex}.txt`).writer({
		highWaterMark: 1024 * 1024 * 50,
	});

	const classifierStream = new WritableStream<string[]>({
		async write(lines) {
			const dniLines: string[] = [];
			const rucLines: string[] = [];

			for (const line of lines) {
				const dniLine = dniParser(line);
				const rucLine = rucParser(line);

				if (dniLine) {
					dniLines.push(dniLine);
				}
				if (rucLine) {
					rucLines.push(rucLine);
				}
			}

			// Write DNI lines with chunk rotation
			for (const dniLine of dniLines) {
				if (dniLinesInCurrentChunk >= FILE_LINES_SPLIT) {
					await currentDniWriter.end();
					dniChunkIndex++;
					dniLinesInCurrentChunk = 0;
					currentDniWriter = Bun.file(`${dnisDir}/chunk_${dniChunkIndex}.txt`).writer({
						highWaterMark: 1024 * 1024 * 50,
					});
					console.log(`Created DNI chunk file ${dniChunkIndex}`);
				}
				currentDniWriter.write(dniLine + '\n');
				dniLinesInCurrentChunk++;
			}

			// Write RUC lines with chunk rotation
			for (const rucLine of rucLines) {
				if (rucLinesInCurrentChunk >= FILE_LINES_SPLIT) {
					await currentRucWriter.end();
					rucChunkIndex++;
					rucLinesInCurrentChunk = 0;
					currentRucWriter = Bun.file(`${rucsDir}/chunk_${rucChunkIndex}.txt`).writer({
						highWaterMark: 1024 * 1024 * 50,
					});
					console.log(`Created RUC chunk file ${rucChunkIndex}`);
				}
				currentRucWriter.write(rucLine + '\n');
				rucLinesInCurrentChunk++;
			}
		},
		async close() {
			await Promise.all([
				currentDniWriter.end(),
				currentRucWriter.end(),
			])
		}
	});

	const decoderStream = new TextDecoderStream("latin1")
	const lineTransformStream = new TransformStream(new LineSplitterWithoutHeader("RUC"));
	const lineGroupTransformStream = new TransformStream(new LineGrouper(100000));

	const file = Bun.file(localFile)
	const fileStream = file.stream()

	console.log("Processing file...");
	await fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)
		.pipeTo(classifierStream);
	console.log("File processed");

	console.log(`Created ${dniChunkIndex + 1} DNI chunk files and ${rucChunkIndex + 1} RUC chunk files`);

	await fs.rm(localFile)

	// Build file paths arrays
	const dniChunkFiles = Array.from({ length: dniChunkIndex + 1 }, (_, i) => `${dnisDir}/chunk_${i}.txt`);
	const rucChunkFiles = Array.from({ length: rucChunkIndex + 1 }, (_, i) => `${rucsDir}/chunk_${i}.txt`);

	return {
		dniChunkFiles,
		rucChunkFiles,
	}
}