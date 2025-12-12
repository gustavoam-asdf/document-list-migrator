import { filesDir, localFile, localZipFile, remoteZipFile } from "./constants";

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

	if (line.includes('ANULACION - ERROR SU')) {
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
	if (line.includes('ANULACION - ERROR SU')) {
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
	console.log("Create directory if not exists...");
	await fs.mkdir(filesDir, { recursive: true })
	console.log("Directory created");

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

	const dnisFilePath = `${filesDir}/dnis.txt`
	const dnisFile = Bun.file(dnisFilePath);
	const dnisWriter = dnisFile.writer({
		highWaterMark: 1024 * 1024 * 100,
	});

	const rucsFilePath = `${filesDir}/rucs.txt`
	const rucFile = Bun.file(rucsFilePath);
	const rucsWriter = rucFile.writer({
		highWaterMark: 1024 * 1024 * 100,
	})

	const classifierStream = new WritableStream<string[]>({
		write(lines) {
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

			dnisWriter.write(dniLines.join('\n'));
			rucsWriter.write(rucLines.join('\n'));
		},
		async close() {
			await Promise.all([
				dnisWriter.end(),
				rucsWriter.end(),
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

	await fs.rm(localFile)

	return {
		dnisPath: dnisFilePath,
		rucsPath: rucsFilePath,
	}
}