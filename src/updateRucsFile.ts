import { filesDir, localFile, localZipFile, remoteZipFile } from "./constants";

import { $ } from "bun";
import { LineSplitter } from "./transformers/LineSplitter";
import { TextDecoderStream } from "./polifylls";
import fs from "node:fs/promises";

export async function updateRucsFile() {
	console.log("Cleaning files directory...");
	await fs.mkdir(filesDir, { recursive: true })

	console.log("Downloading file...");
	const dataZipped = await fetch(remoteZipFile)

	console.log("Saving zip file...");
	await Bun.write(localZipFile, dataZipped)

	console.log("Unzipping file...");
	await $`unzip ${localZipFile} -d ${filesDir} > /dev/null`

	const dnisFilePath = `${filesDir}/dnis.txt`
	const dnisFile = Bun.file(dnisFilePath);

	const dnisWriter = dnisFile.writer({
		highWaterMark: 1024 * 1024 * 100,
	});

	const classifierStream = new WritableStream<string>({
		write(line) {
			const isRuc10 = line.startsWith('10');
			if (!isRuc10) {
				return;
			}

			const firstPipeCharacter = line.indexOf('|');

			if (firstPipeCharacter === -1) {
				return;
			}

			const secondPipeCharacter = line.indexOf('|', firstPipeCharacter + 1);

			if (secondPipeCharacter === -1) {
				return;
			}

			const personLine = line.slice(0, secondPipeCharacter + 1);

			dnisWriter.write(personLine + "\n");
		},
		close() {
			dnisWriter.end();
		}
	});

	const decoderStream = new TextDecoderStream("latin1")
	const lineTransformStream = new TransformStream(new LineSplitter);

	const file = Bun.file(localFile)
	const fileStream = file.stream()

	await fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeTo(classifierStream);

	return {
		dnisPath: dnisFilePath,
		rucsPath: localFile,
	}
}