import { filesDir, localFile, localZipFile, remoteZipFile } from "./constants";

import { $ } from "bun";
import fs from "node:fs/promises";
import { LineSplitter } from "./transformers/LineSplitter";

export async function updateRucsFile() {
	console.log("Cleaning files directory...");
	await fs.rmdir(filesDir, { recursive: true })
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

			dnisWriter.write(line + "\n");
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