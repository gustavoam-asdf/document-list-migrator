import { filesDir, localFile, localZipFile, remoteZipFile } from "./constants";

import { $ } from "bun";
import { LineGrouper } from "./transformers/LineGrouper";
import { LineSplitter } from "./transformers/LineSplitter";
import { LineSplitterWithoutHeader } from "./transformers/LineSplitterWithoutHeader";
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

	const classifierStream = new WritableStream<string[]>({
		write(lines) {
			const parsed = lines.map(line => {
				const isRuc10 = line.startsWith('10');
				if (!isRuc10) {
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

				const personLine = line.slice(0, secondPipeCharacter);

				return `${personLine}\n`;
			})

			dnisWriter.write(parsed.join(''));
		},
		close() {
			dnisWriter.end();
		}
	});

	const decoderStream = new TextDecoderStream("latin1")
	const lineTransformStream = new TransformStream(new LineSplitterWithoutHeader("RUC"));
	const lineGroupTransformStream = new TransformStream(new LineGrouper(100000));

	const file = Bun.file(localFile)
	const fileStream = file.stream()

	await fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)
		.pipeThrough(lineGroupTransformStream)
		.pipeTo(classifierStream);

	return {
		dnisPath: dnisFilePath,
		rucsPath: localFile,
	}
}