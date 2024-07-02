import { TextDecoderStream } from "../polifylls";
import { LineSplitter } from "../transformers/LineSplitter";

// prevents TS errors
declare var self: Worker;

self.onmessage = async (event: MessageEvent<string>) => {
	self.postMessage("DNI worker started");
	const dniFilePath = event.data
	const file = Bun.file(dniFilePath)
	const fileStream = file.stream()

	const decoderStream = new TextDecoderStream("latin1")
	const lineTransformStream = new TransformStream(new LineSplitter);

	const dnisStream = fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)

	for await (const line of dnisStream) {
		console.log(`DNI: ${line}`);
	}

	self.postMessage("DNI worker done");
	process.exit(0);
};