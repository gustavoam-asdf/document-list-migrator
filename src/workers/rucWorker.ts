import { TextDecoderStream } from "../polifylls";
import { LineSplitter } from "../transformers/LineSplitter";

// prevents TS errors
declare var self: Worker;

self.onmessage = async (event: MessageEvent<string>) => {
	self.postMessage("RUC worker started");
	const rucFilePath = event.data
	const file = Bun.file(rucFilePath)
	const fileStream = file.stream()

	const decoderStream = new TextDecoderStream("latin1")
	const lineTransformStream = new TransformStream(new LineSplitter);

	const rucsStream = fileStream
		.pipeThrough(decoderStream)
		.pipeThrough(lineTransformStream)

	for await (const line of rucsStream) {
		console.log(`RUC: ${line}`);
	}

	self.postMessage("RUC worker done");
	process.exit(0);
};