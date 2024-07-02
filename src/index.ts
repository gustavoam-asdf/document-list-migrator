import { TextDecoderStream } from "./polifylls";
import { localFile } from "./constants";
import { updateRucsFile } from "./updateRucsFile";
import { LineSplitter } from "./transformers/LineSplitter";
import { LineGrouper } from "./transformers/LineGrouper";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

// await updateRucsFile();

const file = Bun.file(localFile)
const fileStream = file.stream()

const decoderStream = new TextDecoderStream("latin1")

const rucsPipeStream = new TransformStream<string, string>();
const ruc10PipeStream = new TransformStream<string, string>();

const classifierStream = new WritableStream<string>({
	write(line) {
		const isRuc10 = line.startsWith('10');
		if (!isRuc10) {
			const rucsWriter = rucsPipeStream.writable.getWriter();
			rucsWriter.write(line);
			rucsWriter.releaseLock();
			return;
		}

		const rucsWriter = rucsPipeStream.writable.getWriter();
		const ruc10Writer = ruc10PipeStream.writable.getWriter();
		rucsWriter.write(line);
		ruc10Writer.write(line);

		rucsWriter.releaseLock();
		ruc10Writer.releaseLock();
	}
});

const lineTransformStream = new TransformStream(new LineSplitter);
const lineGrouperStream = new TransformStream(new LineGrouper(100));

fileStream
	.pipeThrough(decoderStream)
	.pipeThrough(lineTransformStream)
	.pipeTo(classifierStream);

console.log("Streams created");

const rucsStream = rucsPipeStream
	.readable
	.pipeThrough(lineGrouperStream);
const ruc10Stream = ruc10PipeStream
	.readable
	.pipeThrough(lineGrouperStream);

async function readGeneralRuc() {
	const worker = new Worker("./src/rucWorker.ts");
	for await (const ruc of rucsStream) {
		worker.postMessage(ruc);
	}
}

async function readRuc10() {
	const worker = new Worker("./src/dniWorker.ts");
	for await (const ruc10 of ruc10Stream) {
		worker.postMessage(ruc10);
	}
}

await Promise.all([readGeneralRuc(), readRuc10()]);

const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);