import { TextDecoderStream } from "./polifylls";
import { localFile } from "./constants";
import { updateRucsFile } from "./updateRucsFile";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

// await updateRucsFile();

const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);


const file = Bun.file(localFile)
const fileStream = file.stream()

const decoderStream = new TextDecoderStream("latin1")

class LineSplitter implements Transformer<string, string> {
	private buffer: string;

	constructor() {
		this.buffer = '';
	}

	transform(chunk: string, controller: TransformStreamDefaultController<string>) {
		this.buffer += chunk;
		let lines = this.buffer.split('\n');
		this.buffer = lines.pop() || ''; // Guardar el último fragmento para la próxima chunk
		for (let line of lines) {
			controller.enqueue(line);
		}
	}

	flush(controller: TransformStreamDefaultController<string>) {
		if (this.buffer) {
			controller.enqueue(this.buffer);
		}
	}
}

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

fileStream
	.pipeThrough(decoderStream)
	.pipeThrough(lineTransformStream)
	.pipeTo(classifierStream);

const rucsStream = rucsPipeStream.readable;
const ruc10Stream = ruc10PipeStream.readable;

for await (const ruc of ruc10Stream) {
	if (!ruc.startsWith('10')) {
		console.log(ruc);
	}
}