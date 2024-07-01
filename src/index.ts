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

const lineTransformStream = new TransformStream(new LineSplitter);

const lineStream = fileStream
	.pipeThrough(decoderStream)
	.pipeThrough(lineTransformStream)

for await (const line of lineStream) {
	console.log(line.toString());
	console.log('---');
}
