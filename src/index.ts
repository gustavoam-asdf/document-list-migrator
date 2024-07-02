import { TextDecoderStream } from "./polifylls";
import { filesDir, localFile } from "./constants";
import { updateRucsFile } from "./updateRucsFile";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

// await updateRucsFile();

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

const dnisFile = Bun.file(`${filesDir}/dnis.txt`);

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

const lineTransformStream = new TransformStream(new LineSplitter);

await fileStream
	.pipeThrough(decoderStream)
	.pipeThrough(lineTransformStream)
	.pipeTo(classifierStream);

const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);