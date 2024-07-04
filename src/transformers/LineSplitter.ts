export class LineSplitter implements Transformer<string, string> {
	private buffer: string;

	constructor() {
		this.buffer = '';
	}

	transform(chunk: string, controller: TransformStreamDefaultController<string>) {
		this.buffer += chunk;
		let lines = this.buffer.split('\n');
		this.buffer = lines.pop() || ''; // Guardar el último fragmento para la próxima chunk
		let hasSkippedHeader = false;
		for (let line of lines) {
			if (!hasSkippedHeader) {
				const isHeader = line.startsWith("RUC");
				if (isHeader) {
					hasSkippedHeader = true;
					continue;
				}
			}

			controller.enqueue(line);
		}
	}

	flush(controller: TransformStreamDefaultController<string>) {
		if (this.buffer) {
			controller.enqueue(this.buffer);
		}
	}
}