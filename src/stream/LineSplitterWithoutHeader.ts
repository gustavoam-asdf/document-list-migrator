export class LineSplitterWithoutHeader implements Transformer<string, string> {
	private buffer: string;
	private hasSkippedHeader = false;

	constructor(private headerFilter: string) {
		this.buffer = '';
	}

	transform(chunk: string, controller: TransformStreamDefaultController<string>) {
		this.buffer += chunk;
		let lines = this.buffer.split('\n');
		this.buffer = lines.pop() || ''; // Guardar el último fragmento para la próxima chunk
		for (let line of lines) {
			if (!this.hasSkippedHeader) {
				const isHeader = line.startsWith(this.headerFilter);
				if (isHeader) {
					this.hasSkippedHeader = true;
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