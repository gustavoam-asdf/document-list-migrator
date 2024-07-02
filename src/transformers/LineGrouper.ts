export class LineGrouper implements Transformer<string, string[]> {
	private buffer: string[];
	constructor(protected size: number) {
		this.buffer = [];
	}

	transform(chunk: string, controller: TransformStreamDefaultController<string[]>) {
		this.buffer.push(chunk);
		if (this.buffer.length >= this.size) {
			controller.enqueue(this.buffer);
			this.buffer = [];
		}
	}

	flush(controller: TransformStreamDefaultController<string[]>) {
		if (this.buffer.length > 0) {
			controller.enqueue(this.buffer);
		}
	}
}