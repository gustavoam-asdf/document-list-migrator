import { mkdirSync } from "node:fs"
import { dirname } from "node:path"

export type RejectEntry = {
	ts: string
	source: string
	rawLine?: string
	error: string
	[key: string]: unknown
}

// Append-only JSONL writer. Cada archivo debe tener un único productor
// (split escribe parse-*, cada worker escribe su propio copy-*-{worker}.jsonl).
// Pensado para fallos raros (<0.01%), por eso no se rota.
type BunFileSink = ReturnType<ReturnType<typeof Bun.file>["writer"]>

export class RejectsWriter {
	private writer: BunFileSink
	private count = 0
	private closed = false

	constructor(private readonly filePath: string) {
		mkdirSync(dirname(filePath), { recursive: true })
		this.writer = Bun.file(filePath).writer({ highWaterMark: 64 * 1024 })
	}

	write(entry: Omit<RejectEntry, "ts">): void {
		if (this.closed) {
			return
		}
		const line = { ...entry, ts: new Date().toISOString() }
		this.writer.write(JSON.stringify(line) + "\n")
		this.count++
	}

	hasEntries(): boolean {
		return this.count > 0
	}

	async flush(): Promise<void> {
		await this.writer.flush()
	}

	async close(): Promise<number> {
		if (this.closed) {
			return this.count
		}
		this.closed = true
		await this.writer.end()
		return this.count
	}

	getCount(): number {
		return this.count
	}

	getPath(): string {
		return this.filePath
	}
}
