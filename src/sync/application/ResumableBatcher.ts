import type { DeltaStream } from "../domain/Entity"
import { SyncInterruptedError } from "./SyncInterruptedError"

type Params = {
	stream: DeltaStream
	// Items ya confirmados en una corrida anterior: el diff es determinista, se saltan.
	alreadyApplied: number
	batchSize: number
	flush: (stream: DeltaStream, items: string[]) => Promise<void>
	stopReason: () => string | null
	onBatch?: (stream: DeltaStream, applied: number, total: number) => void
}

export class ResumableBatcher {
	#skipRemaining: number
	#buffer: string[] = []
	#applied = 0
	readonly skipped: number

	constructor(private readonly params: Params) {
		this.#skipRemaining = params.alreadyApplied
		this.skipped = params.alreadyApplied
	}

	push(item: string): Promise<void> | undefined {
		if (this.#skipRemaining > 0) {
			this.#skipRemaining--
			return undefined
		}
		this.#buffer.push(item)
		if (this.#buffer.length >= this.params.batchSize) {
			return this.flush()
		}
		return undefined
	}

	async flush(): Promise<void> {
		if (this.#buffer.length === 0) {
			return
		}
		const reason = this.params.stopReason()
		if (reason !== null) {
			throw new SyncInterruptedError(reason)
		}
		const items = this.#buffer
		this.#buffer = []
		await this.params.flush(this.params.stream, items)
		this.#applied += items.length
		this.params.onBatch?.(this.params.stream, items.length, this.skipped + this.#applied)
	}

	get applied(): number {
		return this.#applied
	}
}
