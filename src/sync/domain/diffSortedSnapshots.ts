import { rowKey } from "./rowKey"

export type LineChunks = AsyncIterable<string[]>

export type DiffStats = {
	upserts: number
	deletes: number
	unchanged: number
	baseDuplicates: number
	nextDuplicates: number
}

type Sink = (value: string) => void | Promise<void>

export type DiffParams = {
	base: LineChunks
	next: LineChunks
	// Claves que se re-emiten como upsert aunque no hayan cambiado (filas que la BD rechazó antes).
	forceUpsertKeys: ReadonlySet<string>
	onUpsert: Sink
	onDelete: Sink
}

// Sin esto el merge avanza solo con microtareas y no deja correr timers (heartbeat del
// lock, muestreo de memoria) durante todo el diff de una tabla.
const YIELD_EVERY_CHUNKS = 256

function yieldToEventLoop(): Promise<void> {
	return new Promise(resolve => setImmediate(resolve))
}

export class UnsortedSnapshotError extends Error {
	constructor(side: "base" | "next", previousKey: string, key: string) {
		super(`snapshot ${side} no está ordenado: "${key}" aparece después de "${previousKey}"`)
	}
}

// Cursor sobre chunks de líneas ordenadas por clave (orden de bytes). Salta claves
// duplicadas quedándose con la primera aparición, igual en ambos lados, para que el
// resultado sea determinista entre corridas.
class SortedCursor {
	#iterator: AsyncIterator<string[]>
	#chunk: string[] = []
	#index = 0
	#chunksRead = 0
	#previousKey: string | undefined
	line: string | undefined
	key: string | undefined
	duplicates = 0

	constructor(
		chunks: LineChunks,
		private readonly side: "base" | "next",
	) {
		this.#iterator = chunks[Symbol.asyncIterator]()
	}

	async advance(): Promise<void> {
		for (;;) {
			while (this.#index < this.#chunk.length) {
				const line = this.#chunk[this.#index++]!
				if (line.length === 0) {
					continue
				}
				const key = rowKey(line)
				if (this.#previousKey !== undefined) {
					if (key === this.#previousKey) {
						this.duplicates++
						continue
					}
					if (key < this.#previousKey) {
						throw new UnsortedSnapshotError(this.side, this.#previousKey, key)
					}
				}
				this.#previousKey = key
				this.line = line
				this.key = key
				return
			}
			if (++this.#chunksRead % YIELD_EVERY_CHUNKS === 0) {
				await yieldToEventLoop()
			}
			const result = await this.#iterator.next()
			if (result.done) {
				this.line = undefined
				this.key = undefined
				return
			}
			this.#chunk = result.value
			this.#index = 0
		}
	}
}

// Merge-join de dos snapshots ordenados por clave: emite upsert para filas nuevas o
// cambiadas y delete para claves que desaparecieron. Memoria O(chunk), no O(snapshot).
export async function diffSortedSnapshots(params: DiffParams): Promise<DiffStats> {
	const { forceUpsertKeys, onUpsert, onDelete } = params
	const base = new SortedCursor(params.base, "base")
	const next = new SortedCursor(params.next, "next")
	const stats: DiffStats = { upserts: 0, deletes: 0, unchanged: 0, baseDuplicates: 0, nextDuplicates: 0 }

	const emitUpsert = async (line: string) => {
		stats.upserts++
		const pending = onUpsert(line)
		if (pending) {
			await pending
		}
	}
	const emitDelete = async (key: string) => {
		stats.deletes++
		const pending = onDelete(key)
		if (pending) {
			await pending
		}
	}

	await base.advance()
	await next.advance()

	while (base.key !== undefined || next.key !== undefined) {
		if (next.key === undefined || (base.key !== undefined && base.key < next.key)) {
			await emitDelete(base.key!)
			await base.advance()
			continue
		}
		if (base.key === undefined || next.key < base.key) {
			await emitUpsert(next.line!)
			await next.advance()
			continue
		}
		if (base.line !== next.line || forceUpsertKeys.has(next.key)) {
			await emitUpsert(next.line!)
		} else {
			stats.unchanged++
		}
		await base.advance()
		await next.advance()
	}

	stats.baseDuplicates = base.duplicates
	stats.nextDuplicates = next.duplicates
	return stats
}
