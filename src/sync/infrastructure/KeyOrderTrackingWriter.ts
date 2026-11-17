import fs from "node:fs/promises"

import { rowKey } from "../domain/rowKey"
import { sortTsvByKey } from "./sortTsvByKey"

type FileSink = ReturnType<ReturnType<typeof Bun.file>["writer"]>

// Cada cuántos bytes se espera a que el disco alcance al productor (backpressure).
const FLUSH_EVERY_BYTES = 4 * 1024 * 1024

// Escribe filas COPY-text y detecta si ya llegaron ordenadas por clave (orden de
// bytes): en ese caso el sort externo se evita.
export class KeyOrderTrackingWriter {
	readonly #writer: FileSink
	#previousKey: string | undefined
	#sorted = true
	#pendingBytes = 0
	rows = 0

	constructor(private readonly path: string) {
		this.#writer = Bun.file(path).writer({ highWaterMark: 8 * 1024 * 1024 })
	}

	// Devuelve una promesa cuando hay que esperar al disco: el llamador debe esperarla;
	// si no, un productor más rápido que el disco acumula todo en memoria.
	write(row: string): Promise<void> | undefined {
		const key = rowKey(row)
		if (this.#previousKey !== undefined && key < this.#previousKey) {
			this.#sorted = false
		}
		this.#previousKey = key
		const written = this.#writer.write(`${row}\n`)
		this.rows++
		if (typeof written !== "number") {
			// El sink ya pide esperar: se respeta tal cual.
			this.#pendingBytes = 0
			return written.then(() => undefined)
		}
		this.#pendingBytes += written
		if (this.#pendingBytes < FLUSH_EVERY_BYTES) {
			return undefined
		}
		this.#pendingBytes = 0
		return this.#flush()
	}

	async #flush(): Promise<void> {
		await this.#writer.flush()
	}

	// Cierra y deja en `output` el archivo ordenado; devuelve si hizo falta ordenar.
	async finish(output: string, tmpDir: string): Promise<{ sortedExternally: boolean }> {
		await this.#writer.end()
		if (this.#sorted) {
			await fs.rename(this.path, output)
			return { sortedExternally: false }
		}
		await sortTsvByKey(this.path, output, tmpDir)
		return { sortedExternally: true }
	}
}
