import fs from "node:fs/promises"

import { rowKey } from "../domain/rowKey"
import { sortTsvByKey } from "./sortTsvByKey"

type FileSink = ReturnType<ReturnType<typeof Bun.file>["writer"]>

// Escribe filas COPY-text y detecta si ya llegaron ordenadas por clave (orden de
// bytes): en ese caso el sort externo se evita.
export class KeyOrderTrackingWriter {
	readonly #writer: FileSink
	#previousKey: string | undefined
	#sorted = true
	rows = 0

	constructor(private readonly path: string) {
		this.#writer = Bun.file(path).writer({ highWaterMark: 8 * 1024 * 1024 })
	}

	write(row: string): void {
		const key = rowKey(row)
		if (this.#previousKey !== undefined && key < this.#previousKey) {
			this.#sorted = false
		}
		this.#previousKey = key
		this.#writer.write(`${row}\n`)
		this.rows++
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
