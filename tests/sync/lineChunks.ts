import type { LineChunks } from "../../src/sync/domain/diffSortedSnapshots"

// Re-iterable como un archivo: cada iteración arranca desde el principio.
export function lineChunks(lines: readonly string[], chunkSize = 2): LineChunks {
	return {
		async *[Symbol.asyncIterator]() {
			for (let i = 0; i < lines.length; i += chunkSize) {
				yield lines.slice(i, i + chunkSize)
			}
		},
	}
}
