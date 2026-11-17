import type { LineChunks } from "../sync/domain/diffSortedSnapshots"
import { readLineChunks } from "./readLineChunks"

export function fileLineChunks(path: string): LineChunks {
	return {
		[Symbol.asyncIterator]: () => readLineChunks(Bun.file(path).stream()),
	}
}
