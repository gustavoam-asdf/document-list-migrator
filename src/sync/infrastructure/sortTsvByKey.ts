import { $ } from "bun"
import fs from "node:fs/promises"

import { SORT_BUFFER, SORT_PARALLEL } from "../../constants"

// Sort externo (spill a disco) por la primera columna en orden de bytes (LC_ALL=C),
// el mismo orden que usa el diff. `-s` conserva la primera aparición de claves repetidas.
export async function sortTsvByKey(input: string, output: string, tmpDir: string): Promise<void> {
	await fs.mkdir(tmpDir, { recursive: true })
	await $`sort -s -t ${"\t"} -k1,1 -S ${SORT_BUFFER} --parallel=${SORT_PARALLEL} -T ${tmpDir} -o ${output} ${input}`
		.env({ ...process.env, LC_ALL: "C" })
		.quiet()
	await fs.rm(input)
}
