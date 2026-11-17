import { describe, expect, test } from "bun:test"

import { UnsortedSnapshotError, diffSortedSnapshots } from "../../../src/sync/domain/diffSortedSnapshots"
import { lineChunks } from "../lineChunks"

async function diff(base: string[], next: string[], forced: string[] = []) {
	const upserts: string[] = []
	const deletes: string[] = []
	const stats = await diffSortedSnapshots({
		base: lineChunks(base),
		next: lineChunks(next),
		forceUpsertKeys: new Set(forced),
		onUpsert: line => { upserts.push(line) },
		onDelete: key => { deletes.push(key) },
	})
	return { upserts, deletes, stats }
}

describe("diffSortedSnapshots", () => {
	test("emite upsert para filas nuevas y cambiadas, delete para las que desaparecen", async () => {
		const result = await diff(
			["001\tANA", "002\tLUIS", "004\tEVA"],
			["001\tANA", "002\tLUIS ALBERTO", "003\tSOL"],
		)

		expect(result.upserts).toEqual(["002\tLUIS ALBERTO", "003\tSOL"])
		expect(result.deletes).toEqual(["004"])
		expect(result.stats.unchanged).toBe(1)
	})

	test("contra un baseline vacío todo es upsert", async () => {
		const result = await diff([], ["001\tANA", "002\tLUIS"])

		expect(result.upserts).toEqual(["001\tANA", "002\tLUIS"])
		expect(result.deletes).toEqual([])
	})

	test("re-emite como upsert las claves forzadas aunque no hayan cambiado", async () => {
		const result = await diff(["001\tANA", "002\tLUIS"], ["001\tANA", "002\tLUIS"], ["002"])

		expect(result.upserts).toEqual(["002\tLUIS"])
	})

	test("con claves repetidas se queda con la primera aparición", async () => {
		const result = await diff([], ["001\tANA", "001\tOTRA", "002\tLUIS"])

		expect(result.upserts).toEqual(["001\tANA", "002\tLUIS"])
		expect(result.stats.nextDuplicates).toBe(1)
	})

	test("falla si un snapshot no está ordenado en vez de producir un delta incorrecto", async () => {
		await expect(diff([], ["002\tLUIS", "001\tANA"])).rejects.toBeInstanceOf(UnsortedSnapshotError)
	})
})
