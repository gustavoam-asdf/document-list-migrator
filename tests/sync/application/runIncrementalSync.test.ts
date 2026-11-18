import { describe, expect, test } from "bun:test"

import { runIncrementalSync, type SyncDeps } from "../../../src/sync/application/runIncrementalSync"
import { SyncInterruptedError } from "../../../src/sync/application/SyncInterruptedError"
import { InMemoryDatabase } from "./InMemoryDatabase"
import { InMemorySnapshotStore } from "./InMemorySnapshotStore"
import { PadronSource, type Padron } from "./PadronSource"

const dni = (id: string, nombre: string) => `${id}\t${nombre}`
const ruc = (id: string, razonSocial: string) => `${id}\t${razonSocial}\tACTIVO`

const DAY_1: Padron = {
	dni: [dni("00000001", "ANA"), dni("00000002", "LUIS"), dni("00000003", "EVA")],
	ruc: [ruc("20000000001", "ALFA SAC"), ruc("20000000002", "BETA SAC")],
}

const DAY_2: Padron = {
	dni: [dni("00000001", "ANA"), dni("00000002", "LUIS ALBERTO"), dni("00000004", "SOL")],
	ruc: [ruc("20000000001", "ALFA SAC"), ruc("20000000003", "GAMMA SAC")],
}

function setup(db: InMemoryDatabase, padron: Padron) {
	const source = new PadronSource(padron)
	const store = new InMemorySnapshotStore(source)
	let day = 0
	const deps = (overrides: Partial<SyncDeps> = {}): SyncDeps => ({
		source,
		store,
		state: db,
		applier: db,
		databaseBaseline: db,
		batchSize: 1,
		forceDatabaseBaseline: false,
		now: () => new Date(Date.UTC(2026, 10, 17 + day++)),
		stopReason: () => null,
		log: () => {},
		warn: () => {},
		...overrides,
	})
	return { source, store, deps }
}

async function runApplied(deps: SyncDeps) {
	const outcome = await runIncrementalSync(deps)
	if (outcome.kind !== "applied") {
		throw new Error(`se esperaba aplicar un snapshot y salió ${outcome.kind}`)
	}
	return outcome
}

function expectDatabaseEquals(db: InMemoryDatabase, padron: Padron) {
	expect(db.rows("dni")).toEqual([...padron.dni].sort())
	expect(db.rows("ruc")).toEqual([...padron.ruc].sort())
}

describe("runIncrementalSync", () => {
	test("primera corrida sobre una BD ya cargada: compara contra la BD y solo escribe las diferencias", async () => {
		const db = InMemoryDatabase.withRows(DAY_1)
		const { source, deps } = setup(db, DAY_1)
		source.today = DAY_2

		const result = await runApplied(deps())

		expect(result.baseline).toEqual({ kind: "database" })
		expectDatabaseEquals(db, DAY_2)
		// dni: 1 cambio + 1 alta + 1 baja; ruc: 1 alta + 1 baja
		expect(db.appliedItems).toBe(5)
		expect(db.state).toEqual({ kind: "idle", appliedSnapshotId: result.snapshotId })
	})

	test("día siguiente: usa el snapshot del día anterior y no vuelve a leer la BD", async () => {
		const db = new InMemoryDatabase()
		const { source, deps } = setup(db, DAY_1)
		const first = await runApplied(deps())
		const dumpsAfterBootstrap = db.dumps
		source.today = DAY_2

		const second = await runApplied(deps())

		expect(second.baseline).toEqual({ kind: "snapshot", snapshotId: first.snapshotId })
		expect(db.dumps).toBe(dumpsAfterBootstrap)
		expectDatabaseEquals(db, DAY_2)
	})

	test("conserva solo el snapshot aplicado y el anterior", async () => {
		const db = new InMemoryDatabase()
		const { source, store, deps } = setup(db, DAY_1)
		const first = await runApplied(deps())
		source.today = DAY_2
		const second = await runApplied(deps())
		source.today = DAY_1

		const third = await runApplied(deps())

		expect([...store.snapshots.keys()].sort()).toEqual([second.snapshotId, third.snapshotId])
		expect(store.snapshots.has(first.snapshotId)).toBe(false)
	})

	test("si la corrida se corta a mitad, la siguiente retoma sin reaplicar los lotes ya confirmados", async () => {
		const db = new InMemoryDatabase()
		const { source, deps } = setup(db, DAY_1)
		await runIncrementalSync(deps())
		source.today = DAY_2
		const appliedBefore = db.appliedItems
		db.failOnBatch = db.appliedItems + 3

		await expect(runIncrementalSync(deps())).rejects.toThrow("connection terminated")
		expect(db.state.kind).toBe("applying")

		db.failOnBatch = undefined
		await runIncrementalSync(deps())

		expectDatabaseEquals(db, DAY_2)
		// El delta día 1 → día 2 tiene 5 items: cada uno se aplicó exactamente una vez.
		expect(db.appliedItems - appliedBefore).toBe(5)
	})

	test("una señal de parada corta entre lotes y deja el progreso listo para retomar", async () => {
		const db = new InMemoryDatabase()
		const { source, deps } = setup(db, DAY_1)
		await runIncrementalSync(deps())
		source.today = DAY_2
		let batches = 0

		const interrupted = runIncrementalSync(deps({ stopReason: () => (batches++ >= 2 ? "SIGTERM" : null) }))

		await expect(interrupted).rejects.toBeInstanceOf(SyncInterruptedError)
		await runIncrementalSync(deps())
		expectDatabaseEquals(db, DAY_2)
	})

	test("si falla el borrado de snapshots viejos, avisa y la corrida termina bien", async () => {
		const db = new InMemoryDatabase()
		const { store, deps } = setup(db, DAY_1)
		store.pruneExcept = async () => {
			throw new Error("AccessDenied")
		}
		const warnings: string[] = []

		const result = await runApplied(deps({ warn: (component, message) => { warnings.push(`${component}: ${message}`) } }))

		expect(db.state).toEqual({ kind: "idle", appliedSnapshotId: result.snapshotId })
		expect(warnings).toEqual(["s3-prune: no se pudieron borrar snapshots viejos: AccessDenied"])
	})

	test("si el snapshot aplicado ya no está en el store, cae al volcado de la BD sin perder datos", async () => {
		const db = new InMemoryDatabase()
		const { source, store, deps } = setup(db, DAY_1)
		const first = await runApplied(deps())
		store.snapshots.delete(first.snapshotId)
		source.today = DAY_2

		const result = await runApplied(deps())

		expect(result.baseline).toEqual({ kind: "database" })
		expectDatabaseEquals(db, DAY_2)
	})

	test("si SUNAT no publicó un padrón nuevo, no descarga ni escribe en la BD", async () => {
		const db = new InMemoryDatabase()
		const { source, store, deps } = setup(db, DAY_1)
		const first = await runApplied(deps())
		const appliedBefore = db.appliedItems

		const outcome = await runIncrementalSync(deps())

		expect(outcome).toEqual({ kind: "unchanged", resumedSnapshotId: null, appliedSnapshotId: first.snapshotId })
		expect(source.downloads).toBe(1)
		expect(db.appliedItems).toBe(appliedBefore)
		expect([...store.snapshots.keys()]).toEqual([first.snapshotId])
	})

	test("si no se puede leer la huella del padrón, la corrida se hace completa", async () => {
		const db = new InMemoryDatabase()
		const { source, deps } = setup(db, DAY_1)
		await runApplied(deps())
		source.versionUnavailable = true

		const outcome = await runIncrementalSync(deps())

		expect(outcome.kind).toBe("applied")
		expect(source.downloads).toBe(2)
	})

	test("forzar el baseline contra la BD reconcilia aunque el padrón no haya cambiado", async () => {
		const db = new InMemoryDatabase()
		const { deps } = setup(db, DAY_1)
		await runApplied(deps())
		db.tables.dni.delete("00000002")

		const result = await runApplied(deps({ forceDatabaseBaseline: true }))

		expect(result.baseline).toEqual({ kind: "database" })
		expectDatabaseEquals(db, DAY_1)
	})

	test("un delta a medio aplicar se termina aunque el padrón no haya cambiado", async () => {
		const db = new InMemoryDatabase()
		const { source, deps } = setup(db, DAY_1)
		await runApplied(deps())
		source.today = DAY_2
		db.failOnBatch = db.appliedItems + 3
		await expect(runIncrementalSync(deps())).rejects.toThrow("connection terminated")
		db.failOnBatch = undefined
		const pending = db.state
		if (pending.kind !== "applying") {
			throw new Error("se esperaba un delta a medio aplicar")
		}
		const interrupted = pending.target.snapshotId

		const outcome = await runIncrementalSync(deps())

		expect(outcome).toEqual({ kind: "unchanged", resumedSnapshotId: interrupted, appliedSnapshotId: interrupted })
		expectDatabaseEquals(db, DAY_2)
	})
})
