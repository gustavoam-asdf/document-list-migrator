import { ENTITIES, deltaStream, type Entity } from "../domain/Entity"
import { snapshotIdFor, type Baseline, type SyncState, type SyncTarget } from "../domain/SyncState"
import { diffSortedSnapshots, type DiffStats, type LineChunks } from "../domain/diffSortedSnapshots"

import type { DatabaseBaseline } from "./DatabaseBaseline"
import type { DeltaApplier } from "./DeltaApplier"
import { ResumableBatcher } from "./ResumableBatcher"
import type { SnapshotSource } from "./SnapshotSource"
import type { SnapshotStore } from "./SnapshotStore"
import { SyncInterruptedError } from "./SyncInterruptedError"
import { errorMessage } from "../../shared/errorMessage"
import type { SyncStateRepository } from "./SyncStateRepository"

export type SyncDeps = {
	source: SnapshotSource
	store: SnapshotStore
	state: SyncStateRepository
	applier: DeltaApplier
	databaseBaseline: DatabaseBaseline
	batchSize: number
	forceDatabaseBaseline: boolean
	now: () => Date
	stopReason: () => string | null
	log: (message: string) => void
}

export type EntityResult = DiffStats & { skipped: number }

export type SyncResult = {
	resumedSnapshotId: string | null
	snapshotId: string
	baseline: Baseline
	entities: Record<Entity, EntityResult>
}

export async function runIncrementalSync(deps: SyncDeps): Promise<SyncResult> {
	const initial = await deps.state.read()
	const resumedSnapshotId = initial.kind === "applying" ? await resume(deps, initial.target) : null

	const snapshotId = snapshotIdFor(deps.now())
	const prepared = await deps.source.prepare(snapshotId)
	await deps.store.save(prepared)
	deps.log(`[sync] snapshot ${snapshotId} guardado (dni=${prepared.rows.dni} ruc=${prepared.rows.ruc})`)

	const baseline = await chooseBaseline(deps, await deps.state.read())
	const target = await deps.state.beginTarget(snapshotId, baseline)
	deps.log(`[sync] aplicando ${snapshotId} contra ${describeBaseline(baseline)}`)

	const entities = await applyTarget(deps, target, entity => deps.source.read(prepared, entity))
	await deps.state.complete(target)
	await prune(deps, target)

	return { resumedSnapshotId, snapshotId, baseline, entities }
}

async function resume(deps: SyncDeps, target: SyncTarget): Promise<string | null> {
	if (!(await deps.store.exists(target.snapshotId))) {
		deps.log(`[sync] el snapshot ${target.snapshotId} a medio aplicar ya no está en el store: estado desconocido`)
		await deps.state.markUnknown()
		return null
	}

	let effective = target
	if (target.baseline.kind === "snapshot" && !(await deps.store.exists(target.baseline.snapshotId))) {
		deps.log(`[sync] falta el baseline ${target.baseline.snapshotId}: se retoma contra la BD`)
		effective = await deps.state.beginTarget(target.snapshotId, { kind: "database" })
	} else if (target.baseline.kind === "database") {
		// El volcado refleja lo ya aplicado: el progreso anterior no sirve para saltar.
		await deps.state.resetProgress()
	}

	deps.log(`[sync] retomando ${effective.snapshotId} contra ${describeBaseline(effective.baseline)}`)
	await applyTarget(deps, effective, entity => deps.store.read(effective.snapshotId, entity))
	await deps.state.complete(effective)
	return effective.snapshotId
}

async function chooseBaseline(deps: SyncDeps, state: SyncState): Promise<Baseline> {
	if (deps.forceDatabaseBaseline) {
		deps.log("[sync] baseline contra la BD forzado por configuración")
		return { kind: "database" }
	}
	if (state.kind !== "idle") {
		deps.log("[sync] sin snapshot aplicado conocido: baseline contra la BD")
		return { kind: "database" }
	}
	if (!(await deps.store.exists(state.appliedSnapshotId))) {
		deps.log(`[sync] el snapshot aplicado ${state.appliedSnapshotId} no está en el store: baseline contra la BD`)
		return { kind: "database" }
	}
	return { kind: "snapshot", snapshotId: state.appliedSnapshotId }
}

async function applyTarget(
	deps: SyncDeps,
	target: SyncTarget,
	readNext: (entity: Entity) => LineChunks,
): Promise<Record<Entity, EntityResult>> {
	const progress = await deps.state.progress()
	let failedEntity: Entity | null = null
	const stopReason = () => (failedEntity ? `falló ${failedEntity}` : deps.stopReason())

	const settled = await Promise.allSettled(
		ENTITIES.map(async entity => {
			try {
				return await applyEntity(deps, target, entity, readNext(entity), progress, stopReason)
			} catch (error) {
				failedEntity ??= entity
				throw error
			}
		}),
	)

	const errors: unknown[] = settled.flatMap(r => (r.status === "rejected" ? [r.reason] : []))
	if (errors.length > 0) {
		throw errors.find(e => !(e instanceof SyncInterruptedError)) ?? errors[0]
	}

	const [dni, ruc] = settled.map(r => (r.status === "fulfilled" ? r.value : undefined))
	if (!dni || !ruc) {
		throw new Error("applyTarget: resultado incompleto")
	}
	return { dni, ruc }
}

async function applyEntity(
	deps: SyncDeps,
	target: SyncTarget,
	entity: Entity,
	next: LineChunks,
	progress: Awaited<ReturnType<SyncStateRepository["progress"]>>,
	stopReason: () => string | null,
): Promise<EntityResult> {
	const base = target.baseline.kind === "snapshot"
		? deps.store.read(target.baseline.snapshotId, entity)
		: await deps.databaseBaseline.dump(entity)
	// Contra un volcado de la BD los rechazos previos ya aparecen como diferencia.
	const forced = target.baseline.kind === "snapshot"
		? await deps.state.forcedKeys(entity)
		: new Set<string>()

	const batcher = (op: "upsert" | "delete") => {
		const stream = deltaStream(entity, op)
		return new ResumableBatcher({
			stream,
			alreadyApplied: progress[stream],
			batchSize: deps.batchSize,
			flush: (s, items) => deps.applier.apply(s, items),
			stopReason,
			onBatch: (s, applied, total) => deps.log(`[sync] ${s} +${applied} (total ${total})`),
		})
	}
	const upserts = batcher("upsert")
	const deletes = batcher("delete")

	const stats = await diffSortedSnapshots({
		base,
		next,
		forceUpsertKeys: forced,
		onUpsert: line => upserts.push(line),
		onDelete: key => deletes.push(key),
	})
	await upserts.flush()
	await deletes.flush()

	deps.log(
		`[sync] ${entity} listo | upserts=${stats.upserts} deletes=${stats.deletes} sin cambio=${stats.unchanged} ` +
		`| retomados=${upserts.skipped + deletes.skipped} forzados=${forced.size} ` +
		`| duplicados base=${stats.baseDuplicates} nuevo=${stats.nextDuplicates}`,
	)
	return { ...stats, skipped: upserts.skipped + deletes.skipped }
}

async function prune(deps: SyncDeps, target: SyncTarget): Promise<void> {
	const keep = new Set([target.snapshotId])
	if (target.baseline.kind === "snapshot") {
		keep.add(target.baseline.snapshotId)
	}
	try {
		await deps.store.pruneExcept(keep)
	} catch (error) {
		deps.log(`[sync] no se pudieron borrar snapshots viejos: ${errorMessage(error)}`)
	}
}

function describeBaseline(baseline: Baseline): string {
	return baseline.kind === "snapshot" ? `snapshot ${baseline.snapshotId}` : "volcado de la BD"
}
