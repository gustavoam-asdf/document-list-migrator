import type { DatabaseBaseline } from "../../../src/sync/application/DatabaseBaseline"
import type { DeltaApplier } from "../../../src/sync/application/DeltaApplier"
import type { SyncStateRepository } from "../../../src/sync/application/SyncStateRepository"
import { ENTITIES, type DeltaOp, type DeltaStream, type Entity } from "../../../src/sync/domain/Entity"
import type { Baseline, SyncState, SyncTarget } from "../../../src/sync/domain/SyncState"
import type { LineChunks } from "../../../src/sync/domain/diffSortedSnapshots"
import { rowKey } from "../../../src/sync/domain/rowKey"
import { lineChunks } from "../lineChunks"

const STREAM_PARTS: Record<DeltaStream, [Entity, DeltaOp]> = {
	"dni:upsert": ["dni", "upsert"],
	"dni:delete": ["dni", "delete"],
	"ruc:upsert": ["ruc", "upsert"],
	"ruc:delete": ["ruc", "delete"],
}

const emptyProgress =(): Record<DeltaStream, number> => ({ "dni:upsert": 0, "dni:delete": 0, "ruc:upsert": 0, "ruc:delete": 0 })

// Doble de la BD: tablas, estado de sync y progreso, con commit atómico por lote.
export class InMemoryDatabase implements SyncStateRepository, DeltaApplier, DatabaseBaseline {
	readonly tables: Record<Entity, Map<string, string>> = { dni: new Map(), ruc: new Map() }
	#state: SyncState = { kind: "unknown" }
	#progress = emptyProgress()
	#clock = 0
	appliedItems = 0
	dumps = 0
	// Simula un corte de conexión al llegar a este número de lote (1-based).
	failOnBatch: number | undefined
	#batches = 0

	static withRows(rows: Partial<Record<Entity, string[]>>): InMemoryDatabase {
		const db = new InMemoryDatabase()
		for (const entity of ENTITIES) {
			for (const line of rows[entity] ?? []) {
				db.tables[entity].set(rowKey(line), line)
			}
		}
		return db
	}

	rows(entity: Entity): string[] {
		return [...this.tables[entity].values()].sort()
	}

	get state(): SyncState {
		return this.#state
	}

	async read(): Promise<SyncState> {
		return this.#state
	}

	async beginTarget(snapshotId: string, baseline: Baseline): Promise<SyncTarget> {
		const target = { snapshotId, baseline, startedAt: new Date(++this.#clock) }
		this.#state = { kind: "applying", target }
		this.#progress = emptyProgress()
		return target
	}

	async resetProgress(): Promise<void> {
		this.#progress = emptyProgress()
	}

	async progress(): Promise<Record<DeltaStream, number>> {
		return { ...this.#progress }
	}

	async forcedKeys(): Promise<Set<string>> {
		return new Set()
	}

	async complete(target: SyncTarget): Promise<void> {
		this.#state = { kind: "idle", appliedSnapshotId: target.snapshotId }
		this.#progress = emptyProgress()
	}

	async markUnknown(): Promise<void> {
		this.#state = { kind: "unknown" }
	}

	async apply(stream: DeltaStream, items: string[]): Promise<void> {
		this.#batches++
		if (this.#batches === this.failOnBatch) {
			throw new Error("connection terminated")
		}
		const [entity, op] = STREAM_PARTS[stream]
		for (const item of items) {
			if (op === "upsert") {
				this.tables[entity].set(rowKey(item), item)
			} else {
				this.tables[entity].delete(item)
			}
		}
		this.#progress[stream] += items.length
		this.appliedItems += items.length
	}

	async dump(entity: Entity): Promise<LineChunks> {
		this.dumps++
		return lineChunks(this.rows(entity))
	}
}
