import type { PreparedSnapshot } from "../../../src/sync/application/SnapshotSource"
import type { SnapshotStore } from "../../../src/sync/application/SnapshotStore"
import type { Entity } from "../../../src/sync/domain/Entity"
import type { SourceVersion } from "../../../src/sync/domain/SourceVersion"
import type { LineChunks } from "../../../src/sync/domain/diffSortedSnapshots"
import { lineChunks } from "../lineChunks"
import type { PadronSource } from "./PadronSource"

export class InMemorySnapshotStore implements SnapshotStore {
	readonly snapshots = new Map<string, Record<Entity, string[]>>()
	readonly #versions = new Map<string, SourceVersion>()

	constructor(private readonly source: PadronSource) {}

	async exists(id: string): Promise<boolean> {
		return this.snapshots.has(id)
	}

	read(id: string, entity: Entity): LineChunks {
		const snapshot = this.snapshots.get(id)
		if (!snapshot) {
			throw new Error(`snapshot ${id} no existe`)
		}
		return lineChunks(snapshot[entity])
	}

	async save(snapshot: PreparedSnapshot, sourceVersion: SourceVersion): Promise<void> {
		this.snapshots.set(snapshot.id, this.source.rowsOf(snapshot))
		this.#versions.set(snapshot.id, sourceVersion)
	}

	async sourceVersion(id: string): Promise<SourceVersion> {
		return this.snapshots.has(id) ? (this.#versions.get(id) ?? { kind: "unknown" }) : { kind: "unknown" }
	}

	async pruneExcept(keep: ReadonlySet<string>): Promise<void> {
		for (const id of this.snapshots.keys()) {
			if (!keep.has(id)) {
				this.snapshots.delete(id)
			}
		}
	}
}
