import type { PreparedSnapshot } from "../../../src/sync/application/SnapshotSource"
import type { SnapshotStore } from "../../../src/sync/application/SnapshotStore"
import type { Entity } from "../../../src/sync/domain/Entity"
import type { LineChunks } from "../../../src/sync/domain/diffSortedSnapshots"
import { lineChunks } from "../lineChunks"
import type { PadronSource } from "./PadronSource"

export class InMemorySnapshotStore implements SnapshotStore {
	readonly snapshots = new Map<string, Record<Entity, string[]>>()

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

	async save(snapshot: PreparedSnapshot): Promise<void> {
		this.snapshots.set(snapshot.id, this.source.rowsOf(snapshot))
	}

	async pruneExcept(keep: ReadonlySet<string>): Promise<void> {
		for (const id of this.snapshots.keys()) {
			if (!keep.has(id)) {
				this.snapshots.delete(id)
			}
		}
	}
}
