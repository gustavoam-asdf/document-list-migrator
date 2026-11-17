import type { Entity } from "../domain/Entity"
import type { LineChunks } from "../domain/diffSortedSnapshots"
import type { PreparedSnapshot } from "./SnapshotSource"

export interface SnapshotStore {
	exists(id: string): Promise<boolean>
	read(id: string, entity: Entity): LineChunks
	save(snapshot: PreparedSnapshot): Promise<void>
	pruneExcept(keep: ReadonlySet<string>): Promise<void>
}
