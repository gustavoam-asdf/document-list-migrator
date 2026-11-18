import type { Entity } from "../domain/Entity"
import type { SourceVersion } from "../domain/SourceVersion"
import type { LineChunks } from "../domain/diffSortedSnapshots"
import type { PreparedSnapshot } from "./SnapshotSource"

export interface SnapshotStore {
	exists(id: string): Promise<boolean>
	read(id: string, entity: Entity): LineChunks
	save(snapshot: PreparedSnapshot, sourceVersion: SourceVersion): Promise<void>
	// De qué publicación salió el snapshot; `unknown` si no existe o no lo registró.
	sourceVersion(id: string): Promise<SourceVersion>
	pruneExcept(keep: ReadonlySet<string>): Promise<void>
}
