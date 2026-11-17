import type { Entity } from "../domain/Entity"
import type { LineChunks } from "../domain/diffSortedSnapshots"

export type PreparedSnapshot = {
	id: string
	files: Record<Entity, string>
	rows: Record<Entity, number>
}

export interface SnapshotSource {
	// Descarga el padrón y deja un archivo ordenado por clave por entidad.
	prepare(id: string): Promise<PreparedSnapshot>
	read(snapshot: PreparedSnapshot, entity: Entity): LineChunks
}
