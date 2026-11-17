import type { Entity } from "../domain/Entity"
import type { LineChunks } from "../domain/diffSortedSnapshots"

export interface DatabaseBaseline {
	// Volcado ordenado por clave de lo que la tabla tiene hoy.
	dump(entity: Entity): Promise<LineChunks>
}
