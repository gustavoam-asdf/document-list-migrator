export const ENTITIES = ["dni", "ruc"] as const

export type Entity = (typeof ENTITIES)[number]

export type DeltaOp = "upsert" | "delete"

export type DeltaStream = `${Entity}:${DeltaOp}`

export const DELTA_STREAMS: readonly DeltaStream[] = ["dni:upsert", "dni:delete", "ruc:upsert", "ruc:delete"]

export function deltaStream(entity: Entity, op: DeltaOp): DeltaStream {
	return `${entity}:${op}`
}
