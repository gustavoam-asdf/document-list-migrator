// Contra qué se calcula el delta: el snapshot del último día aplicado (S3) o, como
// último recurso, un volcado de lo que hoy tiene la BD (solo lectura, sin downtime).
export type Baseline =
	| { kind: "snapshot"; snapshotId: string }
	| { kind: "database" }

export type SyncTarget = {
	snapshotId: string
	baseline: Baseline
	startedAt: Date
}

export type SyncState =
	// Nunca se aplicó un snapshot, o el estado de la BD dejó de ser conocido.
	| { kind: "unknown" }
	| { kind: "idle"; appliedSnapshotId: string }
	// Hay un delta a medio aplicar: se retoma antes de cualquier otra cosa.
	| { kind: "applying"; target: SyncTarget }

// Ordenable lexicográficamente: el más nuevo es el mayor.
export function snapshotIdFor(date: Date): string {
	return date.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z")
}
