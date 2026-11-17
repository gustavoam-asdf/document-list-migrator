import type { Baseline, SyncState, SyncTarget } from "../domain/SyncState"
import type { DeltaStream, Entity } from "../domain/Entity"

export interface SyncStateRepository {
	read(): Promise<SyncState>
	// Fija el objetivo, reinicia el progreso a cero y sella `startedAt` con el reloj de la BD
	// (el mismo con el que se fechan los rechazos).
	beginTarget(snapshotId: string, baseline: Baseline): Promise<SyncTarget>
	resetProgress(): Promise<void>
	progress(): Promise<Record<DeltaStream, number>>
	// Claves rechazadas antes de que empezara el objetivo actual: estable mientras dure.
	forcedKeys(entity: Entity): Promise<Set<string>>
	complete(target: SyncTarget): Promise<void>
	markUnknown(): Promise<void>
}
