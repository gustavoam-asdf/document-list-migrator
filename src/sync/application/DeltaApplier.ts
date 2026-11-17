import type { DeltaStream } from "../domain/Entity"

export interface DeltaApplier {
	// Aplica el lote y suma su tamaño al progreso del stream en la misma transacción.
	apply(stream: DeltaStream, items: string[]): Promise<void>
}
