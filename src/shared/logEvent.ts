type SyncStatus = "ok" | "skipped" | "interrupted" | "failed"

export type LogEvent =
	| ({ event: "sync_summary"; status: SyncStatus; durationSec: number } & Record<string, unknown>)
	// Algo falló pero la corrida siguió (sale con 0): el exit code no lo delata.
	| { event: "sync_warning"; level: "error" | "warn"; component: string; message: string }

// Una línea JSON en stdout: contrato estable para metric filters de CloudWatch Logs
// (p. ej. `{ $.event = "sync_summary" && $.rejected > 0 }`). No cambiar nombres de campos
// sin avisar a la infra que los filtra.
export function logEvent(event: LogEvent): void {
	console.log(JSON.stringify(event))
}
