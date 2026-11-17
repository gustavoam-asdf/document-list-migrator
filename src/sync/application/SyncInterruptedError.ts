export class SyncInterruptedError extends Error {
	constructor(reason: string) {
		super(`sync interrumpido: ${reason}`)
	}
}
