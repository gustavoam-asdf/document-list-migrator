// Flag global + install. Cualquier componente que tenga un bucle largo
// debería consultar shouldExit() entre iteraciones y salir limpiamente.
let shuttingDown = false
let shutdownReason: string | null = null
const listeners: Array<(reason: string) => void> = []

export function shouldExit(): boolean {
	return shuttingDown
}

export function shutdownReasonText(): string | null {
	return shutdownReason
}

export function onShutdown(listener: (reason: string) => void): void {
	listeners.push(listener)
}

function trigger(reason: string) {
	if (shuttingDown) return
	shuttingDown = true
	shutdownReason = reason
	console.warn(`[shutdown] requested by ${reason} — draining gracefully`)
	for (const l of listeners) {
		try { l(reason) } catch (err) { console.error("[shutdown] listener error:", err) }
	}
}

let installed = false

export function installSignalHandlers(): void {
	if (installed) return
	installed = true
	process.on("SIGTERM", () => trigger("SIGTERM"))
	process.on("SIGINT", () => trigger("SIGINT"))
	// Bun también soporta SIGHUP en linux; en windows estos son no-ops.
	if (process.platform !== "win32") {
		process.on("SIGHUP", () => trigger("SIGHUP"))
	}
}
