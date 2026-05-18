// Distingue errores de conexión/transacción (reintentar mismo batch)
// de errores de fila corrupta (cuartear + escribir a rejects).
const CONNECTION_ERROR_CODES = new Set([
	"ECONNRESET",
	"ECONNREFUSED",
	"ETIMEDOUT",
	"EPIPE",
	"EHOSTUNREACH",
	"ENETUNREACH",
	"CONNECTION_ENDED",
	"CONNECTION_CLOSED",
	"CONNECTION_DESTROYED",
	"57P01", // admin_shutdown
	"57P02", // crash_shutdown
	"57P03", // cannot_connect_now
	"08000", // connection_exception
	"08003", // connection_does_not_exist
	"08006", // connection_failure
	"08001", // sqlclient_unable_to_establish_sqlconnection
	"08004", // sqlserver_rejected_establishment_of_sqlconnection
])

const CONNECTION_ERROR_MESSAGE_HINTS = [
	"connection terminated",
	"connection closed",
	"connection ended",
	"connection reset",
	"socket hang up",
	"server closed",
	"write after end",
]

export function isConnectionError(error: unknown): boolean {
	if (!error || typeof error !== "object") {
		return false
	}

	const code = (error as { code?: unknown }).code
	if (typeof code === "string" && CONNECTION_ERROR_CODES.has(code)) {
		return true
	}

	const message = (error as { message?: unknown }).message
	if (typeof message === "string") {
		const lower = message.toLowerCase()
		for (const hint of CONNECTION_ERROR_MESSAGE_HINTS) {
			if (lower.includes(hint)) {
				return true
			}
		}
	}

	return false
}
