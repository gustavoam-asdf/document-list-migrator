import type { Sql } from "postgres"

import { errorMessage } from "../../shared/errorMessage"

const LOCK_NAME = "document-list-migrator:sync"
const HEARTBEAT_MS = 20_000

export type ReleaseLock = () => Promise<void>

// Advisory lock de sesión: evita dos corridas a la vez (programada + manual). Si el
// proceso muere, Postgres lo libera al cerrarse la conexión.
export async function acquireSyncLock(sql: Sql): Promise<ReleaseLock | null> {
	const connection = await sql.reserve()
	const rows = await connection<{ locked: unknown }[]>`SELECT pg_try_advisory_lock(hashtext(${LOCK_NAME})) AS locked`
	if (rows[0]?.locked !== true) {
		connection.release()
		return null
	}

	// Mantiene viva la conexión reservada (y con ella el lock) durante toda la corrida.
	const heartbeat = setInterval(() => {
		connection`SELECT 1`.catch(error => console.warn(`[sync] heartbeat del lock falló: ${errorMessage(error)}`))
	}, HEARTBEAT_MS)

	return async () => {
		clearInterval(heartbeat)
		try {
			await connection`SELECT pg_advisory_unlock(hashtext(${LOCK_NAME}))`
		} finally {
			connection.release()
		}
	}
}
