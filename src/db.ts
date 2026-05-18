import postgres, { type Sql } from "postgres"

// NOTA: los TCP keepalives NO se setean acá. El campo `connection` de
// postgres.js manda los keys como startup parameters al servidor (como GUCs),
// y `keepalives*` no son GUCs válidos — son params de libpq client-side.
// La forma correcta de mitigar el problema de "COPY zombie idle in transaction"
// es del lado del servidor Postgres:
//   ALTER SYSTEM SET tcp_keepalives_idle = '60';
//   ALTER SYSTEM SET tcp_keepalives_interval = '10';
//   ALTER SYSTEM SET tcp_keepalives_count = '3';
//   ALTER SYSTEM SET idle_in_transaction_session_timeout = '15min';
//   SELECT pg_reload_conf();
// Esto hace que el server detecte clientes muertos en ~1-2 min y cierre
// las transacciones huérfanas. Sin esto, hay que esperar al TCP timeout
// default del kernel (horas).

export const primarySql = postgres({
	host: Bun.env.DATABASE_HOST,
	port: Number(Bun.env.DATABASE_PORT),
	database: Bun.env.DATABASE_NAME,
	username: Bun.env.DATABASE_USER,
	password: Bun.env.DATABASE_PASSWORD,
	ssl: Bun.env.DATABASE_USE_SSL === "true",
	max: 5,
	idle_timeout: 30,
	connect_timeout: 30,
	prepare: false,
	fetch_types: false,
})

export const secondarySql = postgres({
	host: Bun.env.DATABASE_HOST,
	port: Number(Bun.env.DATABASE_PORT),
	database: Bun.env.DATABASE_NAME_SECONDARY,
	username: Bun.env.DATABASE_USER,
	password: Bun.env.DATABASE_PASSWORD,
	ssl: Bun.env.DATABASE_USE_SSL === "true",
	max: 5,
	idle_timeout: 30,
	connect_timeout: 30,
	prepare: false,
	fetch_types: false,
})

export type PingResult = {
	pid: number
	version: string
	maxConnections: number
	latencyMs: number
}

// Round-trip "ping-pong" antes de tocar la base. Verifica:
// - TCP + auth + driver listos
// - Servidor responde queries (no sólo handshake)
// - Versión y max_connections para logs y dimensionar concurrency
// Reintenta con backoff lineal — cubre "DB arrancando" (Azure puede tardar
// unos segundos tras el cold start del container DB).
export async function pingDb(
	sql: Sql,
	dbName: string,
	opts: { retries?: number; retryDelayMs?: number } = {},
): Promise<PingResult> {
	const retries = opts.retries ?? 3
	const retryDelayMs = opts.retryDelayMs ?? 1000
	let lastErr: unknown

	for (let attempt = 1; attempt <= retries; attempt++) {
		const start = Date.now()
		try {
			const rows = await sql<{ pid: number; version: string; max_connections: string }[]>`
				SELECT
					pg_backend_pid() AS pid,
					current_setting('server_version') AS version,
					current_setting('max_connections') AS max_connections
			`
			const row = rows[0]
			if (!row) {
				throw new Error("ping returned no rows")
			}
			const latencyMs = Date.now() - start
			const result: PingResult = {
				pid: row.pid,
				version: row.version,
				maxConnections: parseInt(row.max_connections, 10),
				latencyMs,
			}
			console.log(
				`[${dbName}] ping OK | pid=${result.pid} version=${result.version} ` +
				`max_connections=${result.maxConnections} latency=${result.latencyMs}ms`,
			)
			return result
		} catch (err) {
			lastErr = err
			console.warn(
				`[${dbName}] ping attempt ${attempt}/${retries} failed: ${(err as Error).message}`,
			)
			if (attempt < retries) {
				await new Promise(r => setTimeout(r, retryDelayMs * attempt))
			}
		}
	}
	throw new Error(
		`[${dbName}] ping failed after ${retries} attempts: ${(lastErr as Error)?.message ?? lastErr}`,
	)
}

// Verifica que existan las tablas que vamos a tocar. Falla fast si el schema
// no está desplegado o si conectamos a la base equivocada.
// Una query por tabla para evitar problemas de encoding de arrays con
// `fetch_types: false`. Para 2-3 tablas el overhead es trivial.
export async function verifyTables(
	sql: Sql,
	dbName: string,
	tableNames: string[],
): Promise<void> {
	const missing: string[] = []
	for (const name of tableNames) {
		const rows = await sql<{ exists: boolean }[]>`
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = ${name}
			) AS exists
		`
		if (!rows[0]?.exists) {
			missing.push(name)
		}
	}
	if (missing.length > 0) {
		throw new Error(`[${dbName}] missing required tables: ${missing.join(", ")}`)
	}
	console.log(`[${dbName}] tables verified: ${tableNames.join(", ")}`)
}
