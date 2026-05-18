import postgres, { type Sql } from "postgres"

// Keepalive: detectar sockets muertos en ~1-2 min en lugar de las horas
// que tarda el default del kernel. Sin esto, si un worker muere su COPY
// queda `idle in transaction` hasta que TCP timeout caiga.
const KEEPALIVE_CONNECTION = {
	keepalives: 1,
	keepalives_idle: 60,
	keepalives_interval: 10,
	keepalives_count: 3,
} as const

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
	connection: KEEPALIVE_CONNECTION,
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
	connection: KEEPALIVE_CONNECTION,
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
export async function verifyTables(
	sql: Sql,
	dbName: string,
	tableNames: string[],
): Promise<void> {
	const rows = await sql<{ table_name: string }[]>`
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = ANY(${tableNames})
	`
	const found = new Set(rows.map(r => r.table_name))
	const missing = tableNames.filter(t => !found.has(t))
	if (missing.length > 0) {
		throw new Error(`[${dbName}] missing required tables: ${missing.join(", ")}`)
	}
	console.log(`[${dbName}] tables verified: ${tableNames.join(", ")}`)
}
