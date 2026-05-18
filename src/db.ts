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

export async function getMaxConnections(sql: Sql): Promise<number> {
	const rows = await sql<{ max_connections: string }[]>`SHOW max_connections`
	return parseInt(rows[0]!.max_connections, 10)
}
