import postgres from 'postgres'

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