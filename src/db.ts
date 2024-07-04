import postgres from 'postgres'

export const primarySql = postgres({
	host: Bun.env.DATABASE_HOST,
	port: Number(Bun.env.DATABASE_PORT),
	database: Bun.env.DATABASE_NAME,
	username: Bun.env.DATABASE_USER,
	password: Bun.env.DATABASE_PASSWORD,
	ssl: true,
})

export const secondarySql = postgres({
	host: Bun.env.DATABASE_HOST,
	port: Number(Bun.env.DATABASE_PORT),
	database: Bun.env.DATABASE_NAME_SECONDARY,
	username: Bun.env.DATABASE_USER,
	password: Bun.env.DATABASE_PASSWORD,
	ssl: true,
})