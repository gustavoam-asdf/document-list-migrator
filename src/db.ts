import postgres from 'postgres'

export const sql = postgres({
	host: Bun.env.DATABASE_HOST,
	port: Number(Bun.env.DATABASE_PORT),
	database: Bun.env.DATABASE_NAME,
	username: Bun.env.DATABASE_USER,
	password: Bun.env.DATABASE_PASS,
	ssl: true,
}) 