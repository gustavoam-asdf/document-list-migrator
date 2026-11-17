import type { PsqlEnv } from "./sync/infrastructure/PostgresDatabaseBaseline"

function required(name: string): string {
	const value = Bun.env[name]
	if (!value) {
		throw new Error(`falta la variable ${name}`)
	}
	return value
}

// Misma BD que primarySql, en variables PG* para los subprocesos psql.
export function primaryPsqlEnv(): PsqlEnv {
	return {
		PGHOST: required("DATABASE_HOST"),
		PGPORT: required("DATABASE_PORT"),
		PGUSER: required("DATABASE_USER"),
		PGPASSWORD: required("DATABASE_PASSWORD"),
		PGDATABASE: required("DATABASE_NAME"),
		// Cifra sin verificar el CA, igual que primarySql (ver db.ts).
		PGSSLMODE: Bun.env.DATABASE_USE_SSL === "true" ? "require" : "disable",
		PGCONNECT_TIMEOUT: "30",
	}
}
