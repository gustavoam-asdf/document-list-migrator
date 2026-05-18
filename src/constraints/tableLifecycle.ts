import {
	ENABLE_DROP_PK,
	ENABLE_UNLOGGED,
	MAINTENANCE_WORK_MEM,
	MAX_PARALLEL_MAINTENANCE_WORKERS,
} from "../constants"

import type { Sql } from "postgres"

// Especificación canónica de cada tabla que tocamos.
// El schema está checked-in en docker/database/structure.sql; matchea esos nombres.
type TableSpec = {
	name: string
	pkConstraint: string
	pkColumn: string
}

const PERSONA_NATURAL: TableSpec = {
	name: "PersonaNatural",
	pkConstraint: "PersonaNatural_pkey",
	pkColumn: "dni",
}

const PERSONA_JURIDICA: TableSpec = {
	name: "PersonaJuridica",
	pkConstraint: "PersonaJuridica_pkey",
	pkColumn: "ruc",
}

const FK_NAME = "PersonaJuridica_codigoUbigeo_fkey"

// =========================================================================
// PRE-COPY: dejar las tablas listas para insertar lo más rápido posible
//   - DROP FK   → COPY no chequea referencias contra Ubigeo
//   - DROP PK   → COPY no mantiene índice; lo reconstruimos al final
//   - TRUNCATE  → tablas vacías
//   - UNLOGGED  → COPY no escribe WAL (lo paga el SET LOGGED final, una sola vez)
// Cada tabla tiene su propia tx → corren en paralelo (distintas conexiones del pool).
// =========================================================================
export async function prepareTablesForBulkLoad(sql: Sql, dbName: string): Promise<void> {
	const start = Date.now()
	console.log(`[${dbName}] preparing tables for bulk load (drop_pk=${ENABLE_DROP_PK} unlogged=${ENABLE_UNLOGGED})`)

	await Promise.all([
		preparePersonaNatural(sql),
		preparePersonaJuridica(sql),
	])

	console.log(`[${dbName}] tables ready for bulk load in ${Date.now() - start}ms`)
}

async function preparePersonaNatural(sql: Sql): Promise<void> {
	await sql.begin(async tx => {
		if (ENABLE_DROP_PK) {
			await tx`ALTER TABLE ${tx(PERSONA_NATURAL.name)} DROP CONSTRAINT IF EXISTS ${tx(PERSONA_NATURAL.pkConstraint)}`
		}
		await tx`TRUNCATE ${tx(PERSONA_NATURAL.name)}`
		if (ENABLE_UNLOGGED) {
			await tx`ALTER TABLE ${tx(PERSONA_NATURAL.name)} SET UNLOGGED`
		}
	})
}

async function preparePersonaJuridica(sql: Sql): Promise<void> {
	await sql.begin(async tx => {
		await tx`ALTER TABLE ${tx(PERSONA_JURIDICA.name)} DROP CONSTRAINT IF EXISTS ${tx(FK_NAME)}`
		if (ENABLE_DROP_PK) {
			await tx`ALTER TABLE ${tx(PERSONA_JURIDICA.name)} DROP CONSTRAINT IF EXISTS ${tx(PERSONA_JURIDICA.pkConstraint)}`
		}
		await tx`TRUNCATE ${tx(PERSONA_JURIDICA.name)}`
		if (ENABLE_UNLOGGED) {
			await tx`ALTER TABLE ${tx(PERSONA_JURIDICA.name)} SET UNLOGGED`
		}
	})
}

// =========================================================================
// POST-COPY: restaurar a estado de producción
//   - SET LOGGED   → rewrite la tabla a WAL-protected (única vez, secuencial)
//   - ADD PK       → construye índice con maintenance_work_mem alto y paralelo
//   - ADD FK       → re-valida referencias contra Ubigeo
//   - ANALYZE      → estadísticas frescas para el planner
// =========================================================================
export async function finalizeTablesAfterBulkLoad(sql: Sql, dbName: string): Promise<void> {
	const start = Date.now()
	console.log(`[${dbName}] finalizing tables (set_logged=${ENABLE_UNLOGGED} recreate_pk=${ENABLE_DROP_PK})`)

	await Promise.all([
		finalizePersonaNatural(sql, dbName),
		finalizePersonaJuridica(sql, dbName),
	])

	// ANALYZE fuera de tx — las stats se persisten igual y no necesitan atomicidad con el DDL.
	const analyzeStart = Date.now()
	await Promise.all([
		sql`ANALYZE ${sql(PERSONA_NATURAL.name)}`,
		sql`ANALYZE ${sql(PERSONA_JURIDICA.name)}`,
	])
	console.log(`[${dbName}] ANALYZE done in ${Date.now() - analyzeStart}ms`)

	console.log(`[${dbName}] finalize complete in ${Date.now() - start}ms`)
}

async function finalizePersonaNatural(sql: Sql, dbName: string): Promise<void> {
	const t = Date.now()
	await sql.begin(async tx => {
		// SET es una sentencia GUC: no acepta parámetros, va como literal.
		await tx.unsafe(`SET LOCAL maintenance_work_mem = '${MAINTENANCE_WORK_MEM}'`)
		await tx.unsafe(`SET LOCAL max_parallel_maintenance_workers = ${MAX_PARALLEL_MAINTENANCE_WORKERS}`)

		if (ENABLE_UNLOGGED) {
			const tSetLogged = Date.now()
			await tx`ALTER TABLE ${tx(PERSONA_NATURAL.name)} SET LOGGED`
			console.log(`[${dbName}] ${PERSONA_NATURAL.name} SET LOGGED in ${Date.now() - tSetLogged}ms`)
		}
		if (ENABLE_DROP_PK) {
			const tPk = Date.now()
			await tx`ALTER TABLE ${tx(PERSONA_NATURAL.name)} ADD CONSTRAINT ${tx(PERSONA_NATURAL.pkConstraint)} PRIMARY KEY (${tx(PERSONA_NATURAL.pkColumn)})`
			console.log(`[${dbName}] ${PERSONA_NATURAL.name} PK recreated in ${Date.now() - tPk}ms`)
		}
	})
	console.log(`[${dbName}] ${PERSONA_NATURAL.name} finalize done in ${Date.now() - t}ms`)
}

async function finalizePersonaJuridica(sql: Sql, dbName: string): Promise<void> {
	const t = Date.now()
	await sql.begin(async tx => {
		await tx.unsafe(`SET LOCAL maintenance_work_mem = '${MAINTENANCE_WORK_MEM}'`)
		await tx.unsafe(`SET LOCAL max_parallel_maintenance_workers = ${MAX_PARALLEL_MAINTENANCE_WORKERS}`)

		if (ENABLE_UNLOGGED) {
			const tSetLogged = Date.now()
			await tx`ALTER TABLE ${tx(PERSONA_JURIDICA.name)} SET LOGGED`
			console.log(`[${dbName}] ${PERSONA_JURIDICA.name} SET LOGGED in ${Date.now() - tSetLogged}ms`)
		}
		if (ENABLE_DROP_PK) {
			const tPk = Date.now()
			await tx`ALTER TABLE ${tx(PERSONA_JURIDICA.name)} ADD CONSTRAINT ${tx(PERSONA_JURIDICA.pkConstraint)} PRIMARY KEY (${tx(PERSONA_JURIDICA.pkColumn)})`
			console.log(`[${dbName}] ${PERSONA_JURIDICA.name} PK recreated in ${Date.now() - tPk}ms`)
		}
		const tFk = Date.now()
		await tx.unsafe(
			`ALTER TABLE "${PERSONA_JURIDICA.name}" ADD CONSTRAINT "${FK_NAME}" ` +
			`FOREIGN KEY ("codigoUbigeo") REFERENCES "Ubigeo"(codigo) ` +
			`ON UPDATE CASCADE ON DELETE SET NULL`,
		)
		console.log(`[${dbName}] ${PERSONA_JURIDICA.name} FK recreated in ${Date.now() - tFk}ms`)
	})
	console.log(`[${dbName}] ${PERSONA_JURIDICA.name} finalize done in ${Date.now() - t}ms`)
}
