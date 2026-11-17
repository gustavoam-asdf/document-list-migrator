import { $ } from "bun"
import fs from "node:fs/promises"
import type { Sql } from "postgres"

import type { DatabaseBaseline } from "../application/DatabaseBaseline"
import type { Entity } from "../domain/Entity"
import type { LineChunks } from "../domain/diffSortedSnapshots"
import { PERSONA_TABLES } from "./personaTables"
import type { RateLimiter } from "../../shared/RateLimiter"
import { fileLineChunks } from "../../stream/fileLineChunks"
import { sortTsvByKey } from "./sortTsvByKey"

type Paging = {
	// Bloques de 8 KB por consulta (2048 ≈ 16 MB de tabla).
	pageBlocks: number
	// Compartido por los volcados concurrentes: el tope de filas/s vale para el total.
	limiter: RateLimiter
}

// Variables PG* para psql (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, PGSSLMODE).
export type PsqlEnv = Record<string, string>

// Último recurso: el baseline es lo que la BD tiene hoy. Solo lee.
// - Por rangos de bloques físicos (`ctid`, TID Range Scan, Postgres 14+): lecturas
//   contiguas en orden de disco. Paginar por la PK saltaba por todo el heap (lecturas
//   aleatorias) y llevó la RDS al techo de IOPS.
// - Con `psql \copy` como subproceso, de la BD al archivo: 34M filas a través de
//   postgres.js/Bun dejaron la memoria del proceso creciendo ~200 MB/s en Fargate (OOM).
//   psql usa memoria constante y el formato es el COPY TO del servidor, idéntico al
//   del snapshot.
// La salida queda en orden físico: la ordena el sort del contenedor.
export class PostgresDatabaseBaseline implements DatabaseBaseline {
	constructor(
		private readonly sql: Sql,
		private readonly psqlEnv: PsqlEnv,
		private readonly workDir: string,
		private readonly paging: Paging,
	) {}

	async dump(entity: Entity): Promise<LineChunks> {
		const spec = PERSONA_TABLES[entity]
		await fs.mkdir(this.workDir, { recursive: true })
		const raw = `${this.workDir}/${entity}.dump.tsv`
		const sorted = `${this.workDir}/${entity}.tsv`
		await fs.rm(raw, { force: true })
		const columns = spec.columns.map(c => `"${c}"`).join(", ")

		const start = Date.now()
		// Leído al empezar: el migrador tiene el lock de sync y el api no escribe en estas
		// tablas, así que no aparecen filas nuevas en bloques posteriores durante el volcado.
		const totalBlocks = await this.#relationBlocks(spec.table)
		let rows = 0
		let queries = 0
		for (let first = 0; first < totalBlocks; first += this.paging.pageBlocks) {
			const range = `ctid >= '(${first},0)'::tid AND ctid < '(${first + this.paging.pageBlocks},0)'::tid`
			const copied = await this.#copyAppend(`SELECT ${columns} FROM "${spec.table}" WHERE ${range}`, raw)
			rows += copied
			queries++
			await this.paging.limiter.consume(copied)
		}

		await sortTsvByKey(raw, sorted, `${this.workDir}/tmp-${entity}`)
		console.log(
			`[sync] volcado de ${spec.table}: ${rows} filas, ${totalBlocks} bloques en ${queries} consultas, ${Date.now() - start}ms`,
		)
		return fileLineChunks(sorted)
	}

	// Agrega al archivo el resultado de la consulta; devuelve las filas copiadas.
	async #copyAppend(select: string, file: string): Promise<number> {
		const command = `\\copy (${select}) TO PROGRAM 'cat >> ${file}'`
		const result = await $`psql --no-psqlrc --set ON_ERROR_STOP=1 --command ${command}`
			.env({ ...process.env, ...this.psqlEnv })
			.nothrow()
			.quiet()
		if (result.exitCode !== 0) {
			throw new Error(`psql \\copy falló (exit ${result.exitCode}): ${result.stderr.toString().trim()}`)
		}
		const match = /COPY (\d+)/.exec(result.stdout.toString())
		if (!match?.[1]) {
			throw new Error(`psql \\copy no informó filas copiadas: ${result.stdout.toString().trim()}`)
		}
		return Number(match[1])
	}

	async #relationBlocks(table: string): Promise<number> {
		const rows = await this.sql<{ blocks: unknown }[]>`
			SELECT (pg_relation_size(${`public."${table}"`}::regclass) / current_setting('block_size')::bigint)::int AS blocks
		`
		const blocks = rows[0]?.blocks
		if (typeof blocks !== "number") {
			throw new Error(`volcado: no se pudo leer el tamaño de ${table}`)
		}
		return blocks
	}
}
