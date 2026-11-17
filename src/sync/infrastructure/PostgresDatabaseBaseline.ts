import fs from "node:fs/promises"
import type { Sql } from "postgres"

import type { DatabaseBaseline } from "../application/DatabaseBaseline"
import type { Entity } from "../domain/Entity"
import type { LineChunks } from "../domain/diffSortedSnapshots"
import { KeyOrderTrackingWriter } from "./KeyOrderTrackingWriter"
import { PERSONA_TABLES } from "./personaTables"
import { escapeCopyField } from "../../copy/escapeCopyField"
import { fileLineChunks } from "../../stream/fileLineChunks"

type Paging = {
	pageRows: number
	pauseMs: number
}

// Último recurso: el baseline es lo que la BD tiene hoy. Solo lee, por páginas de
// clave (keyset sobre la PK) con pausa entre páginas: cada página es una consulta
// corta por índice, no un seq scan de minutos que compita con las lecturas del api
// ni un snapshot largo que frene al autovacuum.
//
// SELECT y no `COPY ... TO STDOUT`: con dos volcados concurrentes, el readable de
// COPY TO de postgres.js a veces no cierra aunque el servidor ya terminó.
export class PostgresDatabaseBaseline implements DatabaseBaseline {
	constructor(
		private readonly sql: Sql,
		private readonly workDir: string,
		private readonly paging: Paging,
	) {}

	async dump(entity: Entity): Promise<LineChunks> {
		const spec = PERSONA_TABLES[entity]
		await fs.mkdir(this.workDir, { recursive: true })
		const sorted = `${this.workDir}/${entity}.tsv`
		const writer = new KeyOrderTrackingWriter(`${this.workDir}/${entity}.dump.tsv`)
		const columns = spec.columns.map(c => `"${c}"`).join(", ")
		const key = `"${spec.key}"`
		const firstPage = `SELECT ${columns} FROM "${spec.table}" ORDER BY ${key} LIMIT ${this.paging.pageRows}`
		const nextPage = `SELECT ${columns} FROM "${spec.table}" WHERE ${key} > $1 ORDER BY ${key} LIMIT ${this.paging.pageRows}`

		const start = Date.now()
		let lastKey: string | undefined
		let pages = 0
		for (;;) {
			const rows = await this.sql.unsafe<Record<string, unknown>[]>(
				lastKey === undefined ? firstPage : nextPage,
				lastKey === undefined ? [] : [lastKey],
			)
			pages++
			for (const row of rows) {
				writer.write(toCopyLine(row, spec.columns))
			}

			const last = rows.at(-1)
			if (last === undefined || rows.length < this.paging.pageRows) {
				break
			}
			lastKey = textField(last[spec.key], spec.key)
			if (this.paging.pauseMs > 0) {
				await Bun.sleep(this.paging.pauseMs)
			}
		}

		const { sortedExternally } = await writer.finish(sorted, `${this.workDir}/tmp`)
		console.log(
			`[sync] volcado de ${spec.table}: ${writer.rows} filas en ${pages} páginas, ${Date.now() - start}ms` +
			(sortedExternally ? " (con sort externo)" : ""),
		)
		return fileLineChunks(sorted)
	}
}

// Misma forma que una fila del snapshot (y que `COPY ... TO STDOUT`): `\N` = NULL.
function toCopyLine(row: Record<string, unknown>, columns: readonly string[]): string {
	return columns
		.map(column => {
			const value = row[column]
			return value === null ? "\\N" : escapeCopyField(textField(value, column))
		})
		.join("\t")
}

function textField(value: unknown, column: string): string {
	if (typeof value !== "string") {
		throw new Error(`volcado: la columna ${column} no es texto (${typeof value})`)
	}
	return value
}
