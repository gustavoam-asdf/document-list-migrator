import { Readable } from "node:stream"
import type { Sql } from "postgres"
import { pipeline } from "node:stream/promises"

import type { DeltaOp, DeltaStream, Entity } from "../domain/Entity"
import type { DeltaApplier } from "../application/DeltaApplier"
import { PERSONA_TABLES, type TableSpec } from "./personaTables"
import { extractPgErrorDetails } from "../../errors/pgErrorDetails"
import { isConnectionError } from "../../errors/isConnectionError"
import { rowKey } from "../domain/rowKey"

const MAX_ATTEMPTS = 5
const RETRY_BASE_DELAY_MS = 1000
const LOCK_TIMEOUT = "30s"

// 55P03 lock_not_available, 40001 serialization_failure, 40P01 deadlock_detected.
const TRANSIENT_CODES = new Set(["55P03", "40001", "40P01"])

type Statements = {
	createRowsTemp: string
	copyRows: string
	upsert: string
	createKeysTemp: string
	copyKeys: string
	delete: string
}

const quote = (identifier: string) => `"${identifier}"`

function buildStatements(entity: Entity, spec: TableSpec): Statements {
	const rowsTemp = `_sync_rows_${entity}`
	const keysTemp = `_sync_keys_${entity}`
	const columns = spec.columns.map(quote).join(", ")
	const nonKey = spec.columns.filter(c => c !== spec.key).map(quote)
	return {
		createRowsTemp:
			`CREATE TEMP TABLE IF NOT EXISTS ${rowsTemp} (${spec.columns.map(c => `${quote(c)} text`).join(", ")}) ` +
			`ON COMMIT DELETE ROWS`,
		copyRows: `COPY ${rowsTemp} (${columns}) FROM STDIN`,
		// El WHERE evita reescribir (y generar WAL de) filas que no cambiaron.
		upsert:
			`INSERT INTO ${quote(spec.table)} AS t (${columns}) SELECT ${columns} FROM ${rowsTemp} ` +
			`ON CONFLICT (${quote(spec.key)}) DO UPDATE SET ` +
			`${nonKey.map(c => `${c} = EXCLUDED.${c}`).join(", ")}, "updatedAt" = CURRENT_TIMESTAMP ` +
			`WHERE (${nonKey.map(c => `t.${c}`).join(", ")}) IS DISTINCT FROM (${nonKey.map(c => `EXCLUDED.${c}`).join(", ")})`,
		createKeysTemp: `CREATE TEMP TABLE IF NOT EXISTS ${keysTemp} (key text) ON COMMIT DELETE ROWS`,
		copyKeys: `COPY ${keysTemp} (key) FROM STDIN`,
		delete: `DELETE FROM ${quote(spec.table)} t USING ${keysTemp} k WHERE t.${quote(spec.key)} = k.key`,
	}
}

const STATEMENTS: Record<Entity, Statements> = {
	dni: buildStatements("dni", PERSONA_TABLES.dni),
	ruc: buildStatements("ruc", PERSONA_TABLES.ruc),
}

// Cada lote es una transacción corta (solo locks de fila, sin DDL) que además suma
// su tamaño al progreso: un corte en cualquier punto deja datos y progreso coherentes.
export class PostgresDeltaApplier implements DeltaApplier {
	#rejected = 0

	constructor(
		private readonly sql: Sql,
		// Respiro entre commits para acotar el ritmo de escritura (WAL, I/O) y dejar
		// margen a las lecturas del api aunque el delta del día sea grande.
		private readonly pauseMs: number,
	) {}

	async apply(stream: DeltaStream, items: string[]): Promise<void> {
		const [entity, op] = parseStream(stream)
		await this.#applyIsolatingBadRows(stream, entity, op, items)
	}

	// Un error de datos en un upsert parte el lote por la mitad hasta aislar la fila
	// culpable, que va a migrator.sync_rejects y se reintenta en el próximo objetivo.
	async #applyIsolatingBadRows(stream: DeltaStream, entity: Entity, op: DeltaOp, items: string[]): Promise<void> {
		try {
			await this.#withRetry(() => this.#commitBatch(stream, entity, op, items))
			if (this.pauseMs > 0) {
				await Bun.sleep(this.pauseMs)
			}
		} catch (error) {
			if (op !== "upsert" || !isDataError(error)) {
				throw error
			}
			const [only] = items
			if (items.length === 1 && only !== undefined) {
				await this.#withRetry(() => this.#commitReject(stream, entity, only, error))
				return
			}
			const middle = Math.ceil(items.length / 2)
			await this.#applyIsolatingBadRows(stream, entity, op, items.slice(0, middle))
			await this.#applyIsolatingBadRows(stream, entity, op, items.slice(middle))
		}
	}

	// Filas apartadas a migrator.sync_rejects en esta corrida.
	get rejectedCount(): number {
		return this.#rejected
	}

	async #commitBatch(stream: DeltaStream, entity: Entity, op: DeltaOp, items: string[]): Promise<void> {
		const statements = STATEMENTS[entity]
		await this.sql.begin(async tx => {
			// Seguro: si se pierde el commit, se pierde junto con su progreso y se reaplica.
			await tx.unsafe("SET LOCAL synchronous_commit = off")
			await tx.unsafe("SET LOCAL client_min_messages = warning")
			await tx.unsafe(`SET LOCAL lock_timeout = '${LOCK_TIMEOUT}'`)
			if (op === "upsert") {
				await tx.unsafe(statements.createRowsTemp)
				await copyLines(await tx.unsafe(statements.copyRows).writable(), items)
				await tx.unsafe(statements.upsert)
			} else {
				await tx.unsafe(statements.createKeysTemp)
				await copyLines(await tx.unsafe(statements.copyKeys).writable(), items)
				await tx.unsafe(statements.delete)
			}
			await tx`UPDATE migrator.sync_progress SET done = done + ${items.length} WHERE stream = ${stream}`
		})
	}

	async #commitReject(stream: DeltaStream, entity: Entity, line: string, cause: unknown): Promise<void> {
		const details = extractPgErrorDetails(cause)
		const error = [details.code, details.message, details.detail].filter(Boolean).join(" | ")
		const key = rowKey(line)
		console.error(`[sync] ${stream} fila rechazada key=${key} | ${error}`)
		await this.sql.begin(async tx => {
			await tx`
				INSERT INTO migrator.sync_rejects (entity, key, line, error)
				VALUES (${entity}, ${key}, ${line}, ${error})
				ON CONFLICT (entity, key) DO UPDATE
				SET line = EXCLUDED.line, error = EXCLUDED.error, last_failed_at = now()
			`
			await tx`UPDATE migrator.sync_progress SET done = done + 1 WHERE stream = ${stream}`
		})
		this.#rejected++
	}

	async #withRetry(operation: () => Promise<void>): Promise<void> {
		for (let attempt = 1; ; attempt++) {
			try {
				await operation()
				return
			} catch (error) {
				if (attempt >= MAX_ATTEMPTS || !isTransient(error)) {
					throw error
				}
				const delay = RETRY_BASE_DELAY_MS * 2 ** (attempt - 1)
				console.warn(`[sync] error transitorio (intento ${attempt}/${MAX_ATTEMPTS}), reintento en ${delay}ms: ${extractPgErrorDetails(error).message}`)
				await Bun.sleep(delay)
			}
		}
	}
}

async function copyLines(writable: NodeJS.WritableStream, items: string[]): Promise<void> {
	await pipeline(Readable.from([`${items.join("\n")}\n`]), writable)
}

function parseStream(stream: DeltaStream): [Entity, DeltaOp] {
	switch (stream) {
		case "dni:upsert": return ["dni", "upsert"]
		case "dni:delete": return ["dni", "delete"]
		case "ruc:upsert": return ["ruc", "upsert"]
		case "ruc:delete": return ["ruc", "delete"]
	}
}

function pgCode(error: unknown): string | undefined {
	return extractPgErrorDetails(error).code
}

function isTransient(error: unknown): boolean {
	const code = pgCode(error)
	return isConnectionError(error) || (code !== undefined && TRANSIENT_CODES.has(code))
}

// Clase 22 (data_exception, p. ej. valor demasiado largo) y 23 (integridad).
function isDataError(error: unknown): boolean {
	const code = pgCode(error)
	return code !== undefined && (code.startsWith("22") || code.startsWith("23"))
}
