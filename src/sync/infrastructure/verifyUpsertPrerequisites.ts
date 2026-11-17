import type { Sql } from "postgres"

import { ENTITIES } from "../domain/Entity"
import { PERSONA_TABLES } from "./personaTables"
import { logEvent } from "../../shared/logEvent"

// El upsert necesita la PK (ON CONFLICT). La versión anterior del migrador la dropeaba
// durante la carga masiva: si una corrida vieja murió a mitad, la tabla puede no tenerla.
// No se recrea sola porque ADD PRIMARY KEY bloquea la tabla mientras indexa.
export async function verifyUpsertPrerequisites(sql: Sql): Promise<void> {
	for (const entity of ENTITIES) {
		const { table } = PERSONA_TABLES[entity]
		const rows = await sql<{ has_pk: unknown; persistence: unknown }[]>`
			SELECT
				EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = c.oid AND contype = 'p') AS has_pk,
				c.relpersistence::text AS persistence
			FROM pg_class c
			WHERE c.oid = to_regclass(${`public."${table}"`})
		`
		const row = rows[0]
		if (!row) {
			throw new Error(`falta la tabla ${table}`)
		}
		if (row.has_pk !== true) {
			throw new Error(`${table} no tiene PRIMARY KEY: recrearla (y deduplicar) antes de sincronizar`)
		}
		if (row.persistence === "u") {
			logEvent({
				event: "sync_warning",
				level: "warn",
				component: "unlogged-table",
				message: `${table} es UNLOGGED (resto de la carga masiva vieja): un crash de la BD la vacía. Corregir con ALTER TABLE ... SET LOGGED en una ventana de mantenimiento`,
			})
		}
	}
}
