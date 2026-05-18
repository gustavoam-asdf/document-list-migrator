// Extrae los campos del error de Postgres que vienen del driver `postgres`,
// para que el archivo de rejects tenga toda la info útil del fallo
// (no sólo el message genérico).
export type PgErrorDetails = {
	message: string
	code?: string
	severity?: string
	detail?: string
	hint?: string
	where?: string
	column?: string
	table?: string
	schema?: string
	position?: string
	internalPosition?: string
	routine?: string
}

export function extractPgErrorDetails(error: unknown): PgErrorDetails {
	if (!error || typeof error !== "object") {
		return { message: String(error) }
	}
	const e = error as Record<string, unknown>
	const str = (k: string) => typeof e[k] === "string" ? (e[k] as string) : undefined

	return {
		message: str("message") ?? String(error),
		code: str("code"),
		severity: str("severity_local") ?? str("severity"),
		detail: str("detail"),
		hint: str("hint"),
		where: str("where"),
		column: str("column_name"),
		table: str("table_name"),
		schema: str("schema_name"),
		position: str("position"),
		internalPosition: str("internal_position"),
		routine: str("routine"),
	}
}
