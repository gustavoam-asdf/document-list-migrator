// Escapes a field for Postgres COPY ... FROM STDIN text format.
// Without this, a literal tab/newline/backslash in razonSocial or
// nombreCompleto desaligns the row and pierde el batch entero.
export function escapeCopyField(value: string): string {
	return value
		.replaceAll("\\", "\\\\")
		.replaceAll("\t", "\\t")
		.replaceAll("\n", "\\n")
		.replaceAll("\r", "\\r")
}
