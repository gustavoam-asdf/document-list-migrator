// Escapa un campo para COPY text format. Además de backslash/tab/newline/CR escapa
// \b \f \v porque así los emite `COPY ... TO STDOUT`: el diff compara filas del
// snapshot contra volcados de la BD byte a byte.
export function escapeCopyField(value: string): string {
	return value
		.replaceAll("\\", "\\\\")
		.replaceAll("\t", "\\t")
		.replaceAll("\n", "\\n")
		.replaceAll("\r", "\\r")
		.replaceAll("\b", "\\b")
		.replaceAll("\f", "\\f")
		.replaceAll("\v", "\\v")
}
