// Las filas de snapshot están en formato COPY-text: la primera columna es la PK.
export function rowKey(line: string): string {
	const tab = line.indexOf("\t")
	return tab === -1 ? line : line.slice(0, tab)
}
