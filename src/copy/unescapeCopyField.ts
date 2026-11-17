const ESCAPES: Record<string, string> = {
	"\\": "\\",
	t: "\t",
	n: "\n",
	r: "\r",
	b: "\b",
	f: "\f",
	v: "\v",
}

// Inversa de escapeCopyField: valor real de un campo leído de `COPY ... TO STDOUT`.
export function unescapeCopyField(value: string): string {
	return value.replace(/\\(.)/g, (_, char: string) => ESCAPES[char] ?? char)
}
