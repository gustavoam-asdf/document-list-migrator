// Decodifica un stream de bytes y lo entrega como arrays de líneas (un array por
// chunk leído), sin el "\n" final. Agrupar evita un await por línea en el consumidor.
export async function* readLineChunks(
	stream: AsyncIterable<Uint8Array>,
	encoding: "utf-8" | "latin1" = "utf-8",
): AsyncGenerator<string[]> {
	const decoder = new TextDecoder(encoding)
	let pending = ""
	for await (const bytes of stream) {
		const text = pending + decoder.decode(bytes, { stream: true })
		const lines = text.split("\n")
		pending = lines.pop() ?? ""
		if (lines.length > 0) {
			yield lines
		}
	}
	const tail = pending + decoder.decode()
	if (tail.length > 0) {
		yield [tail]
	}
}
