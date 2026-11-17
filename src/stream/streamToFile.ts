// `Bun.write(path, response)` con bodies de cientos de MB se cuelga sin escribir
// nada (visto con el zip del padrón): se escribe chunk a chunk de forma explícita.
export async function streamToFile(stream: AsyncIterable<Uint8Array>, path: string): Promise<number> {
	const writer = Bun.file(path).writer({ highWaterMark: 8 * 1024 * 1024 })
	let bytes = 0
	try {
		for await (const chunk of stream) {
			writer.write(chunk)
			bytes += chunk.byteLength
		}
	} finally {
		await writer.end()
	}
	return bytes
}
