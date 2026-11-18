import { $ } from "bun"

// Cabeceras de la respuesta final (tras redirecciones) a un HEAD, con nombres en minúscula.
// Por curl, como curlTransfer: la red de Bun no se usa para hablar con SUNAT.
export async function curlHead(url: string, label: string): Promise<Map<string, string>> {
	const result = await $`curl --silent --show-error --fail --location --head --retry 3 --retry-all-errors --connect-timeout 30 --max-time 120 ${url}`
		.nothrow()
		.quiet()
	if (result.exitCode !== 0) {
		const stderr = result.stderr.toString().replaceAll(url, "<url>").trim()
		throw new Error(`curl HEAD de ${label} falló (exit ${result.exitCode}): ${stderr}`)
	}
	const blocks = result.stdout.toString().split(/\r?\n\r?\n/).filter(block => block.trim() !== "")
	const headers = new Map<string, string>()
	for (const line of (blocks.at(-1) ?? "").split(/\r?\n/)) {
		const separator = line.indexOf(":")
		if (separator > 0) {
			headers.set(line.slice(0, separator).trim().toLowerCase(), line.slice(separator + 1).trim())
		}
	}
	return headers
}
