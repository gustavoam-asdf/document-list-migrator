import { $ } from "bun"

type Transfer =
	| { kind: "download"; url: string; file: string }
	| { kind: "upload"; url: string; file: string }

// Transferencias grandes por curl y no por la red de Bun: en Linux, fetch y el cliente S3
// de Bun retienen en memoria nativa todo lo transferido (1,7 GB de ruc.tsv → OOM en
// Fargate). curl va de socket a disco con memoria constante (~8 MB medidos).
// La URL (puede llevar firma y token de sesión) nunca se loguea.
export async function curlTransfer(transfer: Transfer, label: string): Promise<void> {
	const direction = transfer.kind === "download" ? ["-o", transfer.file] : ["-T", transfer.file]
	const result = await $`curl --silent --show-error --fail --location --retry 5 --retry-all-errors --connect-timeout 30 ${direction} ${transfer.url}`
		.nothrow()
		.quiet()
	if (result.exitCode !== 0) {
		const stderr = result.stderr.toString().replaceAll(transfer.url, "<url>").trim()
		throw new Error(`curl ${transfer.kind} de ${label} falló (exit ${result.exitCode}): ${stderr}`)
	}
}
