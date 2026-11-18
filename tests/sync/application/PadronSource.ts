import type { PreparedSnapshot, SnapshotSource } from "../../../src/sync/application/SnapshotSource"
import type { Entity } from "../../../src/sync/domain/Entity"
import type { SourceVersion } from "../../../src/sync/domain/SourceVersion"
import type { LineChunks } from "../../../src/sync/domain/diffSortedSnapshots"
import { lineChunks } from "../lineChunks"

export type Padron = Record<Entity, string[]>

// Doble de la descarga de SUNAT: entrega el padrón del "día" configurado, ya ordenado.
// Su huella sale del contenido: publicar otro padrón cambia el ETag.
export class PadronSource implements SnapshotSource {
	readonly #prepared = new Map<string, Padron>()
	downloads = 0
	// Simula que el HEAD a SUNAT falla.
	versionUnavailable = false

	constructor(public today: Padron) {}

	async currentVersion(): Promise<SourceVersion> {
		if (this.versionUnavailable) {
			return { kind: "unknown" }
		}
		return { kind: "known", tag: Bun.hash(JSON.stringify(this.today)).toString(16) }
	}

	async prepare(id: string): Promise<PreparedSnapshot> {
		this.downloads++
		const padron = { dni: [...this.today.dni].sort(), ruc: [...this.today.ruc].sort() }
		this.#prepared.set(id, padron)
		return {
			id,
			files: { dni: `${id}/dni.tsv`, ruc: `${id}/ruc.tsv` },
			rows: { dni: padron.dni.length, ruc: padron.ruc.length },
			parseRejects: { dni: 0, ruc: 0 },
		}
	}

	read(snapshot: PreparedSnapshot, entity: Entity): LineChunks {
		return lineChunks(this.rowsOf(snapshot)[entity])
	}

	rowsOf(snapshot: PreparedSnapshot): Padron {
		const padron = this.#prepared.get(snapshot.id)
		if (!padron) {
			throw new Error(`snapshot ${snapshot.id} no fue preparado`)
		}
		return padron
	}
}
