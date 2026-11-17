import type { PreparedSnapshot, SnapshotSource } from "../../../src/sync/application/SnapshotSource"
import type { Entity } from "../../../src/sync/domain/Entity"
import type { LineChunks } from "../../../src/sync/domain/diffSortedSnapshots"
import { lineChunks } from "../lineChunks"

export type Padron = Record<Entity, string[]>

// Doble de la descarga de SUNAT: entrega el padrón del "día" configurado, ya ordenado.
export class PadronSource implements SnapshotSource {
	readonly #prepared = new Map<string, Padron>()

	constructor(public today: Padron) {}

	async prepare(id: string): Promise<PreparedSnapshot> {
		const padron = { dni: [...this.today.dni].sort(), ruc: [...this.today.ruc].sort() }
		this.#prepared.set(id, padron)
		return {
			id,
			files: { dni: `${id}/dni.tsv`, ruc: `${id}/ruc.tsv` },
			rows: { dni: padron.dni.length, ruc: padron.ruc.length },
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
