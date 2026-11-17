import type { Entity } from "../domain/Entity"

export type TableSpec = {
	table: string
	key: string
	// Mismo orden que las columnas de la fila COPY-text del snapshot.
	columns: readonly string[]
}

export const PERSONA_TABLES: Record<Entity, TableSpec> = {
	dni: {
		table: "PersonaNatural",
		key: "dni",
		columns: ["dni", "nombreCompleto"],
	},
	ruc: {
		table: "PersonaJuridica",
		key: "ruc",
		columns: [
			"ruc",
			"razonSocial",
			"estado",
			"condicionDomicilio",
			"tipoVia",
			"nombreVia",
			"codigoZona",
			"tipoZona",
			"numero",
			"interior",
			"lote",
			"departamento",
			"manzana",
			"kilometro",
			"codigoUbigeo",
		],
	},
}
