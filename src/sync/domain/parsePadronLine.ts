import { escapeCopyField } from "../../copy/escapeCopyField"

export type ParseResult =
	| { kind: "row"; row: string }
	| { kind: "reject"; reason: string }
	| { kind: "skip" }

const ANULADO_MARKER = "|ANULACION - ERROR SU|"

// Fila de PersonaNatural en formato COPY-text: dni, nombreCompleto.
export function parseDniLine(line: string): ParseResult {
	// SUNAT codifica a las personas naturales como RUCs que empiezan con "10".
	if (!line.startsWith("10")) {
		return { kind: "skip" }
	}
	if (line.includes(ANULADO_MARKER)) {
		return { kind: "skip" }
	}

	const firstPipe = line.indexOf("|")
	if (firstPipe === -1) {
		return { kind: "reject", reason: "no pipe separator" }
	}
	const secondPipe = line.indexOf("|", firstPipe + 1)
	if (secondPipe === -1) {
		return { kind: "reject", reason: "only one pipe separator" }
	}

	const ruc = line.slice(0, firstPipe).trim()
	const nombre = line.slice(firstPipe + 1, secondPipe).trim().replace(/\s+/g, " ")

	if (!ruc) {
		return { kind: "reject", reason: "empty ruc" }
	}
	if (!nombre || nombre === "-") {
		return { kind: "reject", reason: "empty nombre" }
	}
	if (ruc.length < 4) {
		return { kind: "reject", reason: "ruc too short to extract dni" }
	}

	const dni = ruc.slice(2, -1)
	return { kind: "row", row: `${escapeCopyField(dni)}\t${escapeCopyField(nombre)}` }
}

// Fila de PersonaJuridica en formato COPY-text. Orden de columnas: ruc, razonSocial,
// estado, condicionDomicilio, tipoVia, nombreVia, codigoZona, tipoZona, numero,
// interior, lote, departamento, manzana, kilometro, codigoUbigeo.
export function parseRucLine(line: string): ParseResult {
	if (line.includes(ANULADO_MARKER)) {
		return { kind: "skip" }
	}

	// El escape de backslash a `\\` y su reversión `\\|` → `-` preservan los `-`
	// literales dentro de un campo mientras `|-+` (campo con solo guiones) queda vacío.
	const cleaned = line
		.trim()
		.replace(/\s+/g, " ")
		.replace(/\\/g, "\\\\")
		.replace(/\|-+/g, "|")
		.replace(/\|+$/, "")
		.replace(/\\\\\|/g, "-")

	const fields = cleaned.split("|").map(v => {
		const t = v.trim()
		return t === "" ? undefined : t
	})

	const [
		ruc,
		razonSocial,
		estado,
		condicionDomicilio,
		ubigeo,
		tipoVia,
		nombreVia,
		codigoZona,
		tipoZona,
		numero,
		interior,
		lote,
		departamento,
		manzana,
		kilometro,
	] = fields

	if (!ruc) {
		return { kind: "reject", reason: "empty ruc" }
	}
	if (!razonSocial) {
		return { kind: "reject", reason: "empty razonSocial" }
	}
	if (!estado) {
		return { kind: "reject", reason: "empty estado" }
	}

	const isPersonaJuridica = !ruc.startsWith("10")

	// `\N` es el NULL de COPY-text. Para RUC "10*" (persona natural) la dirección va NULL.
	const addr = (v: string | undefined) => (isPersonaJuridica && v ? escapeCopyField(v) : "\\N")

	const row = [
		escapeCopyField(ruc),
		escapeCopyField(razonSocial),
		escapeCopyField(estado),
		escapeCopyField(condicionDomicilio ?? "NO HABIDO"),
		addr(tipoVia),
		addr(nombreVia),
		addr(codigoZona),
		addr(tipoZona),
		addr(numero),
		addr(interior),
		addr(lote),
		addr(departamento),
		addr(manzana),
		addr(kilometro),
		addr(ubigeo),
	].join("\t")

	return { kind: "row", row }
}
