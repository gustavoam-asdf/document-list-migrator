import type { Sql } from "postgres"

// INEI SISCONCODE ubigeo catalog. Public (no login) and — unlike SEACE — reachable
// from AWS (verified from a Fargate task). Served over TLS with a non-verifiable
// cert on :8443 (skip verification) and the HTML is ISO-8859-1 (latin1). Codes match
// SUNAT's (both derive from the INEI numbering) EXCEPT 9 districts INEI renumbered
// when it split off new provinces; the SUNAT RUC padron still uses the old codes, so
// those are re-added from SUNAT_DIVERGENT below. The FK on PersonaJuridica.codigoUbigeo
// is intentionally dropped, so a code not present here just isn't shown (no failure).
const BASE = "https://webapp.inei.gob.pe:8443/sisconcode"

export type UbigeoRow = { codigo: string; region: string; provincia: string; distrito: string }

// 9 districts SUNAT keeps under old codes (INEI moved them to newly-created provinces).
// The padron uses SUNAT codes, so seed these explicitly with SUNAT's names. Stable:
// these are historical renumberings, not something that changes over time.
const SUNAT_DIVERGENT: UbigeoRow[] = [
	{ codigo: "090708", region: "HUANCAVELICA", provincia: "TAYACAJA", distrito: "HUANDO" },
	{ codigo: "160109", region: "LORETO", provincia: "MAYNAS", distrito: "PUTUMAYO" },
	{ codigo: "160111", region: "LORETO", provincia: "MAYNAS", distrito: "YAQUERANA" },
	{ codigo: "160114", region: "LORETO", provincia: "MAYNAS", distrito: "TENIENTE MANUEL CLAVERO" },
	{ codigo: "160203", region: "LORETO", provincia: "ALTO AMAZONAS", distrito: "BARRANCA" },
	{ codigo: "160204", region: "LORETO", provincia: "ALTO AMAZONAS", distrito: "CAHUAPANAS" },
	{ codigo: "160207", region: "LORETO", provincia: "ALTO AMAZONAS", distrito: "MANSERICHE" },
	{ codigo: "160208", region: "LORETO", provincia: "ALTO AMAZONAS", distrito: "MORONA" },
	{ codigo: "160209", region: "LORETO", provincia: "ALTO AMAZONAS", distrito: "PASTAZA" },
]

async function fetchLatin1(url: string, init?: RequestInit): Promise<string> {
	// Bun's fetch accepts a per-request `tls` option to skip cert verification.
	const res = await fetch(url, { ...init, tls: { rejectUnauthorized: false } } as RequestInit)
	if (!res.ok) {
		throw new Error(`HTTP ${res.status} for ${url}`)
	}
	return new TextDecoder("latin1").decode(await res.arrayBuffer())
}

// Detect the most recent UBIGEO version (e.g. "8-1" for 2025) instead of hardcoding.
async function detectVersion(): Promise<string> {
	const html = await fetchLatin1(`${BASE}/proyecto/loadModuloBusqueda.htm`, {
		method: "POST",
		headers: { "content-type": "application/x-www-form-urlencoded" },
		body: "proyectoId=3&versionId=1&tipoCategoriaId=1",
	})
	const versions: { year: number; id: string }[] = []
	const re = /<option value="(\d+-\d+)"[^>]*>(\d{4})/g
	let m: RegExpExecArray | null
	while ((m = re.exec(html)) !== null) {
		versions.push({ id: m[1], year: parseInt(m[2], 10) })
	}
	const latest = versions.sort((a, b) => a.year - b.year).at(-1)
	if (!latest) {
		throw new Error("no ubigeo versions found in SISCONCODE response")
	}
	return latest.id
}

// The whole tree comes as lines "<indent>CODE Name", CODE of 2 (dept), 4 (province)
// or 6 (district) digits (repeated across the tree — dedupe by first occurrence).
// Names are upper-cased to match the existing all-caps convention in the table.
export function parseTree(html: string): UbigeoRow[] {
	const deptos = new Map<string, string>()
	const provincias = new Map<string, string>()
	const distritos = new Map<string, string>()

	for (const raw of html.split("\n")) {
		const m = raw.replace(/^\s+/, "").match(/^(\d{2}|\d{4}|\d{6})\s+([^<]+)/)
		if (!m) {
			continue
		}
		const code = m[1]
		const name = m[2].trim().toUpperCase()
		if (!name) {
			continue
		}
		const map = code.length === 2 ? deptos : code.length === 4 ? provincias : distritos
		if (!map.has(code)) {
			map.set(code, name)
		}
	}

	const rows: UbigeoRow[] = []
	for (const [codigo, distrito] of distritos) {
		const region = deptos.get(codigo.slice(0, 2))
		const provincia = provincias.get(codigo.slice(0, 4))
		if (region && provincia) {
			rows.push({ codigo, region, provincia, distrito })
		}
	}

	// Overlay the SUNAT-divergent codes (they never collide with INEI, which codes
	// those districts differently).
	const byCode = new Map(rows.map(r => [r.codigo, r]))
	for (const r of SUNAT_DIVERGENT) {
		byCode.set(r.codigo, r)
	}
	return [...byCode.values()]
}

export async function fetchUbigeos(): Promise<UbigeoRow[]> {
	const version = await detectVersion()
	console.log(`[ubigeo] INEI SISCONCODE version ${version}`)
	const html = await fetchLatin1(
		`${BASE}/ubigeo/listaBusquedaUbigeoPorUbicacionGeografica.htm` +
			`?versionCategoriaPK=${version}&nivel=1&strVersion=X&strDpto=TODOS&strProv=TODOS&strDist=TODOS&flagDpto=&flagProv=&flagDist=`,
	)
	return parseTree(html)
}

// Scrape once per run.
let memo: Promise<UbigeoRow[]> | null = null
export function getUbigeos(): Promise<UbigeoRow[]> {
	if (!memo) {
		memo = fetchUbigeos()
	}
	return memo
}

export async function countUbigeos(sql: Sql): Promise<number> {
	const rows = await sql<{ count: string }[]>`SELECT count(*)::text AS count FROM "Ubigeo"`
	return parseInt(rows[0]?.count ?? "0", 10)
}

export async function refreshUbigeoTable(sql: Sql, dbName: string, rows: UbigeoRow[]): Promise<void> {
	const start = Date.now()
	const batchSize = 500
	for (let i = 0; i < rows.length; i += batchSize) {
		const batch = rows.slice(i, i + batchSize)
		await sql`
			INSERT INTO "Ubigeo" ${sql(batch, "codigo", "region", "provincia", "distrito")}
			ON CONFLICT (codigo) DO UPDATE SET
				region = excluded.region,
				provincia = excluded.provincia,
				distrito = excluded.distrito,
				"updatedAt" = now()
		`
	}
	console.log(`[${dbName}] Ubigeo refreshed: ${rows.length} rows upserted in ${Date.now() - start}ms`)
}
