import { filesDir } from "./constants"

import type { Sql } from "postgres"

// SEACE's ubigeo picker is server-rendered: each <select> arrives already filled
// according to the URL params (v_dep_cod, v_pro_cod). So a plain fetch + HTML
// parse gets the whole ubigeo tree — no headless browser, no puppeteer.
// https://prod3.seace.gob.pe/portalseace-uiwd-pub/ControllerServletOpen?pageid=0&portletid=BuscadorUbigeo&scriptdo=doView&v_dep_cod=01&v_pro_cod=01
const source =
	"https://prod3.seace.gob.pe/portalseace-uiwd-pub/ControllerServletOpen?pageid=0&portletid=BuscadorUbigeo&scriptdo=doView"

const cacheFile = `${filesDir}/ubigeo.json`

export type UbigeoRow = {
	codigo: string
	region: string
	provincia: string
	distrito: string
}

// A department+province pair: enough to fetch that province's districts and to
// label the resulting rows. Regions and provinces almost never change, so this
// "skeleton" is read from the DB on routine refreshes instead of re-scraped.
type ProvinceRef = {
	dep: string
	region: string
	pro: string
	provincia: string
}

// Exterior (v_dep_cod=98) does not populate provinces/districts like the rest, so
// it is fixed to a single synthetic ubigeo (codigo 980100), as the old scrape did.
const EXTERIOR: UbigeoRow = { codigo: "980100", region: "EXTERIOR", provincia: "EXTERIOR", distrito: "MULTIDISTRITAL" }

const regionUrl = (dep: string) => `${source}&v_dep_cod=${dep}`
const provinceUrl = (dep: string, pro: string) => `${source}&v_dep_cod=${dep}&v_pro_cod=${pro}`

// Fetch with a few retries: the SEACE server is occasionally flaky.
async function fetchHtml(url: string, retries = 3): Promise<string> {
	let lastErr: unknown
	for (let attempt = 1; attempt <= retries; attempt++) {
		try {
			const res = await fetch(url)
			if (!res.ok) {
				throw new Error(`HTTP ${res.status}`)
			}
			return await res.text()
		} catch (err) {
			lastErr = err
			if (attempt < retries) {
				await new Promise(r => setTimeout(r, 500 * attempt))
			}
		}
	}
	throw new Error(`fetch failed after ${retries} attempts (${url}): ${(lastErr as Error)?.message ?? lastErr}`)
}

// Parse the <option> list of a named <select>. The SEACE markup is quirky
// (uppercase <OPTION>, the value attribute and its closing '>' on separate lines),
// so the match is case-insensitive and spans newlines. Each option is labelled
// "CC-NAME"; the leading "CC-" is stripped so `value` is just the name. The "00"
// option is the "MULTI*" placeholder, not a real ubigeo, so it is dropped.
function parseOptions(html: string, selectName: string): { code: string; value: string }[] {
	const select = html.match(new RegExp(`<select[^>]*name=["']${selectName}["'][^>]*>([\\s\\S]*?)</select>`, "i"))
	if (!select) {
		throw new Error(`select "${selectName}" not found`)
	}

	const optionRe = /<option\s+value=["']([^"']*)["'][^>]*>\s*([^<]*?)\s*<\/option>/gi
	const options: { code: string; value: string }[] = []
	let match: RegExpExecArray | null
	while ((match = optionRe.exec(select[1])) !== null) {
		const code = match[1].trim()
		if (code === "00") {
			continue
		}
		const value = match[2].replace(new RegExp(`^${code}\\s*-\\s*`), "").trim()
		options.push({ code, value })
	}
	return options
}

// Run an async fn over items with a bounded number in flight: fast, but polite to
// the SEACE server. Results keep the input order.
async function mapPool<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
	const results = new Array<R>(items.length)
	let next = 0
	async function worker(): Promise<void> {
		while (next < items.length) {
			const index = next++
			results[index] = await fn(items[index]!)
		}
	}
	await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker))
	return results
}

// Scrape the department+province skeleton from SEACE (regions -> provinces). Only
// used on a full scrape (bootstrap / forced); routine refreshes read it from the DB.
async function scrapeStructure(): Promise<ProvinceRef[]> {
	const regions = parseOptions(await fetchHtml(source), "v_dep_cod").filter(region => region.code !== "98")

	const regionProvinces = await mapPool(regions, 8, async region => ({
		region,
		provinces: parseOptions(await fetchHtml(regionUrl(region.code)), "v_pro_cod"),
	}))

	return regionProvinces.flatMap(({ region, provinces }) =>
		provinces.map(province => ({
			dep: region.code,
			region: region.value,
			pro: province.code,
			provincia: province.value,
		})),
	)
}

// Fetch the districts for each province and expand to full ubigeo rows. This is the
// only SEACE traffic on a routine refresh (the skeleton comes from the DB). The
// district fetches share a single, evenly bounded concurrency.
async function scrapeDistricts(structure: ProvinceRef[]): Promise<UbigeoRow[]> {
	const perProvince = await mapPool(structure, 8, async province => {
		const districts = parseOptions(await fetchHtml(provinceUrl(province.dep, province.pro)), "v_dis_cod")
		return districts.map(district => ({
			codigo: `${province.dep}${province.pro}${district.code}`,
			region: province.region,
			provincia: province.provincia,
			distrito: district.value,
		}))
	})

	return [...perProvince.flat(), EXTERIOR]
}

// Full scrape: regions -> provinces -> districts. Used on bootstrap or when forced.
export async function scrapeFullTree(): Promise<UbigeoRow[]> {
	return scrapeDistricts(await scrapeStructure())
}

// Read the department+province skeleton from the existing Ubigeo rows. The codigo is
// dep(2)+pro(2)+dis(2), so DISTINCT dep/pro (with their names) reconstructs the
// skeleton with zero SEACE traffic. Exterior (98) is excluded — it is re-added by
// scrapeDistricts and does not have a real province page.
async function structureFromDb(sql: Sql): Promise<ProvinceRef[]> {
	return sql<ProvinceRef[]>`
		SELECT DISTINCT
			left(codigo, 2) AS dep,
			region,
			substring(codigo, 3, 2) AS pro,
			provincia
		FROM "Ubigeo"
		WHERE codigo NOT LIKE '98%'
		ORDER BY dep, pro
	`
}

// Resolve the ubigeo rows to upsert, scraping SEACE at most once per run (memoized
// so the two DB phases share the result). Full scrape on bootstrap (empty table) or
// when UBIGEO_FULL_SCRAPE=true; otherwise fetch only districts, taking the stable
// department/province skeleton from the DB so those pages are not requested again.
let resolveMemo: Promise<UbigeoRow[]> | null = null
export function resolveUbigeoRows(sql: Sql): Promise<UbigeoRow[]> {
	if (!resolveMemo) {
		resolveMemo = (async () => {
			const forceFull = Bun.env.UBIGEO_FULL_SCRAPE === "true"
			const existing = await countUbigeos(sql)
			let rows: UbigeoRow[]
			if (forceFull || existing === 0) {
				console.log(`[ubigeo] full scrape (${forceFull ? "forced" : "bootstrap: empty table"})`)
				rows = await scrapeFullTree()
			} else {
				const structure = await structureFromDb(sql)
				console.log(`[ubigeo] districts-only refresh over ${structure.length} provinces (skeleton from DB)`)
				rows = await scrapeDistricts(structure)
			}
			await Bun.write(cacheFile, JSON.stringify(rows, null, 2))
			console.log(`[ubigeo] resolved ${rows.length} ubigeos -> ${cacheFile}`)
			return rows
		})()
	}
	return resolveMemo
}

export async function countUbigeos(sql: Sql): Promise<number> {
	const rows = await sql<{ count: string }[]>`SELECT count(*)::text AS count FROM "Ubigeo"`
	return parseInt(rows[0]?.count ?? "0", 10)
}

// Upsert every scraped ubigeo: add new codes, refresh names on existing ones. It
// does NOT delete — a code no longer listed in SEACE may still be referenced by a
// persona (FK ON DELETE SET NULL would null real data), so stale rows are kept.
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

// Render the rows as a COPY block (manual seeding / inspection helper).
export function toCopySql(rows: UbigeoRow[]): string {
	const header = `COPY public."Ubigeo" (codigo, region, provincia, distrito) FROM stdin;\n`
	const body = rows.map(row => `${row.codigo}\t${row.region}\t${row.provincia}\t${row.distrito}`).join("\n")
	return `${header}${body}\n`
}
