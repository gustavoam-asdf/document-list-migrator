import { filesDir } from "../src/constants"
import { scrapeFullTree, toCopySql } from "../src/ubigeo"

// Manual tool: dump the full ubigeo tree from SEACE to files/ for inspection or
// one-off seeding. The migrator refreshes Ubigeo on its own (Sundays / bootstrap),
// so this is not part of the deploy — just a way to eyeball the data.
const rows = await scrapeFullTree()

await Bun.write(`${filesDir}/ubigeo.json`, JSON.stringify(rows, null, 2))
await Bun.write(`${filesDir}/ubigeo.copy.sql`, toCopySql(rows))

console.log(`Done: ${rows.length} ubigeos -> ${filesDir}/ubigeo.json + ${filesDir}/ubigeo.copy.sql`)
