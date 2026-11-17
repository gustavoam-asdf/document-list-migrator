export const remoteZipFile = Bun.env.MIGRATOR_PADRON_URL || "http://www2.sunat.gob.pe/padron_reducido_ruc.zip"
export const rootDir = process.cwd()
export const filesDir = `${rootDir}/files`
export const snapshotWorkDir = `${filesDir}/snapshot`
export const baselineWorkDir = `${filesDir}/baseline`
export const snapshotCacheDir = `${filesDir}/snapshot-cache`
export const rejectsDir = `${filesDir}/rejects`

// Filas por transacción al aplicar el delta. Lotes cortos = locks de fila breves y
// poco trabajo perdido si se corta la corrida.
export const SYNC_BATCH_ROWS = parseInt(Bun.env.MIGRATOR_SYNC_BATCH_ROWS || "20000", 10)
// Tope de filas/s escritas en la BD (dni + ruc). 0 = sin límite. A 20k/s un delta
// diario de miles de filas no se nota; una carga desde cero (34M) toma ~30 min.
export const SYNC_MAX_ROWS_PER_SEC = parseInt(Bun.env.MIGRATOR_SYNC_MAX_ROWS_PER_SEC || "20000", 10)

// Volcado de la BD (baseline de último recurso): bloques de 8 KB por consulta y tope de
// filas/s leídas (ambas tablas). 0 = sin límite. A 100k/s, 34M filas ≈ 6 min.
export const BASELINE_PAGE_BLOCKS = parseInt(Bun.env.MIGRATOR_BASELINE_PAGE_BLOCKS || "2048", 10)
export const BASELINE_MAX_ROWS_PER_SEC = parseInt(Bun.env.MIGRATOR_BASELINE_MAX_ROWS_PER_SEC || "100000", 10)

// Último recurso manual: ignora el snapshot de S3 y calcula el delta contra un volcado de la BD.
export const FORCE_DATABASE_BASELINE = Bun.env.MIGRATOR_FORCE_DATABASE_BASELINE === "true"

export const SNAPSHOT_BUCKET = Bun.env.MIGRATOR_SNAPSHOT_BUCKET ?? ""
export const SNAPSHOT_REGION = Bun.env.MIGRATOR_SNAPSHOT_REGION || Bun.env.AWS_REGION || ""
export const SNAPSHOT_PREFIX = Bun.env.MIGRATOR_SNAPSHOT_PREFIX || "snapshots"
// Solo para S3 compatibles fuera de AWS (MinIO en local). Vacío = AWS.
export const SNAPSHOT_ENDPOINT = Bun.env.MIGRATOR_SNAPSHOT_ENDPOINT || undefined

// Intervalo de la línea JSON {"event":"mem"} (diagnóstico de OOM). 0 = apagado.
export const MEMORY_SAMPLE_MS = parseInt(Bun.env.MIGRATOR_MEMORY_SAMPLE_MS || "0", 10)

// `sort` externo: memoria antes de volcar a disco y threads.
export const SORT_BUFFER = Bun.env.MIGRATOR_SORT_BUFFER || "512M"
export const SORT_PARALLEL = parseInt(Bun.env.MIGRATOR_SORT_PARALLEL || "2", 10)
