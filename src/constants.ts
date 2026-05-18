export const remoteZipFile = "http://www2.sunat.gob.pe/padron_reducido_ruc.zip"
export const rootDir = process.cwd()
export const filesDir = `${rootDir}/files`
export const localZipFile = `${filesDir}/list.zip`
export const localFile = `${filesDir}/padron_reducido_ruc.txt`

export const dnisDir = `${filesDir}/dnis`
export const rucsDir = `${filesDir}/rucs`
export const rejectsDir = `${filesDir}/rejects`

// Tamaño del chunk file. Solo controla memoria + granularidad de retry,
// el paralelismo ahora lo decide el supervisor (AIMD). Default 1M.
export const FILE_LINES_SPLIT = parseInt(Bun.env.FILE_LINES_SPLIT || "1000000", 10)

// Filas por COPY pipeline call dentro del worker. Fila RUC pesa ~6x la DNI,
// por eso el default RUC es menor.
export const BATCH_ROWS_DNI = parseInt(Bun.env.MIGRATOR_BATCH_ROWS_DNI || "200000", 10)
export const BATCH_ROWS_RUC = parseInt(Bun.env.MIGRATOR_BATCH_ROWS_RUC || "70000", 10)

// Supervisor / AIMD
export const INITIAL_CONCURRENCY = parseInt(Bun.env.MIGRATOR_INITIAL_CONCURRENCY || "4", 10)
export const MIN_CONCURRENCY = parseInt(Bun.env.MIGRATOR_MIN_CONCURRENCY || "2", 10)
// Si no se setea, se deriva de SHOW max_connections al arrancar la fase.
export const MAX_CONCURRENCY_ENV = Bun.env.MIGRATOR_MAX_CONCURRENCY
	? parseInt(Bun.env.MIGRATOR_MAX_CONCURRENCY, 10)
	: undefined
export const MAX_CONCURRENCY_FALLBACK = 16
// Margen reservado en max_connections para otras conexiones (lectores, monitoreo).
export const CONNECTION_HEADROOM = parseInt(Bun.env.MIGRATOR_CONNECTION_HEADROOM || "5", 10)
export const PROBE_INTERVAL_MS = parseInt(Bun.env.MIGRATOR_PROBE_INTERVAL_MS || "15000", 10)
// Umbral en % de cambio de throughput para subir/bajar concurrency.
export const PROBE_THRESHOLD_PCT = parseFloat(Bun.env.MIGRATOR_PROBE_THRESHOLD_PCT || "5")

// Tiempo máximo que un worker puede pasar sin reportar progress antes de
// asumir que está colgado y terminarlo. Default 5 min — generoso para no
// matar workers en batches grandes/lentos pero rápido vs el default TCP.
export const WORKER_LIVENESS_TIMEOUT_MS = parseInt(Bun.env.MIGRATOR_WORKER_LIVENESS_TIMEOUT_MS || "300000", 10)

// Tiempo que damos a los workers para drenar su batch actual y cerrar COPY
// cuando recibimos SIGTERM/SIGINT. Si lo exceden, worker.terminate() forzado.
export const SHUTDOWN_GRACE_MS = parseInt(Bun.env.MIGRATOR_SHUTDOWN_GRACE_MS || "60000", 10)
