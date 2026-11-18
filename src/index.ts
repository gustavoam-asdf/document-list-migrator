import {
	BASELINE_MAX_ROWS_PER_SEC,
	BASELINE_PAGE_BLOCKS,
	MEMORY_SAMPLE_MS,
	FORCE_DATABASE_BASELINE,
	SNAPSHOT_BUCKET,
	SNAPSHOT_ENDPOINT,
	SNAPSHOT_PREFIX,
	SNAPSHOT_REGION,
	SYNC_MAX_ROWS_PER_SEC,
	SYNC_BATCH_ROWS,
	baselineWorkDir,
	rejectsDir,
	remoteZipFile,
	snapshotCacheDir,
	snapshotWorkDir,
} from "./constants"
import { countUbigeos, getUbigeos, refreshUbigeoTable } from "./ubigeo"
import { installSignalHandlers, shouldExit, shutdownReasonText } from "./shared/shutdown"
import { pingDb, primarySql, verifyTables } from "./db"

import { PostgresDatabaseBaseline } from "./sync/infrastructure/PostgresDatabaseBaseline"
import { PostgresDeltaApplier } from "./sync/infrastructure/PostgresDeltaApplier"
import { PostgresSyncStateRepository } from "./sync/infrastructure/PostgresSyncStateRepository"
import { S3SnapshotStore } from "./sync/infrastructure/S3SnapshotStore"
import { SunatSnapshotSource } from "./sync/infrastructure/SunatSnapshotSource"
import { SyncInterruptedError } from "./sync/application/SyncInterruptedError"
import { acquireSyncLock } from "./sync/infrastructure/acquireSyncLock"
import { errorMessage } from "./shared/errorMessage"
import fs from "node:fs/promises"
import { redis } from "./redis"
import { runIncrementalSync, type SyncOutcome } from "./sync/application/runIncrementalSync"
import { logEvent } from "./shared/logEvent"
import { startMemorySampler } from "./shared/startMemorySampler"
import { RateLimiter } from "./shared/RateLimiter"
import { primaryPsqlEnv } from "./psqlEnv"
import { verifyUpsertPrerequisites } from "./sync/infrastructure/verifyUpsertPrerequisites"

const startTime = Date.now()
console.log(`PID: ${process.pid}`)

installSignalHandlers()
if (MEMORY_SAMPLE_MS > 0) {
	startMemorySampler(MEMORY_SAMPLE_MS)
}

const stateKey = "document-list:update-data-state"

// El api solo lee `lastUpdateAt`. `isUpdating: true` ya no se escribe: no hay BD
// secundaria a la que desviar lecturas y la sincronización no deja tablas vacías.
type UpdateDataState = { isUpdating: false; lastUpdateAt: Date }

function wait(ms: number) {
	return new Promise<void>(resolve => setTimeout(resolve, ms))
}

async function shutdownAndExit(code: number): Promise<never> {
	console.log("[main] closing pool and exiting")
	try { await Promise.race([primarySql.end({ timeout: 5 }), wait(7_000)]) } catch (e) { console.warn("[main] primarySql.end failed", e) }
	try { redis.disconnect() } catch (e) { console.warn("[main] redis.disconnect failed", e) }
	process.exit(code)
}

// Ubigeo se refresca desde INEI los domingos (UTC) o con la tabla vacía. Es un upsert:
// no bloquea lecturas. Best-effort: si INEI falla se sigue con lo que hay.
async function refreshUbigeoIfDue(): Promise<void> {
	const ubigeoCount = await countUbigeos(primarySql)
	const forceRefresh = Bun.env.UBIGEO_FORCE_REFRESH === "true"
	const isSunday = new Date().getUTCDay() === 0
	if (!isSunday && ubigeoCount > 0 && !forceRefresh) {
		console.log(`[primary] Ubigeo left as-is (${ubigeoCount} rows, not Sunday)`)
		return
	}
	const reason = ubigeoCount === 0 ? "empty table (bootstrap)" : forceRefresh ? "forced" : "weekly refresh (Sunday)"
	try {
		console.log(`[primary] refreshing Ubigeo from INEI: ${reason}`)
		await refreshUbigeoTable(primarySql, "primary", await getUbigeos())
	} catch (err) {
		warn("ubigeo", `refresh desde INEI falló, se sigue con la tabla actual: ${errorMessage(err)}`)
	}
}

function warn(component: string, message: string): void {
	logEvent({ event: "sync_warning", level: "error", component, message })
}

const applier = new PostgresDeltaApplier(primarySql, new RateLimiter(SYNC_MAX_ROWS_PER_SEC))
const state = new PostgresSyncStateRepository(primarySql)

type Outcome =
	| { kind: "locked" }
	| { kind: "done"; sync: SyncOutcome; pendingRejects: number }

async function main(): Promise<Outcome> {
	if (!SNAPSHOT_BUCKET || !SNAPSHOT_REGION) {
		throw new Error("faltan MIGRATOR_SNAPSHOT_BUCKET / MIGRATOR_SNAPSHOT_REGION")
	}

	await pingDb(primarySql, "primary")
	await verifyTables(primarySql, "primary", ["PersonaNatural", "PersonaJuridica"])

	const releaseLock = await acquireSyncLock(primarySql)
	if (!releaseLock) {
		console.warn("[main] otra corrida tiene el lock de sincronización: no hay nada que hacer")
		return { kind: "locked" }
	}

	try {
		await refreshUbigeoIfDue()
		await verifyUpsertPrerequisites(primarySql)
		await state.ensureSchema()

		const sync = await runIncrementalSync({
			source: new SunatSnapshotSource({ remoteZipUrl: remoteZipFile, workDir: snapshotWorkDir, rejectsDir }),
			store: new S3SnapshotStore({ bucket: SNAPSHOT_BUCKET, region: SNAPSHOT_REGION, prefix: SNAPSHOT_PREFIX, endpoint: SNAPSHOT_ENDPOINT, cacheDir: snapshotCacheDir }),
			state,
			applier,
			databaseBaseline: new PostgresDatabaseBaseline(primarySql, primaryPsqlEnv(), baselineWorkDir, {
				pageBlocks: BASELINE_PAGE_BLOCKS,
				limiter: new RateLimiter(BASELINE_MAX_ROWS_PER_SEC),
			}),
			batchSize: SYNC_BATCH_ROWS,
			forceDatabaseBaseline: FORCE_DATABASE_BASELINE,
			now: () => new Date(),
			stopReason: () => (shouldExit() ? (shutdownReasonText() ?? "señal") : null),
			log: message => console.log(message),
			warn,
		})

		// `lastUpdateAt` es la fecha de los datos que muestra el api: sin cambios, no se toca.
		if (sync.kind === "applied" || sync.resumedSnapshotId !== null) {
			const updateState: UpdateDataState = { isUpdating: false, lastUpdateAt: new Date() }
			await redis.set(stateKey, JSON.stringify(updateState))
		}

		await fs.rm(snapshotWorkDir, { recursive: true, force: true })
		await fs.rm(baselineWorkDir, { recursive: true, force: true })
		await fs.rm(snapshotCacheDir, { recursive: true, force: true })

		return { kind: "done", sync, pendingRejects: await state.pendingRejects() }
	} finally {
		await releaseLock().catch(error => warn("lock", `no se pudo liberar el lock: ${errorMessage(error)}`))
	}
}

const durationSec = () => Math.round((Date.now() - startTime) / 1000)

function summarize(outcome: Outcome): void {
	if (outcome.kind === "locked") {
		logEvent({ event: "sync_summary", status: "skipped", reason: "locked", durationSec: durationSec() })
		return
	}
	const { sync: result, pendingRejects } = outcome
	if (result.kind === "unchanged") {
		logEvent({
			event: "sync_summary",
			status: "skipped",
			reason: "source-unchanged",
			appliedSnapshotId: result.appliedSnapshotId,
			resumedSnapshotId: result.resumedSnapshotId,
			pendingRejects,
			durationSec: durationSec(),
		})
		return
	}
	const { dni, ruc } = result.entities
	logEvent({
		event: "sync_summary",
		status: "ok",
		snapshotId: result.snapshotId,
		resumedSnapshotId: result.resumedSnapshotId,
		baseline: result.baseline.kind,
		upserts: dni.upserts + ruc.upserts,
		deletes: dni.deletes + ruc.deletes,
		unchanged: dni.unchanged + ruc.unchanged,
		rejected: applier.rejectedCount,
		pendingRejects,
		parseRejects: result.parseRejects.dni + result.parseRejects.ruc,
		durationSec: durationSec(),
		dni: { upserts: dni.upserts, deletes: dni.deletes, unchanged: dni.unchanged },
		ruc: { upserts: ruc.upserts, deletes: ruc.deletes, unchanged: ruc.unchanged },
	})
}

let exitCode: number
try {
	summarize(await main())
	exitCode = 0
} catch (error) {
	const interrupted = error instanceof SyncInterruptedError
	if (interrupted) {
		console.warn(`[main] ${error.message} — el progreso quedó guardado, la próxima corrida retoma`)
	} else {
		console.error("[main] sync falló — la próxima corrida retoma desde el último lote confirmado:", error)
	}
	logEvent({
		event: "sync_summary",
		status: interrupted ? "interrupted" : "failed",
		error: errorMessage(error),
		rejected: applier.rejectedCount,
		durationSec: durationSec(),
	})
	exitCode = interrupted ? 130 : 1
}
await shutdownAndExit(exitCode)
