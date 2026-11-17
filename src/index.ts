import {
	BASELINE_PAGE_PAUSE_MS,
	BASELINE_PAGE_ROWS,
	FORCE_DATABASE_BASELINE,
	SNAPSHOT_BUCKET,
	SNAPSHOT_ENDPOINT,
	SNAPSHOT_PREFIX,
	SNAPSHOT_REGION,
	SYNC_BATCH_PAUSE_MS,
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
import { runIncrementalSync } from "./sync/application/runIncrementalSync"
import { verifyUpsertPrerequisites } from "./sync/infrastructure/verifyUpsertPrerequisites"

const startTime = Date.now()
console.log(`PID: ${process.pid}`)

installSignalHandlers()

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
		console.warn(`[primary] Ubigeo refresh failed, continuing with existing table: ${errorMessage(err)}`)
	}
}

async function main(): Promise<number> {
	if (!SNAPSHOT_BUCKET || !SNAPSHOT_REGION) {
		throw new Error("faltan MIGRATOR_SNAPSHOT_BUCKET / MIGRATOR_SNAPSHOT_REGION")
	}

	await pingDb(primarySql, "primary")
	await verifyTables(primarySql, "primary", ["PersonaNatural", "PersonaJuridica"])

	const releaseLock = await acquireSyncLock(primarySql)
	if (!releaseLock) {
		console.warn("[main] otra corrida tiene el lock de sincronización: no hay nada que hacer")
		return 0
	}

	try {
		await refreshUbigeoIfDue()
		await verifyUpsertPrerequisites(primarySql)

		const state = new PostgresSyncStateRepository(primarySql)
		await state.ensureSchema()

		const result = await runIncrementalSync({
			source: new SunatSnapshotSource({ remoteZipUrl: remoteZipFile, workDir: snapshotWorkDir, rejectsDir }),
			store: new S3SnapshotStore({ bucket: SNAPSHOT_BUCKET, region: SNAPSHOT_REGION, prefix: SNAPSHOT_PREFIX, endpoint: SNAPSHOT_ENDPOINT, cacheDir: snapshotCacheDir }),
			state,
			applier: new PostgresDeltaApplier(primarySql, SYNC_BATCH_PAUSE_MS),
			databaseBaseline: new PostgresDatabaseBaseline(primarySql, baselineWorkDir, {
				pageRows: BASELINE_PAGE_ROWS,
				pauseMs: BASELINE_PAGE_PAUSE_MS,
			}),
			batchSize: SYNC_BATCH_ROWS,
			forceDatabaseBaseline: FORCE_DATABASE_BASELINE,
			now: () => new Date(),
			stopReason: () => (shouldExit() ? (shutdownReasonText() ?? "señal") : null),
			log: message => console.log(message),
		})

		const updateState: UpdateDataState = { isUpdating: false, lastUpdateAt: new Date() }
		await redis.set(stateKey, JSON.stringify(updateState))

		await fs.rm(snapshotWorkDir, { recursive: true, force: true })
		await fs.rm(baselineWorkDir, { recursive: true, force: true })
		await fs.rm(snapshotCacheDir, { recursive: true, force: true })

		const { dni, ruc } = result.entities
		console.log(
			`\n========== ALL DONE in ${((Date.now() - startTime) / 1000).toFixed(1)}s ==========\n` +
			`snapshot: ${result.snapshotId}` +
			(result.resumedSnapshotId ? ` (antes se retomó ${result.resumedSnapshotId})` : "") + "\n" +
			`baseline: ${result.baseline.kind === "snapshot" ? result.baseline.snapshotId : "volcado de la BD"}\n` +
			`dni: upserts=${dni.upserts} deletes=${dni.deletes} sin cambio=${dni.unchanged}\n` +
			`ruc: upserts=${ruc.upserts} deletes=${ruc.deletes} sin cambio=${ruc.unchanged}\n`,
		)
		return 0
	} finally {
		await releaseLock().catch(error => console.warn(`[main] no se pudo liberar el lock: ${errorMessage(error)}`))
	}
}

let exitCode: number
try {
	exitCode = await main()
} catch (error) {
	if (error instanceof SyncInterruptedError) {
		console.warn(`[main] ${error.message} — el progreso quedó guardado, la próxima corrida retoma`)
		exitCode = 130
	} else {
		console.error("[main] sync falló — la próxima corrida retoma desde el último lote confirmado:", error)
		exitCode = 1
	}
}
await shutdownAndExit(exitCode)
