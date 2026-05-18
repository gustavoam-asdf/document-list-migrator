import {
	CONNECTION_HEADROOM,
	INITIAL_CONCURRENCY,
	MAX_CONCURRENCY_ENV,
	MAX_CONCURRENCY_FALLBACK,
	MIN_CONCURRENCY,
	PROBE_INTERVAL_MS,
	PROBE_THRESHOLD_PCT,
	SHUTDOWN_GRACE_MS,
	WORKER_LIVENESS_TIMEOUT_MS,
} from "../constants"
import { installSignalHandlers, shouldExit } from "../shared/shutdown"
import { spawnWorker, type WorkerHandle, type WorkerProgressMessage } from "./WorkerPromise"

type WorkerType = "dni" | "ruc"

type ChunkTask = {
	workerType: WorkerType
	filePath: string
	chunkIndex: number
}

export type PhaseResult = {
	phase: string
	totalInserted: number
	totalRejected: number
	dniInserted: number
	rucInserted: number
	peakConcurrency: number
	finalTarget: number
	initialTarget: number
	maxConcurrency: number
	avgRps: number
	elapsedMs: number
	terminatedWorkers: number
	shutdownTriggered: boolean
}

export type RunPhaseParams = {
	phase: "secondary" | "primary"
	useSecondaryDb: boolean
	dniChunkFiles: string[]
	rucChunkFiles: string[]
	maxConnections: number
}

export function resolveMaxConcurrency(maxConnections: number): number {
	if (MAX_CONCURRENCY_ENV !== undefined) {
		return MAX_CONCURRENCY_ENV
	}
	const derived = Math.max(MIN_CONCURRENCY, maxConnections - CONNECTION_HEADROOM)
	return Math.min(derived, MAX_CONCURRENCY_FALLBACK)
}

export async function runPhase(params: RunPhaseParams): Promise<PhaseResult> {
	const { phase, useSecondaryDb, dniChunkFiles, rucChunkFiles, maxConnections } = params
	const start = Date.now()

	installSignalHandlers()

	const queue: ChunkTask[] = [
		...rucChunkFiles.map((filePath, i) => ({ workerType: "ruc" as WorkerType, filePath, chunkIndex: i })),
		...dniChunkFiles.map((filePath, i) => ({ workerType: "dni" as WorkerType, filePath, chunkIndex: i })),
	]
	let queueIdx = 0

	const maxConcurrency = resolveMaxConcurrency(maxConnections)
	if (maxConcurrency < MIN_CONCURRENCY) {
		throw new Error(
			`Resolved maxConcurrency=${maxConcurrency} is below MIN_CONCURRENCY=${MIN_CONCURRENCY} ` +
			`(max_connections=${maxConnections}, headroom=${CONNECTION_HEADROOM})`,
		)
	}
	let target = Math.min(INITIAL_CONCURRENCY, maxConcurrency, queue.length || MIN_CONCURRENCY)

	console.log(
		`[supervisor:${phase}] phase start | queue=${queue.length} (ruc=${rucChunkFiles.length}, dni=${dniChunkFiles.length}) ` +
		`| max_connections=${maxConnections} maxConcurrency=${maxConcurrency} initialTarget=${target}`,
	)

	type ProgressEvent = { ts: number; rows: number }
	const progressEvents: ProgressEvent[] = []
	const WINDOW_MS = PROBE_INTERVAL_MS

	let totalInserted = 0
	let totalRejected = 0
	let dniInserted = 0
	let rucInserted = 0
	let peakConcurrency = 0
	let lastWindowRps: number | null = null
	let probeCount = 0
	let rpsAccumulator = 0
	let terminatedWorkers = 0
	let shutdownAnnounced = false

	type ActiveEntry = {
		handle: WorkerHandle
		lastProgressAt: number
		startedAt: number
		settled: Promise<void>
	}
	const active = new Map<string, ActiveEntry>()

	function trimWindow() {
		const cutoff = Date.now() - WINDOW_MS
		while (progressEvents.length > 0 && progressEvents[0]!.ts < cutoff) {
			progressEvents.shift()
		}
	}

	function windowRps(): number {
		trimWindow()
		if (progressEvents.length === 0) return 0
		const rows = progressEvents.reduce((s, e) => s + e.rows, 0)
		const spanMs = Math.max(WINDOW_MS, Date.now() - progressEvents[0]!.ts)
		return rows / (spanMs / 1000)
	}

	function handleProgress(name: string, workerType: WorkerType, msg: WorkerProgressMessage) {
		const entry = active.get(name)
		if (entry) entry.lastProgressAt = Date.now()
		progressEvents.push({ ts: Date.now(), rows: msg.rows })
		totalInserted += msg.rows
		totalRejected += msg.rejected
		if (workerType === "dni") dniInserted += msg.rows
		else rucInserted += msg.rows
	}

	function spawnOne(): boolean {
		if (queueIdx >= queue.length) return false
		if (shouldExit()) return false
		const task = queue[queueIdx++]!
		const name = `${task.workerType}-${phase}-${task.chunkIndex}`
		const workerPath = task.workerType === "dni"
			? "../workers/dniWorker.ts"
			: "../workers/rucWorker.ts"

		const handle = spawnWorker({
			workerPath,
			name,
			startMessage: { filePath: task.filePath, useSecondaryDb },
			onProgress: msg => handleProgress(name, task.workerType, msg),
		})

		const settled = handle.promise.then(() => undefined, err => {
			console.error(`[supervisor:${phase}] worker ${name} crashed:`, err)
		})

		const now = Date.now()
		active.set(name, { handle, lastProgressAt: now, startedAt: now, settled })
		peakConcurrency = Math.max(peakConcurrency, active.size)
		settled.finally(() => active.delete(name))
		return true
	}

	function fillSlots() {
		while (active.size < target && queueIdx < queue.length) {
			if (!spawnOne()) break
		}
	}

	function adjustTarget() {
		const rps = windowRps()
		probeCount++
		rpsAccumulator += rps

		let arrow = "="
		if (lastWindowRps !== null && lastWindowRps > 0 && rps > 0) {
			const delta = (rps - lastWindowRps) / lastWindowRps
			const threshold = PROBE_THRESHOLD_PCT / 100
			if (delta > threshold && target < maxConcurrency) {
				target++
				arrow = "↑"
			} else if (delta < -threshold && target > MIN_CONCURRENCY) {
				target--
				arrow = "↓"
			}
		}

		const deltaPct = lastWindowRps !== null && lastWindowRps > 0
			? (((rps - lastWindowRps) / lastWindowRps) * 100).toFixed(1) + "%"
			: "—"

		console.log(
			`[supervisor:${phase}] active=${active.size}/${target} max=${maxConcurrency} ` +
			`rps=${Math.round(rps).toLocaleString()} Δ=${deltaPct} ${arrow} ` +
			`queue=${queue.length - queueIdx} done=${totalInserted.toLocaleString()} rejected=${totalRejected}`,
		)

		lastWindowRps = rps
	}

	// Watchdog: si un worker no reportó progress en WORKER_LIVENESS_TIMEOUT_MS,
	// asumimos que está colgado y lo terminamos. La conexión COPY queda
	// `idle in transaction` hasta que el keepalive TCP del Postgres la detecte
	// (capa 2 de la defensa).
	function checkLiveness() {
		const now = Date.now()
		for (const [name, entry] of active) {
			const sinceProgress = now - entry.lastProgressAt
			if (sinceProgress > WORKER_LIVENESS_TIMEOUT_MS) {
				console.error(
					`[supervisor:${phase}] worker ${name} unresponsive for ${Math.round(sinceProgress / 1000)}s — terminating`,
				)
				entry.handle.terminate()
				terminatedWorkers++
				active.delete(name)
			}
		}
	}

	function announceShutdown() {
		if (shutdownAnnounced) return
		shutdownAnnounced = true
		console.warn(`[supervisor:${phase}] propagating shutdown to ${active.size} active workers`)
		for (const entry of active.values()) {
			entry.handle.requestShutdown()
		}
	}

	fillSlots()

	const timer = setInterval(() => {
		if (shouldExit()) announceShutdown()
		adjustTarget()
		checkLiveness()
		fillSlots()
	}, PROBE_INTERVAL_MS)

	try {
		while (active.size > 0) {
			if (shouldExit()) announceShutdown()
			// Carrera entre: cualquier worker termine, o el grace period venza.
			const drainPromises = [...active.values()].map(e => e.settled)
			if (shutdownAnnounced) {
				// Una vez en shutdown, damos al grace period absoluto, después terminate.
				const graceTimeout = new Promise<"grace">(resolve =>
					setTimeout(() => resolve("grace"), SHUTDOWN_GRACE_MS),
				)
				const winner = await Promise.race([
					Promise.race(drainPromises).then(() => "drained" as const),
					graceTimeout,
				])
				if (winner === "grace") {
					console.error(
						`[supervisor:${phase}] grace period (${SHUTDOWN_GRACE_MS}ms) expired — force terminating ${active.size} workers`,
					)
					for (const entry of active.values()) {
						entry.handle.terminate()
						terminatedWorkers++
					}
					active.clear()
					break
				}
			} else {
				await Promise.race(drainPromises)
				fillSlots()
			}
		}
	} finally {
		clearInterval(timer)
	}

	const elapsedMs = Date.now() - start
	const avgRps = probeCount > 0 ? rpsAccumulator / probeCount : (totalInserted / (elapsedMs / 1000))

	const result: PhaseResult = {
		phase,
		totalInserted,
		totalRejected,
		dniInserted,
		rucInserted,
		peakConcurrency,
		finalTarget: target,
		initialTarget: Math.min(INITIAL_CONCURRENCY, maxConcurrency),
		maxConcurrency,
		avgRps,
		elapsedMs,
		terminatedWorkers,
		shutdownTriggered: shutdownAnnounced,
	}

	const tag = shutdownAnnounced ? "PHASE ABORTED (shutdown)" : "PHASE DONE"
	console.log(
		`[supervisor:${phase}] ${tag} in ${(elapsedMs / 1000).toFixed(1)}s | ` +
		`inserted ${totalInserted.toLocaleString()} (dni ${dniInserted.toLocaleString()} + ruc ${rucInserted.toLocaleString()}) ` +
		`| rejected ${totalRejected} | terminated ${terminatedWorkers} ` +
		`| avg ${Math.round(avgRps).toLocaleString()} rps ` +
		`| concurrency: initial=${result.initialTarget} peak=${peakConcurrency} final=${target} max=${maxConcurrency}`,
	)

	return result
}
