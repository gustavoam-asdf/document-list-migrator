import { readFileSync } from "node:fs"

import { logEvent } from "./logEvent"

const MB = 1024 * 1024

// cgroup v2 (/sys/fs/cgroup/memory.stat) o v1 (/sys/fs/cgroup/memory/memory.stat).
const CGROUP_FILES = [
	{ stat: "/sys/fs/cgroup/memory.stat", current: "/sys/fs/cgroup/memory.current", max: "/sys/fs/cgroup/memory.max" },
	{ stat: "/sys/fs/cgroup/memory/memory.stat", current: "/sys/fs/cgroup/memory/memory.usage_in_bytes", max: "/sys/fs/cgroup/memory/memory.limit_in_bytes" },
]

// v2: anon/file/file_dirty/file_writeback/shmem. v1: rss/cache/dirty/writeback/shmem.
const STAT_KEYS: Record<string, string> = {
	anon: "anonMB",
	rss: "anonMB",
	file: "fileMB",
	cache: "fileMB",
	file_dirty: "dirtyMB",
	dirty: "dirtyMB",
	file_writeback: "writebackMB",
	writeback: "writebackMB",
	shmem: "shmemMB",
}

function readText(path: string): string | undefined {
	try {
		return readFileSync(path, "utf8")
	} catch {
		return undefined
	}
}

function toMB(raw: string | undefined): number | undefined {
	const value = Number(raw?.trim())
	return Number.isFinite(value) ? Math.round(value / MB) : undefined
}

function sampleCgroup(): Record<string, number> {
	for (const files of CGROUP_FILES) {
		const stat = readText(files.stat)
		if (stat === undefined) {
			continue
		}
		const sample: Record<string, number> = {}
		for (const line of stat.split("\n")) {
			const [key, value] = line.split(" ")
			const field = key === undefined ? undefined : STAT_KEYS[key]
			const mb = toMB(value)
			if (field !== undefined && mb !== undefined && sample[field] === undefined) {
				sample[field] = mb
			}
		}
		const current = toMB(readText(files.current))
		const max = toMB(readText(files.max))
		if (current !== undefined) {
			sample.currentMB = current
		}
		if (max !== undefined) {
			sample.maxMB = max
		}
		return sample
	}
	return {}
}

// Una línea JSON {"event":"mem",...} cada `intervalMs`: memoria del cgroup (anon vs.
// caché de archivos) y del proceso Bun, para diagnosticar OOM que las métricas de ECS
// (cada 60 s) no alcanzan a ver.
export function startMemorySampler(intervalMs: number): () => void {
	const sample = () => {
		const proc = process.memoryUsage()
		logEvent({
			event: "mem",
			...sampleCgroup(),
			rssMB: Math.round(proc.rss / MB),
			heapUsedMB: Math.round(proc.heapUsed / MB),
			externalMB: Math.round(proc.external / MB),
			arrayBuffersMB: Math.round(proc.arrayBuffers / MB),
		})
	}
	sample()
	const timer = setInterval(sample, intervalMs)
	timer.unref()
	return () => clearInterval(timer)
}
