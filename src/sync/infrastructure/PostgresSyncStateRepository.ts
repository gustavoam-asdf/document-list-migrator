import type { Sql, TransactionSql } from "postgres"

import { DELTA_STREAMS, type DeltaStream, type Entity } from "../domain/Entity"
import type { Baseline, SyncState, SyncTarget } from "../domain/SyncState"
import type { SyncStateRepository } from "../application/SyncStateRepository"

// Schema propio del migrador: Prisma (dueño del schema `public`) no lo ve ni lo migra.
const SCHEMA_DDL = `
	CREATE SCHEMA IF NOT EXISTS migrator;
	CREATE TABLE IF NOT EXISTS migrator.sync_state (
		id smallint PRIMARY KEY CHECK (id = 1),
		applied_snapshot_id text,
		target_snapshot_id text,
		target_baseline_snapshot_id text,
		target_started_at timestamptz,
		updated_at timestamptz NOT NULL DEFAULT now()
	);
	INSERT INTO migrator.sync_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
	CREATE TABLE IF NOT EXISTS migrator.sync_progress (
		stream text PRIMARY KEY,
		done bigint NOT NULL
	);
	CREATE TABLE IF NOT EXISTS migrator.sync_rejects (
		entity text NOT NULL,
		key text NOT NULL,
		line text NOT NULL,
		error text NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now(),
		last_failed_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (entity, key)
	);
`

type RawStateRow = {
	applied_snapshot_id: unknown
	target_snapshot_id: unknown
	target_baseline_snapshot_id: unknown
	target_started_ms: unknown
}

export class PostgresSyncStateRepository implements SyncStateRepository {
	constructor(private readonly sql: Sql) {}

	async ensureSchema(): Promise<void> {
		await this.sql.begin(async tx => {
			await tx.unsafe("SET LOCAL client_min_messages = warning")
			await tx.unsafe(SCHEMA_DDL)
		})
	}

	async read(): Promise<SyncState> {
		const rows = await this.sql<RawStateRow[]>`
			SELECT applied_snapshot_id, target_snapshot_id, target_baseline_snapshot_id,
				(extract(epoch FROM target_started_at) * 1000)::float8 AS target_started_ms
			FROM migrator.sync_state WHERE id = 1
		`
		const row = rows[0]
		if (!row) {
			throw new Error("migrator.sync_state sin fila: correr ensureSchema primero")
		}
		return parseState(row)
	}

	async beginTarget(snapshotId: string, baseline: Baseline): Promise<SyncTarget> {
		const baselineId = baseline.kind === "snapshot" ? baseline.snapshotId : null
		const startedMs = await this.sql.begin(async tx => {
			const rows = await tx<{ started_ms: unknown }[]>`
				UPDATE migrator.sync_state
				SET target_snapshot_id = ${snapshotId},
					target_baseline_snapshot_id = ${baselineId},
					target_started_at = now(),
					updated_at = now()
				WHERE id = 1
				RETURNING (extract(epoch FROM target_started_at) * 1000)::float8 AS started_ms
			`
			await resetProgressRows(tx)
			return rows[0]?.started_ms
		})
		if (typeof startedMs !== "number") {
			throw new Error("beginTarget: no se pudo fijar el objetivo")
		}
		return { snapshotId, baseline, startedAt: new Date(startedMs) }
	}

	async resetProgress(): Promise<void> {
		await this.sql.begin(tx => resetProgressRows(tx))
	}

	async progress(): Promise<Record<DeltaStream, number>> {
		const rows = await this.sql<{ stream: unknown; done: unknown }[]>`
			SELECT stream, done::text AS done FROM migrator.sync_progress
		`
		const progress: Record<DeltaStream, number> = { "dni:upsert": 0, "dni:delete": 0, "ruc:upsert": 0, "ruc:delete": 0 }
		for (const row of rows) {
			const stream = DELTA_STREAMS.find(s => s === row.stream)
			if (stream && typeof row.done === "string") {
				progress[stream] = Number(row.done)
			}
		}
		return progress
	}

	async forcedKeys(entity: Entity): Promise<Set<string>> {
		const rows = await this.sql<{ key: unknown }[]>`
			SELECT r.key FROM migrator.sync_rejects r, migrator.sync_state s
			WHERE s.id = 1 AND r.entity = ${entity} AND r.created_at < s.target_started_at
		`
		return new Set(rows.flatMap(r => (typeof r.key === "string" ? [r.key] : [])))
	}

	async complete(target: SyncTarget): Promise<void> {
		await this.sql.begin(async tx => {
			// Rechazos que no volvieron a fallar en este objetivo ya quedaron resueltos.
			await tx`
				DELETE FROM migrator.sync_rejects r USING migrator.sync_state s
				WHERE s.id = 1 AND r.last_failed_at < s.target_started_at
			`
			const updated = await tx`
				UPDATE migrator.sync_state
				SET applied_snapshot_id = target_snapshot_id,
					target_snapshot_id = NULL,
					target_baseline_snapshot_id = NULL,
					target_started_at = NULL,
					updated_at = now()
				WHERE id = 1 AND target_snapshot_id = ${target.snapshotId}
			`
			if (updated.count !== 1) {
				throw new Error(`complete: el objetivo en la BD ya no es ${target.snapshotId}`)
			}
			await tx`DELETE FROM migrator.sync_progress`
		})
	}

	async markUnknown(): Promise<void> {
		await this.sql.begin(async tx => {
			await tx`
				UPDATE migrator.sync_state
				SET applied_snapshot_id = NULL,
					target_snapshot_id = NULL,
					target_baseline_snapshot_id = NULL,
					target_started_at = NULL,
					updated_at = now()
				WHERE id = 1
			`
			await tx`DELETE FROM migrator.sync_progress`
		})
	}
}

async function resetProgressRows(tx: TransactionSql): Promise<void> {
	await tx`DELETE FROM migrator.sync_progress`
	for (const stream of DELTA_STREAMS) {
		await tx`INSERT INTO migrator.sync_progress (stream, done) VALUES (${stream}, 0)`
	}
}

function optionalText(value: unknown, field: string): string | null {
	if (value === null) {
		return null
	}
	if (typeof value !== "string" || value.length === 0) {
		throw new Error(`migrator.sync_state.${field} inválido`)
	}
	return value
}

function parseState(row: RawStateRow): SyncState {
	const applied = optionalText(row.applied_snapshot_id, "applied_snapshot_id")
	const target = optionalText(row.target_snapshot_id, "target_snapshot_id")
	const baselineId = optionalText(row.target_baseline_snapshot_id, "target_baseline_snapshot_id")

	if (target !== null) {
		if (typeof row.target_started_ms !== "number") {
			throw new Error("migrator.sync_state: objetivo sin target_started_at")
		}
		const baseline: Baseline = baselineId === null ? { kind: "database" } : { kind: "snapshot", snapshotId: baselineId }
		return { kind: "applying", target: { snapshotId: target, baseline, startedAt: new Date(row.target_started_ms) } }
	}
	return applied === null ? { kind: "unknown" } : { kind: "idle", appliedSnapshotId: applied }
}
