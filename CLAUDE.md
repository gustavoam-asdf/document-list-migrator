# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Goals

One-shot **ECS Fargate scheduled job**, **once per day**, that keeps `PersonaNatural` / `PersonaJuridica` in sync with SUNAT's `padron_reducido_ruc.zip` with **incremental, non-disruptive** updates:

- **RDS cost first.** The container runs for a short window; the RDS instance is always on. Do heavy work in the container (download, parse, sort, diff) and send the DB only the rows that changed.
- **The DB never goes down and reads are not saturated.** No `TRUNCATE`, no DDL on persona tables, no long transactions: just short upsert/delete transactions (`MIGRATOR_SYNC_BATCH_ROWS`, default 20k) that take row locks only, with a pause between batches (`MIGRATOR_SYNC_BATCH_PAUSE_MS`). There is a single DB (the secondary was retired).
- **Resumable.** Each batch commits its rows **and** its progress in the same transaction. A run killed at any point (SIGTERM, crash, timeout) resumes on the next run from the last committed batch.
- **No periodic full reload.** The last resort is a diff against a **read-only dump of the DB**, not a truncate.

**Data volume**: zip ~380 MB, ~1.5 GB uncompressed, ~18M rows. Everything must stream: never read a whole file into memory or accumulate a table's worth of rows.

## Runtime & package manager

**Bun** project. Use `bun`/`bunx`, never `npm`/`pnpm`. The image needs the system `unzip`, `sort` (coreutils) and `curl` binaries.

## Commands

- `bun install`
- `bun run dev` / `bun start`: run `src/index.ts`
- `bun run test`: unit tests (`tests/`, `bun:test`)
- `bun run typecheck`: `tsc --noEmit`
- `bun run build`: bundle to `dist/`
- `docker compose up`: migrator + Postgres 16 + Redis Stack, using `.env` (see `.env.example`)

There is no lint configuration.

## Architecture (`src/sync/`, soft clean architecture)

- `domain/`: pure logic. `diffSortedSnapshots` (streaming merge-join of two key-sorted files → upserts/deletes), `parsePadronLine` (SUNAT line → COPY-text row), `SyncState`.
- `application/`: `runIncrementalSync` (use case) plus its ports, one interface per file. `ResumableBatcher` groups the delta into batches and skips the ones already confirmed.
- `infrastructure/`: S3 (`S3SnapshotStore`, `awsCredentials`), Postgres (`PostgresDeltaApplier`, `PostgresSyncStateRepository`, `PostgresDatabaseBaseline`, `acquireSyncLock`), SUNAT (`SunatSnapshotSource`), `sortTsvByKey`.
- `src/index.ts`: composition root.

### Run flow

1. Ping the DB, take a session advisory lock (a second concurrent run exits 0), refresh Ubigeo if due, check that the persona tables have their PK (`ON CONFLICT` needs it).
2. If `migrator.sync_state` has an unfinished target, **resume it first**: re-diff the same inputs (deterministic) and skip the `migrator.sync_progress` count per stream.
3. Download the padrón and write `dni.tsv` / `ruc.tsv` in COPY-text format, sorted by key. SUNAT usually ships it sorted by RUC; if not, external `sort` (`LC_ALL=C`).
4. Upload the snapshot to S3 (`<prefix>/<id>/{dni,ruc}.tsv` + `manifest.json`, written last = complete).
5. Pick the baseline: the last applied snapshot in S3; otherwise (first run, missing snapshot, unknown state, `MIGRATOR_FORCE_DATABASE_BASELINE=true`) a dump of the DB. The dump is paged by key (keyset over the PK, `MIGRATOR_BASELINE_PAGE_ROWS`, with `MIGRATOR_BASELINE_PAGE_PAUSE_MS` between pages): short index queries, no minutes-long seq scan or snapshot. A container sort runs only if the DB order doesn't match byte order.
6. Diff and apply dni and ruc in parallel, then `complete` (applied = target), prune S3 down to the current snapshot plus the previous one, and set Redis `document-list:update-data-state` = `{isUpdating:false,lastUpdateAt}`. `isUpdating:true` is never written anymore.

Exit codes: 0 ok; 130 interrupted by a signal (progress saved); 1 failure (the next run resumes).

### Log contract (CloudWatch alarms depend on it)

`src/shared/logEvent.ts` writes single-line JSON to stdout. Metric filters in andape-deploy alert on `{ $.event = "sync_summary" && $.pendingRejects > 0 }` and `{ $.event = "sync_warning" && $.level = "error" }`. **Don't rename `event`, `status`, `pendingRejects`, `rejected` or `level`**: if they change, the alarms stop firing and nobody notices.
- `sync_summary`: one per run, with `status` ∈ ok|skipped|interrupted|failed.
- `sync_warning`: non-fatal failures (`component` ∈ ubigeo|s3-prune|lock|unlogged-table).

### Container memory (Fargate 4 GB)

- **Large transfers never go through Bun's network stack.** On Linux, `fetch`, `Bun.write(path, response)` and the `S3Client` (upload, `stream()`, `slice().arrayBuffer()`) keep everything transferred in native memory (a 1.7 GB ruc.tsv means ~1.8 GB of RSS). That caused the OOMs of 1.4.0 and 1.4.1 in Fargate (upstream issues oven-sh/bun#42722 and #29083). The padrón and the snapshot data use `curl` (`src/net/curlTransfer.ts`) with presigned URLs, at ~8 MB. `S3Client` is only for small operations (manifest, list, delete, presign).
- Big file writers wait for the disk (`flush`) so a fast producer doesn't pile up in memory.
- Anonymous memory peak: the two parallel `sort` processes (`MIGRATOR_SORT_BUFFER`, 512M each). Verified with a 2 GB limit.
- Every 5 s the migrator writes `{"event":"mem",...}` (cgroup anon/file/dirty + Bun RSS/heap) to diagnose OOMs that ECS metrics, sampled every 60 s, miss.

### Sync state (`migrator` schema in the DB)

Created by the migrator itself (`ensureSchema`). Prisma, which owns `public`, does not see it.

- `sync_state`: `applied_snapshot_id`, plus `target_*` while a delta is being applied.
- `sync_progress`: items confirmed per stream (`dni:upsert`, `dni:delete`, `ruc:upsert`, `ruc:delete`).
- `sync_rejects`: rows the DB rejected (class 22/23 errors, e.g. a value too long). They are isolated by bisecting the batch and retried as forced upserts on the next target. They are deleted once they stop failing.

Invariant: the state lives in the same DB as the data, so a PITR restore brings the matching `applied_snapshot_id` along with it.

### Line-parsing quirks

These are business rules; don't "simplify" them without understanding the source data:
- The file is latin1, not UTF-8.
- `parseRucLine` collapses whitespace, escapes backslashes to `\\`, collapses `|-+` to `|`, strips trailing pipes, then turns `\\|` back into `-`. This preserves literal `-` inside a field while treating a dash-only field as empty.
- RUCs starting with `10` (natural persons) get every address column as `\N`. The DNI is `ruc.slice(2, -1)`.
- The header (first line starting with `RUC`) is skipped once, and `|ANULACION - ERROR SU|` rows are dropped.
- `escapeCopyField` escapes exactly what `COPY TO` emits (`\\ \t \n \r \b \f \v`): the diff compares snapshot rows against DB dumps byte for byte.

### Configuration (`Bun.env`, see `src/constants.ts`)

- `DATABASE_*` (host, port, user, password, name, `DATABASE_USE_SSL`), `REDIS_*`.
- `MIGRATOR_SNAPSHOT_BUCKET`, `MIGRATOR_SNAPSHOT_REGION` (required), `MIGRATOR_SNAPSHOT_PREFIX`. In ECS, credentials come from the task role (container credentials endpoint); elsewhere, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`.
- `MIGRATOR_SYNC_BATCH_ROWS`, `MIGRATOR_SYNC_BATCH_PAUSE_MS`, `MIGRATOR_FORCE_DATABASE_BASELINE`, `MIGRATOR_BASELINE_PAGE_ROWS`, `MIGRATOR_BASELINE_PAGE_PAUSE_MS`, `MIGRATOR_SORT_BUFFER`, `MIGRATOR_SORT_PARALLEL`.

### Auxiliary

- `src/ubigeo.ts` + `docs/ubigeo.md`: Ubigeo refresh from INEI (upsert, Sundays or when the table is empty).
- `script/ubigeoScrapper.ts`: one-off Puppeteer scraper, not part of the pipeline.
- `docker/database/structure.sql`: reference schema (the api's Prisma is the owner).

## Code style

- Tabs for indentation.
- No barrel files; one symbol per port/adapter file.
- Spanish in comments, logs and DB columns; match the existing language.
