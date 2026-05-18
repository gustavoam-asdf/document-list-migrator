# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Performance goal

This project is designed to run as a one-shot **Azure Container Job** (or equivalent: AWS Batch, GCP Cloud Run Jobs), **invoked once per day**, and squeezes **maximum throughput** out of the container during that single run. Because invocations are infrequent, image cold-start / size is not a priority — wall-clock time of the migration itself is.

**Data volume**: `padron_reducido_ruc.zip` is ~380 MB compressed and ~1.5 GB uncompressed (tens of millions of lines). Everything must stream — never read the file into memory, never accumulate a full table's worth of rows, never materialize the whole dataset as an array.

Decisions that look unusual usually exist for performance reasons:

- Worker-per-chunk-file parallelism sized via `FILE_LINES_SPLIT`.
- Postgres `COPY ... FROM STDIN` instead of multi-row `INSERT`.
- FK constraint is dropped during bulk load and recreated afterwards.
- COPY streams are kept open across many batches (one COPY transaction per worker).
- Lines are grouped in 100k-row chunks before being pushed downstream.
- High-watermark of 50 MiB on the chunk-file writers.

**Both CPUs matter — container AND database.** The migrator is built to push parallelism on the container side (one Bun `Worker` per chunk file, COPY streams kept open, batches grouped at 100k) while staying within what the Postgres instance can absorb. Tune `FILE_LINES_SPLIT` (= number of workers per DB phase) to balance the two: fewer/bigger chunks underuse the container's cores; more/smaller chunks saturate the DB's CPU/IO and start to degrade total throughput. The primary/secondary phases are deliberately run **sequentially** because both DBs typically share the same Postgres host — running them in parallel would just contend for the same DB CPU.

When editing, prefer changes that preserve or improve throughput. Don't add abstraction layers, per-row work, or "safer" defaults that re-introduce roundtrips on the hot path without a strong reason.

## Runtime & package manager

This is a **Bun** project (presence of `bun.lock`, `Bun.env`, `Bun.file`, `Worker` from Bun, `bun:` shell). Use `bun`/`bunx`, never `npm`/`pnpm`.

## Common commands

- `bun install` — install dependencies
- `bun run dev` / `bun start` — run the migrator entrypoint (`src/index.ts`)
- `bun run build` — bundles `src/index.ts` plus both worker entrypoints (`src/workers/dniWorker.ts`, `src/workers/rucWorker.ts`) into `dist/` with code splitting. The workers must be bundled as separate entrypoints because `index.ts` instantiates them via `new Worker(new URL("./workers/..."))`.
- `docker compose up` — runs the migrator alongside Postgres 16 and Redis Stack, using `.env` (see `.env.example`).

There is no test suite and no lint configuration. Don't invent commands for these.

## High-level architecture

The migrator downloads SUNAT's `padron_reducido_ruc.zip` (Peruvian RUC registry, latin1-encoded, pipe-separated), splits it into chunked files, and bulk-loads two Postgres databases in parallel.

### End-to-end pipeline (`src/index.ts`)

1. **Download & split** (`updateRucsFile.ts`): fetch the remote zip, unzip with the system `unzip` binary, stream-decode latin1, then for each line emit either:
   - a **DNI line** (only RUCs starting with `10`, which encode natural persons — DNI is extracted as `ruc.slice(2, -1)`), or
   - a **RUC line** (cleaned/normalized — see "Line-parsing quirks" below).
   Lines are written into rotating chunk files under `files/dnis/chunk_N.txt` and `files/rucs/chunk_N.txt`, rotating every `FILE_LINES_SPLIT` lines (default 5,000,000, configurable via env). Both `|ANULACION - ERROR SU|` rows are dropped.
2. **Secondary DB load**: drop FK constraint → `TRUNCATE PersonaNatural` & `PersonaJuridica` → spawn one Bun `Worker` per chunk file for DNIs and RUCs in parallel → recreate FK.
3. **Set Redis flag** `document-list:update-data-state` to `{isUpdating: true, startedAt}`.
4. **Primary DB load**: same flow as step 2 against the primary DB.
5. **Set Redis flag** back to `{isUpdating: false, lastUpdateAt}`, delete chunk dirs, `process.exit(0)`.

The two-database design means consumers can read from the *primary* DB while the *secondary* is being rewritten, then swap.

### Workers (`src/workers/{dni,ruc}Worker.ts`)

Each worker:
- receives `{filePath, useSecondaryDb, workerName}` via `postMessage`,
- streams the chunk file through `LineSplitter`/`LineSplitterWithoutHeader` + `LineGrouper(100_000)` (grouped to amortize per-batch overhead),
- writes TSV rows into a Postgres `COPY ... FROM STDIN` writable stream (`sql\`COPY ...\`.writable()` from the `postgres` package),
- keeps the COPY stream open across all batches by passing `{end: false}` to `pipeline` and stripping the stream's `error`/`close`/`finish`/`end` listeners after each batch (the comment in source warns this is intentional — needed so the COPY transaction can commit at the end),
- on batch failure, calls `retryToInsert` which recursively quarters the failing batch with fresh COPY streams until parts are <5 rows, then logs the offenders and moves on.

When editing the workers, preserve the listener-stripping and `{end: false}` pattern — removing it breaks the streaming COPY.

### Progress tracking (`WorkerPromise.ts`)

`WorkerProgressTracker` aggregates `{count}` results posted by workers. `WorkerPromise` wraps `new Worker(...)` as a promise that resolves with the worker's `{workerName, count}` message and feeds the tracker. There are four trackers per run (primary-dni, primary-ruc, secondary-dni, secondary-ruc).

### FK constraint management (`constraintManager.ts`)

Drops `PersonaJuridica_codigoUbigeo_fkey` before bulk load and recreates it after. Both operations retry up to 3 times with backoff. Drop failures are logged-and-continued; recreate failures rethrow on the last attempt. The constraint references `Ubigeo(codigo)` with `ON UPDATE CASCADE ON DELETE SET NULL`.

### Line-parsing quirks

These are encoded business rules — don't "simplify" them without understanding the source data:
- The SUNAT file is latin1, not UTF-8 (`new TextDecoderStream("latin1")` in `updateRucsFile.ts`).
- `rucParser` collapses whitespace, escapes backslashes to `\\\\`, replaces `|-+` segments with `|`, strips trailing pipes, then converts `\\\\|` back to `-`. This dance preserves literal `-` inside fields while treating leading `-` as "empty field".
- In `rucWorker`, only RUCs **not** starting with `10` (i.e., true juridical persons) get address fields populated; for `10*` RUCs all address columns are written as `\N` (Postgres COPY null marker).
- `LineSplitterWithoutHeader("RUC")` skips the first line that starts with `"RUC"` (the file header), once.

### Configuration

All config is via env vars (read through `Bun.env`):
- `DATABASE_*` (host, port, user, password) plus `DATABASE_NAME` (primary) and `DATABASE_NAME_SECONDARY`, `DATABASE_USE_SSL`.
- `REDIS_*` for the state flag.
- `FILE_LINES_SPLIT` controls chunk size = parallelism (one worker per chunk file per DB). Default 5M.

`src/db.ts` exports `primarySql` and `secondarySql` with `max: 5, prepare: false, fetch_types: false`. The two pools point at different `database` names on the same host.

### Auxiliary

- `script/ubigeoScrapper.ts` is a one-off Puppeteer scraper for the Ubigeo (region/province/district) catalog from `prod3.seace.gob.pe`. Not part of the main pipeline.
- `docker/database/structure.sql` is the canonical schema; `ubigeo-document_list-dump.sql` seeds the `Ubigeo` table.

## Code style notes from existing code

- Tabs for indentation.
- No barrel files anywhere; each module exports its own symbol(s).
- Spanish is used freely in comments, log messages, and DB column names (`razonSocial`, `nombreCompleto`, etc.) — match the existing language when editing nearby code.
