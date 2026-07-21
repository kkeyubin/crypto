# Task 5 Durable Live Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make routed Binance live capture durably accepted before acknowledgement, crash-recoverable, uniquely normalized across shards, and consumable only through an approved PostgreSQL live catalog.

**Architecture:** Replace the per-day JSON ledger and in-memory acceptance queue with one bounded SQLite WAL journal per symbol/dataset/day. SQLite owns event identity, normalized primary-key uniqueness, durable spool state, and prepared/published/cataloged batch state; immutable shard paths and checksums are deterministic and recoverable. PostgreSQL migration `20260721_0003` adds the formal approved live-partition registry, while the worker restores stream state, registers every `LiveWriteResult`, and acknowledges a local batch only after the catalog/state transaction commits.

**Tech Stack:** Python 3.12, stdlib `sqlite3` WAL/FULL synchronous mode, PyArrow/Parquet, SQLAlchemy 2 async, Alembic, PostgreSQL, pytest, DuckDB-safe relative paths.

## Global Constraints

- No live Binance requests, credentials, trading, mutable production data, or public port exposure.
- Use strict RED/GREEN TDD for every behavior change.
- `markPrice@1s` funding fields remain provisional observations and never become realized `FUNDING_SCHEMA` rows.
- Raw in-progress klines remain durable; only closed klines enter archive-compatible normalized storage.
- Node commands run after `source /Users/kyle/.nvm/nvm.sh && nvm use`.
- All storage paths remain descriptor-derived below the secure data root.

---

### Task 1: Parser Numeric and Market-Semantic Boundaries

**Files:**
- Modify: `services/api/src/crypto_research/market/binance/streams.py`
- Modify: `services/api/tests/market/test_stream_messages.py`

**Interfaces:**
- Consumes: `parse_stream_message(message, receive_time)`.
- Produces: the same `ParsedStreamEvent` API with all integer source fields constrained to unsigned int64 and price/quantity invariants checked.

- [ ] Add parameterized failing tests for negative and greater-than-int64 timestamps/IDs/counts, zero/negative prices, negative quantities/volumes, inverted OHLC, crossed best bid/ask, and aggregate trade ID ranges.
- [ ] Run `.venv/bin/pytest -q tests/market/test_stream_messages.py` and confirm each new case fails for the intended missing validation.
- [ ] Add `_unsigned_int64`, `_positive_decimal_text`, and `_nonnegative_decimal_text`; enforce `low <= open/close <= high`, `first_trade_id <= last_trade_id`, and `bid <= ask` without converting persisted decimal strings.
- [ ] Re-run the parser suite and Ruff until green.

### Task 2: Partition SQLite Durable Spool and Lease Fencing

**Files:**
- Create: `services/api/src/crypto_research/market/live_journal.py`
- Modify: `services/api/src/crypto_research/market/live_storage.py`
- Modify: `services/api/tests/market/test_live_storage.py`

**Interfaces:**
- Produces: `LiveStorage.accept(lease, event) -> AcceptedLiveEvent`.
- Produces: `LiveStorage.accept(lease, event) -> LiveAcceptResult`; close remains available only through `LiveWriterLease.close()`.
- Journal schema:
  `events(identity PRIMARY KEY, payload_hash, event_json, receive_time, state, batch_id)`,
  `normalized_records(normalized_key PRIMARY KEY, payload_hash, event_identity)`,
  `batches(batch_id PRIMARY KEY, state, manifest_json, created_at)` and
  `batch_events(batch_id, event_identity, PRIMARY KEY(batch_id,event_identity))`.

- [ ] Add failing tests proving `accept` commits SQLite before returning, survives a new `LiveStorage` instance, uses WAL/FULL synchronous mode, keeps journals day-partitioned and publication batches bounded, performs identity lookup without rewriting a JSON ledger, and serializes `close` against an in-flight accept/publish call.
- [ ] Add failing tests proving same identity/same payload is idempotent, same identity/different payload conflicts, closed-kline `open_time` and aggregate ID are unique normalized keys across separate batches, and same normalized key/different payload fails closed.
- [ ] Run the focused storage tests and capture RED.
- [ ] Implement structured per-day journal creation, explicit `BEGIN IMMEDIATE` transactions, normalized-key reservation, bounded publication batches, and event reconstruction with payload-hash revalidation.
- [ ] Change `LiveWriterLease.close` and every storage mutation to share the storage `RLock`, checking `active` only after acquiring it.
- [ ] Remove `events-index.json` code and re-run focused tests/Ruff to GREEN.

### Task 3: Prepared Batch Recovery and Immutable Publication

**Files:**
- Modify: `services/api/src/crypto_research/market/live_journal.py`
- Modify: `services/api/src/crypto_research/market/live_storage.py`
- Modify: `services/api/tests/market/test_live_storage.py`

**Interfaces:**
- Produces: `LiveStorage.publish_next_batch(lease, max_events=...) -> LiveWriteResult | None`.
- Produces: `LiveStorage.acknowledge_cataloged(lease, batch_id) -> None`.
- `LiveWriteResult` contains `batch_id`, immutable raw/normalized `StoredLivePartition` records, event count, symbol/dataset/date, and source-event range.

- [ ] Add failure-injection tests for crashes after batch prepare, raw publication, normalized publication, and PostgreSQL-before-local acknowledgement; a reopened storage instance must publish or return the same deterministic result without duplicates.
- [ ] Verify prepared batch manifests contain deterministic relative paths, checksums, schema name, sort/unique keys, source-event range, and row count before any final shard is made consumable.
- [ ] Run focused tests and capture RED.
- [ ] Implement bounded per-partition batch preparation, deterministic gzip NDJSON and zstd Parquet bytes, immutable object verification, prepared/published/cataloged state transitions, and startup recovery of every non-cataloged batch.
- [ ] Ensure published-but-not-cataloged shards are returned for catalog retry and never deleted or treated as approved locally.
- [ ] Re-run focused storage tests/Ruff to GREEN.

### Task 4: PostgreSQL Live Catalog and Migration 0003

**Files:**
- Create: `services/api/migrations/versions/20260721_0003_live_data_catalog.py`
- Modify: `services/api/src/crypto_research/db/models.py`
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Create: `services/api/src/crypto_research/market/live_catalog.py`
- Modify: `services/api/tests/db/test_models.py`
- Modify: `services/api/tests/db/test_repositories.py`
- Modify: `services/api/tests/db/test_postgres_integration.py`
- Create: `services/api/tests/market/test_live_catalog.py`

**Interfaces:**
- Adds `live_data_partitions` with deterministic ID, batch ID, layer, symbol, dataset, partition date, relative path, checksum, schema name, sort keys, unique keys, min/max source event time, row count, approval status, and timestamps.
- Produces: `SqlAlchemyLiveCatalogRepository.register_batch`, `list_approved_normalized`, and the database-backed `approved_parquet_paths` query boundary.
- Query boundary returns only approved normalized relative Parquet paths in deterministic range order.

- [ ] Add failing model/migration tests for checksums, positive row counts, time order, supported layer/status, unique path, and immutable batch/layer/path identity.
- [ ] Add failing repository tests proving idempotent identical registration, conflict rejection, and approved-only deterministic DuckDB-safe query results.
- [ ] Capture RED, then add migration/model/repository implementation and immutable approval timestamps.
- [ ] Extend the opt-in PostgreSQL test to migrate to head and exercise real live registration uniqueness/rollback; run it when `CRYPTO_TEST_DATABASE_URL` is available, otherwise preserve the explicit skip.
- [ ] Run DB/catalog focused suites and Ruff to GREEN.

### Task 5: Durable Worker Acceptance, Restart Recovery, and DB Resilience

**Files:**
- Modify: `services/api/src/crypto_research/market/worker.py`
- Modify: `services/api/src/crypto_research/market/__main__.py`
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Modify: `services/api/tests/market/test_worker.py`

**Interfaces:**
- `WorkerRepository.list_stream_states() -> tuple[StreamState, ...]`.
- `WorkerRepository.commit_live_batch(result, states) -> tuple[LiveDataPartition, ...]` performs catalog registration and stream-state updates in one PostgreSQL transaction.
- `handle_message` returns only after `LiveStorage.accept` durably commits.

- [ ] Add failing tests proving accepted events survive publisher retry exhaustion/restart, `LiveWriteResult` reaches the repository, and catalog acknowledgement occurs only after the database commit succeeds.
- [ ] Add failing tests proving startup restores `last_event_at`, details, and `source_mode`, then records `worker_restart` gaps from the last durable timestamp.
- [ ] Add failing tests proving every direct/proxy mode change updates an already-connected stream state, transient SQLAlchemy/asyncpg failures in the main loop retry with a bounded counter, and a failed `record_gap` keeps its anchor for retry.
- [ ] Capture RED, then replace the in-memory event queue with durable `accept` plus a bounded wakeup signal; have the publisher drain/recover SQLite batches.
- [ ] Add `SessionWorkerRepository.list_stream_states` and `commit_live_batch`; classify transient database failures separately from transport failures and preserve primary exceptions through cleanup.
- [ ] Re-run worker, reconnect, storage, DB, and runtime suites to GREEN.

### Task 6: Documentation and Final Verification

**Files:**
- Modify: `.superpowers/sdd/task-5-report.md`
- Modify: `docs/superpowers/specs/2026-07-21-phase-1-binance-data-foundation-design.md`

**Interfaces:**
- Documents durable acceptance, crash recovery, catalog-only consumption, provisional funding, migration, and remaining deployment acceptance gates.

- [ ] Run `.venv/bin/pytest -q tests/market`.
- [ ] Run `.venv/bin/pytest -q` and record the opt-in PostgreSQL status separately.
- [ ] Run `.venv/bin/ruff check src tests migrations` and `.venv/bin/python scripts/export_schemas.py --check`.
- [ ] Run Node contract generation/type checks and `git diff --exit-code contracts` under NVM.
- [ ] Run `git diff --check`, update the report with exact results, commit with a Conventional Commit message, and send the commit hash for re-review.
