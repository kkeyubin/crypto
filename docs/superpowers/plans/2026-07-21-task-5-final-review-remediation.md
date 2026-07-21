# Task 5 Final Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve migration history and make live spool recovery, decimal persistence, and live catalog time filtering fail closed and dataset-correct.

**Architecture:** Keep `20260721_0003` byte-identical to its first committed form and add `20260721_0004` for canonical event-time metadata and stronger catalog constraints. Live SQLite journals remain a fixed 64-bucket set keyed by stream uniqueness domain; startup rejects legacy day journals with explicit recovery guidance. Parsing canonicalizes numeric zero before a message can reach durable accept, while catalog selection and DuckDB filtering use dataset-specific canonical time rather than Binance source event time.

**Tech Stack:** Python 3.12, pytest, SQLite WAL, SQLAlchemy 2, Alembic, PostgreSQL, PyArrow, DuckDB, React/Vitest.

## Global Constraints

- Do not access live Binance, credentials, trading, mutable production data, or public ports.
- Use test-first RED-GREEN cycles for every behavior change.
- `20260721_0003` must match commit `38ee93a` byte-for-byte.
- Existing catalog rows without trustworthy canonical ranges must make `20260721_0004` fail with recovery instructions; never infer from source event time.
- The reviewed `0003` implementation and intermediate commits were never deployed to the server.

---

### Task 1: Immutable Migration History and Canonical Catalog Migration

**Files:**
- Modify: `services/api/tests/market/test_live_catalog.py`
- Restore: `services/api/migrations/versions/20260721_0003_live_data_catalog.py`
- Create: `services/api/migrations/versions/20260721_0004_live_canonical_time.py`
- Modify: `services/api/src/crypto_research/db/models.py`

**Interfaces:**
- Produces: non-null `min_canonical_time` and `max_canonical_time` columns on `LiveDataPartitionRow`.
- Produces: a migration that aborts if pre-existing `live_data_partitions` rows prevent trustworthy canonical backfill.

- [ ] Add a test comparing 0003 to `git show 38ee93a:.../20260721_0003_live_data_catalog.py`, plus offline SQL assertions for a 0004 revision chained from 0003.
- [ ] Run the tests and confirm failure because 0003 was mutated and 0004 does not exist.
- [ ] Restore 0003 exactly, add 0004 columns/index/constraints with an explicit empty-table guard, and update the ORM model.
- [ ] Run the migration/model tests and Alembic offline generation to GREEN.

### Task 2: Legacy Spool Fail-Closed Startup

**Files:**
- Modify: `services/api/tests/market/test_live_storage.py`
- Modify: `services/api/src/crypto_research/market/live_storage.py`

**Interfaces:**
- Produces: startup detection for `spool/binance/usdm/<symbol>/<dataset>/date=*/journal.sqlite3` that never follows symlinks and raises `LiveStorageError` with the recovery command.

- [ ] Add tests for a real legacy journal and a symlinked legacy tree.
- [ ] Run them and confirm current startup silently ignores the legacy spool.
- [ ] Implement descriptor-relative bounded directory traversal and explicit fail-closed guidance.
- [ ] Run storage tests to GREEN, including fixed-bucket restart recovery.

### Task 3: Arrow-Safe Scientific Zero

**Files:**
- Modify: `services/api/tests/market/test_stream_messages.py`
- Modify: `services/api/tests/market/test_worker.py`
- Modify: `services/api/src/crypto_research/market/binance/streams.py`

**Interfaces:**
- Produces: every decimal field stores numeric zero as the exact string `"0"`; positive fields still reject zero and signed zero.

- [ ] Add table-driven parser coverage for `0E+100`, `0E+1000`, and `-0E+100` across all decimal fields, plus a worker durable-accept assertion.
- [ ] Run and confirm unbounded zero exponents are currently preserved.
- [ ] Canonicalize any finite numeric zero to `"0"` before semantic sign checks and before returning `ParsedStreamEvent`.
- [ ] Run parser/worker/storage decimal tests to GREEN.

### Task 4: Dataset-Canonical Query Timeline

**Files:**
- Modify: `services/api/src/crypto_research/market/live_storage.py`
- Modify: `services/api/src/crypto_research/market/live_catalog.py`
- Modify: `services/api/tests/market/test_live_storage.py`
- Modify: `services/api/tests/market/test_live_catalog.py`
- Modify: `services/api/tests/db/test_models.py`
- Modify: `services/api/tests/db/test_postgres_integration.py`

**Interfaces:**
- `StoredLivePartition` adds `min_canonical_time` and `max_canonical_time`.
- Canonical mapping is `klines.open_time`, `agg_trades.transact_time`, `book_ticker.transact_time`, and `mark_price.event_time`.
- `list_approved_normalized` selects shard overlap only on canonical columns.

- [ ] Add failing storage/catalog tests for provenance range versus canonical range and a final kline whose source event crosses midnight while `open_time` stays in the prior day.
- [ ] Add two adjacent one-day DuckDB queries proving only the canonical day returns the kline.
- [ ] Populate canonical ranges in manifests/results, persist them, validate exact dataset mapping, and switch repository overlap/row guards to canonical columns.
- [ ] Run live storage/catalog/model/integration tests to GREEN.

### Task 5: Design and Recovery Documentation

**Files:**
- Modify: `docs/superpowers/specs/2026-07-21-phase-1-binance-data-foundation-design.md`
- Modify: `docs/superpowers/plans/2026-07-21-task-5-durable-live-recovery.md`
- Modify: `docs/runbooks/market-data-recovery.md`
- Modify: `.superpowers/sdd/task-5-report.md`

**Interfaces:**
- Documents `spool/binance/usdm/bucket=00..3f/journal.sqlite3`, legacy detection, canonical/source ranges, and 0004 recovery commands.

- [ ] Replace obsolete per-day journal claims with fixed-bucket topology and per-event `partition_key` behavior.
- [ ] Record that 0003/intermediate commits were not deployed and that 0004 intentionally aborts on unexpected rows.
- [ ] Add copyable detection, backup, empty-catalog verification, migration, and restart commands.
- [ ] Search documentation for stale per-day journal statements and correct all Task 5 references.

### Task 6: Final Verification and Commit

**Files:**
- Verify all modified files.

- [ ] Run `cd services/api && .venv/bin/pytest -q` and `.venv/bin/ruff check src tests`.
- [ ] Run schema export check and Alembic offline upgrade SQL.
- [ ] Run contracts generation/type checks and verify no generated drift.
- [ ] Run Web Vitest and production build using NVM Node 24.
- [ ] Inspect `git diff --check`, commit with a Conventional Commit message, and request independent final review.
