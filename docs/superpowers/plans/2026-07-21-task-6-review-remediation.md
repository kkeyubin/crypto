# Task 6 Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the market-data control API fail closed on incomplete evidence, remain correct under concurrent requests, and keep all database work bounded and symbol-scoped.

**Architecture:** SQLAlchemy repositories acquire PostgreSQL transaction advisory locks for mutation identities, project durable object state, and return bulk operational evidence. The service merges validated current archive intervals, applies exact four-stream and heartbeat freshness gates, and keeps routes thin. PostgreSQL-only invariants are covered by the existing opt-in integration suite.

**Tech Stack:** Python 3.12, FastAPI, Pydantic 2, SQLAlchemy 2 async, asyncpg, PostgreSQL, Pytest, Ruff.

## Global Constraints

- Strict TDD: each production change follows an observed focused RED and GREEN.
- `DELETE` only disables; no data, history, files, manifests, or audit rows are deleted.
- No API accepts exchange credentials, arbitrary source URLs, paths, or SQL.
- All timestamps and interval comparisons use UTC and half-open `[start, end)` ranges.
- PostgreSQL integration remains opt-in through `CRYPTO_TEST_DATABASE_URL`.
- The requested delivery is one reviewed fix commit after the complete verification matrix.

---

### Task 1: Current archive coverage and metadata trust

**Files:**
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Modify: `services/api/src/crypto_research/market/control.py`
- Modify: `services/api/tests/routes/test_control.py`
- Modify: `services/api/tests/routes/test_repository.py`

**Interfaces:**
- Produces: `list_symbol_summaries(symbols)` with per-symbol current manifest intervals, metadata trust, gaps, and effective job statuses.
- Consumes: configured `SymbolState.history_start/history_end` and approved `DataManifest` values.

- [x] Add a failing service test where kline profile coverage is complete but mark-price or funding has an internal gap; assert `data_ready` and eligibility remain blocked.
- [x] Add a failing repository test proving stale, wrong-source, empty, wrong-symbol, non-perpetual, and non-trading metadata snapshots remain unverified.
- [x] Add a failing repository SQL test requiring symbol, required dataset, approval, latest version, and coarse date predicates before manifest parsing.
- [x] Implement validated current-manifest extraction and an interval merge that accepts adjacent/overlapping half-open ranges but rejects every hidden gap.
- [x] Define trusted metadata as source `binance_usdm_exchange_info`, age at most 24 hours, and a payload containing a matching `PERPETUAL`/`TRADING` symbol entry.
- [x] Run the focused tests until green.

### Task 2: Exact live and operations health gates

**Files:**
- Modify: `services/api/src/crypto_research/market/control.py`
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Modify: `services/api/tests/routes/test_control.py`

**Interfaces:**
- Consumes: `streams_for_symbols((symbol,))` as the canonical four-stream expectation.
- Produces: per-symbol eligibility and global operations health that require all expected rows to be connected and fresh.

- [x] Add failing tests for each missing, stale, degraded, and duplicate/unexpected stream case.
- [x] Add failing operations tests for more than 100 streams and absent, stopped, degraded, or stale worker heartbeats.
- [x] Implement the exact expected-name gate with `live_stale_after_seconds`; no single recent stream can stand in for another.
- [x] Query all active stream rows and fail source/archive health closed unless the worker is `running` and fresh.
- [x] Run the focused tests until green.

### Task 3: Disabled claim gate and object-derived job status

**Files:**
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Modify: `services/api/tests/db/test_repositories.py`
- Modify: `services/api/tests/routes/test_repository.py`

**Interfaces:**
- Produces: `_effective_job_status(persisted, object_states)` where objects always dominate when present.
- Produces: `claim_backfill_object` restricted through job ownership to enabled symbols.

- [x] Add a failing status matrix test: failed wins, all approved succeeds, any active state runs, planned/source-pending queues, and only zero objects fall back to persisted state.
- [x] Add a failing claim SQL/behavior test requiring joins to `ingestion_jobs` and enabled `symbols`.
- [x] Implement the status projection and claim restriction without changing lease attempt fencing.
- [x] Run focused repository and runner tests until green.

### Task 4: Concurrent idempotent mutations

**Files:**
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Modify: `services/api/tests/routes/test_repository.py`
- Modify: `services/api/tests/db/test_postgres_integration.py`

**Interfaces:**
- Produces: symbol lock key `crypto-research:symbol:<SYMBOL>` and job/object identity locks acquired with `pg_advisory_xact_lock(hashtextextended(key, 0))`.

- [x] Add failing statement tests requiring lock-before-read for add/enable/disable and deterministic job/object writes.
- [x] Add opt-in two-session PostgreSQL tests proving identical requests serialize to one row/audit, conflicting identities fail, and state-change audit occurs once.
- [x] Acquire transaction locks, expire cached ORM state where needed, then re-read and compare immutable identity before insert/update.
- [x] Run unit tests and the PostgreSQL test when `CRYPTO_TEST_DATABASE_URL` is available.

### Task 5: Bounded bulk summaries and HTTP query boundary

**Files:**
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Modify: `services/api/src/crypto_research/market/catalog.py`
- Modify: `services/api/src/crypto_research/routes/symbols.py`
- Modify: `services/api/src/crypto_research/routes/data.py`
- Modify: `services/api/tests/routes/test_symbols.py`
- Modify: `services/api/tests/routes/test_data.py`
- Modify: `services/api/tests/routes/test_repository.py`
- Modify: `services/api/tests/market/test_catalog.py`

**Interfaces:**
- Produces: one bulk summary request per paginated symbol list, grouped gap/status aggregates, and date-prefiltered approved catalog reads.

- [x] Add a failing list test that rejects per-symbol summary calls and accepts one bulk summary result.
- [x] Add failing SQL tests for grouped summaries and archive catalog symbol/dataset/date prefilters.
- [x] Add failing POST tests showing unknown query parameters return redacted `422`.
- [x] Implement bulk summary projection, grouped aggregate queries, current bounded coverage reads, catalog prefilters, and `Depends(no_query)` on both POST routes.
- [x] Run focused API, repository, and catalog tests until green.

### Task 6: PostgreSQL acceptance, report, and fix commit

**Files:**
- Modify: `services/api/tests/db/test_postgres_integration.py`
- Modify: `.superpowers/sdd/task-6-report.md`

**Interfaces:**
- Verifies: concurrent symbol/job idempotency, complete archive coverage, exact four-stream eligibility, disabled claim behavior, and object status projection against PostgreSQL.

- [x] Run `cd services/api && .venv/bin/pytest -q tests/routes tests/test_api.py tests/db/test_repositories.py tests/market/test_catalog.py`.
- [x] Run `cd services/api && .venv/bin/pytest -q` and record the exact pass/skip result.
- [x] Run Ruff, schema export, contract generation/type/drift checks, Alembic head/history, and `git diff --check`.
- [x] Update the Task 6 report with every observed RED, final command result, and the live PostgreSQL limitation if the opt-in URL is absent.
- [x] Review the staged diff, confirm no unrelated changes or secrets, and commit with `fix: harden market data control invariants`.
