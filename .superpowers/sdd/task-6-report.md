# Task 6 Report: Symbol and Market Data Control API

## Scope

Implemented the Phase 1 Task 6 HTTP control surface for symbol configuration,
archive backfill planning, persisted market-data inspection, eligibility, stream
state, and redacted operations health.

This task does not add a trading API, credentials, arbitrary URLs, arbitrary
filesystem paths, arbitrary SQL, public ports, or destructive market-data
deletion. `DELETE /api/symbols/{symbol}` disables collection while preserving
historical state. The existing `20260721_0004` migration already contains the
required durable fields, so no `0005` migration was needed.

## TDD evidence

The route, service, repository, and transaction behavior was developed with
focused failing tests first. Material RED checkpoints included:

1. Route tests initially failed with
   `ModuleNotFoundError: crypto_research.market.control` before the market-data
   control service and routers existed.
2. The production service-factory failure test observed zero rollbacks before
   request transaction cleanup was corrected.
3. The approved-partition repository test initially found no approval predicate
   in the generated SQL.
4. A coverage-isolation test initially observed live datasets
   `('kline_1m', 'mark_price')`, proving that live mark ticks could incorrectly
   substitute for official archive `markPriceKlines` coverage.
5. An `InterfaceError` regression initially returned the mapped availability
   response before the broad database handler was narrowed to operational
   unavailability only.
6. JSON string booleans such as `"false"` initially passed request validation
   before the HTTP adapters used `StrictBool`.
7. Backfill creation initially persisted zero archive objects before the service
   was connected to `ArchiveBackfillPlanner`.
8. Completed durable archive objects initially left their parent API job in
   `queued`; repository projection now derives the effective job state.
9. A disabled symbol with a stale persisted stream initially degraded global
   health before active stream queries were restricted to enabled symbols.

Each checkpoint was made green with a focused test before broader regression
testing.

The rejected-review remediation also followed focused RED/GREEN cycles. The
material RED observations were:

10. A symbol with complete kline profile evidence but an internal mark-price
    gap was reported `data_ready`.
11. A validated manifest with a declared `missing_intervals` hole was exposed
    as one continuous archive interval.
12. The summary trusted an arbitrary metadata row without checking source,
    freshness, symbol identity, `PERPETUAL`, or `TRADING` status.
13. Eligibility accepted one recent stream as a substitute for the exact four
    required Binance USD-M streams; missing and stale peers did not fail closed.
14. Operations health materialized only the first 100 active stream rows and
    did not fail closed for absent, stopped, or stale worker heartbeats.
15. Persisted job state could override durable object state, and failed-job
    health double-counted jobs rather than using the same effective projection.
16. A disabled symbol's planned object remained claimable because the claim SQL
    did not join through the owning job to enabled symbols.
17. Concurrent idempotent writes had no transaction-scoped identity lock, so
    lock-before-read statement tests initially found no
    `pg_advisory_xact_lock(hashtextextended(...))` calls.
18. Symbol listing performed one summary query per symbol, while the catalog
    query lacked coarse date predicates before exact Python overlap checks.
19. Both POST routes returned `200` for an unknown query parameter before the
    strict query-boundary dependency was added.

Each remediation test was observed failing for the stated reason before its
production change, then rerun green. The final missing-interval regression and
its related archive/control tests passed as `11 passed`.

## Implementation

### Public HTTP surface

- `GET /api/symbols`
- `POST /api/symbols`
- `GET /api/symbols/{symbol}`
- `DELETE /api/symbols/{symbol}`
- `POST /api/symbols/{symbol}/backfills`
- `GET /api/backfills/{job_id}`
- `GET /api/symbols/{symbol}/partitions`
- `GET /api/symbols/{symbol}/gaps`
- `GET /api/symbols/{symbol}/profile`
- `GET /api/symbols/{symbol}/eligibility`
- `GET /api/symbols/{symbol}/streams`
- `GET /api/operations/market-data`

The existing liveness and readiness routes remain public. OpenAPI exposes only
health and the market-data control surface.

### Service rules

- Symbol configuration is normalized and idempotent. Repeating the same active
  configuration succeeds; changing identity-bearing configuration conflicts;
  re-adding a disabled symbol enables it and records audit evidence.
- Path/body symbol mismatches conflict, missing resources return `404`, and
  configuration conflicts return `409`.
- Backfills require UTC, non-empty closed ranges and enforce
  `history_max_days`.
- Historical ingestion uses only approved Binance Vision archive datasets.
  `aggTrades` requires both symbol-level and request-level explicit opt-in.
- Backfill IDs are deterministic per symbol, data type, and time range.
- Backfill creation persists structured `BackfillObject` plans through the
  existing archive planner; it does not merely create an orphan job record.
- Lists use stable ordering and bounded `limit`/`offset` pagination. Unknown
  query parameters are rejected.
- Paginated symbol lists use one bulk summary operation with four bounded or
  grouped SQL query classes, avoiding per-symbol N+1 reads. Catalog reads apply
  symbol, dataset, approval, and coarse partition-date predicates before exact
  half-open interval validation in Python.
- Profile and eligibility evidence remains symbol-scoped, including BTC/PEPE
  isolation.
- Eligibility delegates to the existing Phase 1 policy/evidence service.
- Operations output is intentionally redacted and fails closed when durable
  evidence is absent.

### Persistence and request lifecycle

- Production requests create a real SQLAlchemy session and a
  `SqlAlchemyDataStateRepository`-backed service.
- Successful requests commit. Handler, factory, and commit failures roll back.
  Sessions always close.
- Only SQLAlchemy `OperationalError` is translated to a redacted `503` database
  availability response. Programming errors are not swallowed.
- Validation responses remove raw input/context fields so secrets or untrusted
  payload fragments are not echoed.
- Repository reads are symbol-scoped, approval-scoped, stably ordered, and
  bounded before materialization.
- Effective backfill status and failed-job counts include durable object state.
- Claim selection joins through jobs to enabled symbols, so disabling a symbol
  blocks new claims while preserving lease-attempt fencing; enabling it allows
  the queued object to resume.
- Mutations serialize normalized symbol, deterministic job, and deterministic
  object identities with PostgreSQL transaction advisory locks. Fresh rereads
  preserve idempotency, reject conflicting immutable identity, and emit audit
  events once per actual state change.
- Active stream health joins enabled symbols and aggregates the exact expected
  four stream names per active symbol. Missing, disconnected, stale, future, or
  unexpected-only observations cannot satisfy the gate.

## Review hardening

Self-review added or corrected the following after the initial green route
surface:

- rollback and close coverage for service-factory and programming failures;
- approval predicates in partition SQL;
- strict JSON boolean validation;
- archive-object planning during backfill creation;
- effective job status derived from durable object state;
- exclusion of live mark ticks from official archive eligibility evidence;
- disabled-symbol exclusion from active stream health;
- narrow `OperationalError` handling and redacted validation errors.

The final rejected-review pass additionally hardened all ten reported
invariants:

- Archive readiness now requires current, approved, validated kline, mark-price,
  and funding manifests to provide gapless coverage of the configured range.
  Declared manifest holes are subtracted before adjacent/overlapping intervals
  are merged.
- Metadata is verified only from a fresh `binance_usdm_exchange_info` snapshot
  whose payload contains the matching `PERPETUAL` and `TRADING` symbol.
- Eligibility requires the canonical four live streams for that symbol, and
  operations health aggregates every active symbol rather than a first-100
  window.
- Worker heartbeat health requires a present, `running`, fresh, non-future
  heartbeat. Archive/source health fails closed otherwise.
- Durable object states always dominate persisted parent job state: failed wins,
  all approved succeeds, active states run, planned/source-pending queue, and
  only zero objects fall back to the persisted state.
- Disabled symbols cannot yield new claims; reenabling preserves and resumes the
  same durable object with its lease-attempt fence intact.
- Symbol/job/object mutations use deterministic transaction advisory locks and
  conflict-safe rereads.
- Symbol summaries are bulk and grouped, catalog reads are date-prefiltered,
  pagination remains bounded, and both POST endpoints reject unknown query
  parameters with redacted `422` responses.
- A real two-session PostgreSQL acceptance test now covers concurrent
  idempotency/audit behavior plus Task 6 status, claim, coverage, and stream
  invariants when the opt-in database URL is available.

## Verification

Passed:

```bash
cd services/api && .venv/bin/pytest -q tests/routes tests/test_api.py
cd services/api && .venv/bin/pytest -q tests/routes tests/test_api.py tests/db/test_repositories.py tests/market/test_catalog.py
cd services/api && .venv/bin/pytest -q
cd services/api && .venv/bin/ruff check src tests migrations
cd services/api && .venv/bin/python scripts/export_schemas.py --check
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:test-generation
npm run contracts:check-types
npm run contracts:types && git diff --exit-code contracts
npm run web:test -- --run
npm run web:build
cd services/api && .venv/bin/alembic heads
cd services/api && .venv/bin/alembic history
git diff --check
```

The expanded focused Task 6 suite passed with `67 passed`; the full API suite
passed with `577 passed, 1 skipped`. Contract generation, contract TypeScript
checking, generated-schema drift checks, Web tests, the Web production build
(`50 modules transformed`), Ruff, exported schema checks, Alembic continuity,
and whitespace checks passed. Alembic has one head: `20260721_0004`; the review
required no schema change, so no `0005` migration was added.

The single skipped module is the PostgreSQL integration suite gated by
`CRYPTO_TEST_DATABASE_URL`. It now includes real two-session Task 6 concurrency
and invariant coverage. On this machine `CRYPTO_TEST_DATABASE_URL` is unset,
Docker is unavailable, and no PostgreSQL listener is present on
`127.0.0.1:5432`, so that opt-in acceptance run could not be executed locally.
The test is committed and remains the explicit environment-dependent
verification risk; it is not reported as passed.

## Files

- `services/api/src/crypto_research/api.py`
- `services/api/src/crypto_research/db/repositories.py`
- `services/api/src/crypto_research/market/control.py`
- `services/api/src/crypto_research/routes/__init__.py`
- `services/api/src/crypto_research/routes/symbols.py`
- `services/api/src/crypto_research/routes/data.py`
- `services/api/src/crypto_research/routes/operations.py`
- `services/api/tests/test_api.py`
- `services/api/tests/routes/conftest.py`
- `services/api/tests/routes/test_symbols.py`
- `services/api/tests/routes/test_data.py`
- `services/api/tests/routes/test_operations.py`
- `services/api/tests/routes/test_control.py`
- `services/api/tests/routes/test_repository.py`
- `services/api/tests/db/test_postgres_integration.py`
- `services/api/tests/db/test_repositories.py`
- `services/api/tests/market/test_catalog.py`
- `docs/superpowers/plans/2026-07-21-task-6-review-remediation.md`
- `.superpowers/sdd/task-6-report.md`
