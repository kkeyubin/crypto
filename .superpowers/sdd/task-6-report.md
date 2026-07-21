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
- Active stream health joins enabled symbols so disabled stale streams cannot
  degrade current operations health.

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

## Verification

Passed:

```bash
cd services/api && .venv/bin/pytest -q tests/routes tests/test_api.py
cd services/api && .venv/bin/pytest -q
cd services/api && .venv/bin/ruff check src tests migrations
cd services/api && .venv/bin/python scripts/export_schemas.py --check
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:test-generation
npm run contracts:check-types
npm run contracts:types && git diff --exit-code contracts
cd services/api && .venv/bin/alembic heads
cd services/api && .venv/bin/alembic history
git diff --check
```

The focused Task 6 suite passed with `30 passed`; the full API suite passed with
`563 passed, 1 skipped`. Contract generation,
contract TypeScript checking, generated-schema drift checks, Ruff, exported
schema checks, Alembic continuity, and whitespace checks passed. Alembic has one
head: `20260721_0004`.

The single skipped test is the existing PostgreSQL integration test gated by
`CRYPTO_TEST_DATABASE_URL`. This machine has neither a Docker executable nor a
PostgreSQL listener on `127.0.0.1:5432`, so a live PostgreSQL run could not be
performed locally. Production dependency construction and transaction lifecycle
are covered with real SQLAlchemy types and request-scoped test doubles, but live
PostgreSQL behavior remains the explicit environment-dependent verification
risk.

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
- `.superpowers/sdd/task-6-report.md`
