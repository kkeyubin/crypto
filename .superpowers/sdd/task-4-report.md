# Task 4: Backfill Orchestration, Catalog, Gaps, Profiles, and Eligibility

## Status

Complete. Phase 1 now has a restart-safe archive backfill state machine, a
versioned approved catalog, descriptor-anchored DuckDB reads, explicit gap and
repair evidence, approved-data-only profiles, centralized fail-closed
eligibility, and independently degraded public REST capabilities.

## Delivered behavior

- Backfill objects persist the exact sequence `planned -> downloading ->
  checksum_verified -> normalized -> validated -> catalog_approved`, with
  `source_pending` and `failed` terminal alternatives. Every transition and its
  evidence is committed before the next external stage starts.
- PostgreSQL claims use `FOR UPDATE SKIP LOCKED`, database `now()`, lease expiry,
  and monotonically increasing attempt tokens. Renewals and transitions are
  conditional updates fenced by owner, token, state, and an unexpired
  database-time lease.
- `ArchiveBackfillStages` executes the Task 3 verified archive pipeline from an
  approved structured plan: download and official checksum validation, raw ZIP
  retention, restart-safe descriptor reopening, deterministic normalization,
  immutable checksum-versioned Parquet publication, six validation checks, and
  manifest/catalog approval. Deterministic planner and old-404 gap wiring are
  included.
- Catalog approval requires checksum, schema, ordering, uniqueness, range, and
  row-count success. Same-source retries are idempotent; changed official bytes
  create a new immutable version. DuckDB accepts no caller path, opens only
  catalog paths beneath the secure data root, rechecks file SHA-256, and reads
  only the latest approved version for each exact coverage partition.
- The migration persists full manifests and binds manifest, partition, source
  object, URL, and checksums with composite foreign keys and lowercase SHA-256
  checks. Backfill catalog evidence uses a composite manifest/partition
  relation.
- Gaps cover missing minute opens, aggregate-trade ID discontinuities,
  source-unknown disconnect windows, missing official archives, and partition
  coverage. A repair closes only from approved evidence for the same symbol,
  data type, and complete half-open range; aggregate-trade gaps additionally
  require the recorded missing ID range.
- Profiles are computed only through the approved catalog boundary. Every
  metric includes its own sample count and coverage fraction, and BTC/PEPE
  fixtures prove independent volatility, jump, spread, funding, and liquidity
  results.
- Eligibility evaluates every reason independently, including
  `metadata_unverified`, insufficient history/coverage/liquidity, stale live
  data, unrepaired gaps, data-not-ready, and required-source degradation.
- Public REST classifies 451, timeout, DNS, TLS, connect, 429, server, HTTP, and
  response-validation failures. Endpoint type determines metadata versus repair
  capability, so callers cannot mislabel health; archive and live health remain
  isolated.
- `DataManifest` now binds archive coverage to its exact daily/monthly source
  period and REST coverage to exact millisecond request bounds. Epoch conversion
  is integer-based and sub-millisecond boundaries fail closed.

## TDD evidence

### RED

The initial focused tests failed during collection because the six Task 4
market modules did not exist. Subsequent adversarial RED runs included:

```text
.venv/bin/pytest -q tests/market/test_backfill.py -k production_archive_stages
ImportError: cannot import name 'ArchiveBackfillStages'

.venv/bin/pytest -q tests/market/test_gaps.py -k aggregate_trade_gap_requires
TypeError: ApprovedCoverage.__init__() got an unexpected keyword argument
'recovered_id_start'

.venv/bin/pytest -q tests/db/test_models.py -k backfill_objects
assert any("source_checksum ~" in str(item.sqltext) for item in checks)
AssertionError
```

Other RED cases covered stale worker fencing with a reused worker ID,
latest-version-only reads, wrong-symbol/data-type/unapproved gap evidence,
endpoint capability mislabeling, source-period/REST-boundary mismatches,
sub-millisecond catalog queries, and composite evidence constraints.

### GREEN

Each failure received the smallest corresponding implementation before the
next behavior. Final focused Task 4 verification:

```text
cd services/api
.venv/bin/pytest -q tests/market
```

Result: `114 passed`.

## Review hardening

The first independent review identified lease transaction/fencing, latest
catalog version, typed repair evidence, capability authority, concrete pipeline
wiring, relational evidence, source-bound manifest coverage, and float epoch
risks. The implementation now commits every durable stage; uses atomic
database-time lease conditions and attempt tokens; selects only latest approved
versions; requires typed and ID-aware repair evidence; derives REST capability
from endpoint; provides production Task 3 stages/planner/sink; enforces
composite relations and SHA-256 formats; binds coverage to provenance; and uses
integer millisecond arithmetic.

The second review then found five integration holes. Replanning now excludes a
discovered checksum from immutable plan identity. Durable gap reconciliation
accepts only partition IDs and derives locked approval, identity, coverage,
validator, and contiguous aggregate-ID evidence from PostgreSQL. Concrete
archive validation persists minute and aggregate-ID gaps before catalog
approval. Long external stages, including catalog publication, run under a
bounded lease heartbeat with cancellation cleanup. SQL heartbeats require an
independent repository/session, so renewal commits cannot commit the main
catalog transaction or release its advisory lock; missing SQL heartbeat
injection fails fast. Blocking archive/Parquet work runs in worker threads so
renewals can execute. Finally, differing overlapping active ranges
are rejected; PostgreSQL serializes approvals per symbol/data type with a
transaction advisory lock so monthly and daily coverage cannot race into the
active catalog.

## Verification

```text
cd services/api
.venv/bin/pytest -q
# 343 passed, 1 skipped

.venv/bin/ruff check src tests
# All checks passed!

.venv/bin/python scripts/export_schemas.py --check
.venv/bin/alembic -c alembic.ini heads
# 20260721_0002 (head)

CRYPTO_DATABASE_URL=postgresql+asyncpg://user:pass@localhost/db \
  .venv/bin/alembic -c alembic.ini upgrade head --sql
# generated 210 lines of PostgreSQL migration SQL

source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:test-generation
npm run contracts:check-types
npm run contracts:types

git diff --check
```

All listed checks completed successfully. Generated `DataManifest` JSON Schema
and TypeScript declarations include the public `exchange_info` endpoint.

## Remaining environment note

`tests/db/test_postgres_integration.py` is intentionally opt-in and was the one
skipped test because `CRYPTO_TEST_DATABASE_URL` was not set. Alembic's
PostgreSQL offline compilation passed, and the integration test now explicitly
expires a lease using database time before proving safe restart takeover. No
live exchange, mutable production data, credentials, trading, or public port
was used.

The final independent review approved the implementation with no remaining
Critical or Important findings. Its targeted publish-heartbeat verification
passed; the real PostgreSQL dual-session composition remains in the opt-in
integration suite described above.

## PostgreSQL gate follow-up

The first real PostgreSQL run exposed that scalar FK IDs alone did not give the
SQLAlchemy unit of work an ORM relationship topology. A single flush containing
new source, partition, and manifest rows could therefore emit the manifest
INSERT before its partition and fail `fk_data_manifests_partition_source`.

A focused RED fake-session regression reproduced the actual constraint order
as `manifest FK observed before partition INSERT`. Catalog approval now flushes
new source, then partition, then manifest explicitly, while keeping all three
flushes inside the same uncommitted transaction. The same regression also
exposed strict Pydantic reloading of a JSON-mode dump; persisted manifests now
exclude computed `resolved_url` and are rehydrated through JSON validation.

Focused verification passed with `21 passed, 1 skipped`; final API verification
passed with `341 passed, 1 skipped`. The skipped case remains the opt-in real
PostgreSQL suite when `CRYPTO_TEST_DATABASE_URL` is absent.

The second real PostgreSQL gate exposed cumulative lease age across several
short stages: because each operation completed before the periodic heartbeat
interval, none renewed, while intervening durable commits consumed the original
claim lease. Every external stage now renews and commits through the independent
heartbeat repository before its side-effect coroutine is created, then retains
periodic renewal while running. The awaitable factory means a rejected
pre-stage renewal creates no unawaited coroutine. Regressions cover cumulative
short stages, pre-stage failure without operation creation, and periodic
failure cancellation; the opt-in PostgreSQL case now includes multiple short
stages below the heartbeat interval. Final verification passed with
`343 passed, 1 skipped`.
