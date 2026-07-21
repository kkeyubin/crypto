# Phase 1 Binance Data Foundation Implementation Plan

> **Execution method:** Use `superpowers:subagent-driven-development` task by task, with a fresh implementation agent and review gates. Use `superpowers:test-driven-development` for every behavior change and `superpowers:verification-before-completion` before claiming Phase 1 complete.

**Goal:** Deliver a production-deployable Binance USDⓈ-M historical/live data foundation whose checksums, partitions, gaps, freshness, profiles, and fail-closed eligibility are independently inspectable for each symbol.

**Architecture:** Extend the Phase 0 FastAPI modular monolith with PostgreSQL operational state, an archive-first historical pipeline, partitioned Parquet storage, DuckDB catalog queries, and a separately supervised WebSocket market worker. Archive and stream adapters terminate at validated normalized records; only catalog-approved partitions and fresh stream state may be consumed by later phases.

**Scope source:** `docs/superpowers/specs/2026-07-21-phase-1-binance-data-foundation-design.md`

**Technology:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy asyncio, Alembic, asyncpg, Polars, PyArrow, DuckDB, `httpx2`, `websockets`, PostgreSQL 17, React 19, TypeScript 5.9, Vitest, Docker Compose.

## Non-Negotiable Execution Rules

- Use Binance public data only. Add no API-key, order, account, wallet, or live-trading setting.
- Use `Decimal` or decimal-preserving Arrow types for price/quantity/funding values; never normalize through `float`.
- Require an explicit UTC `historyStart` and `historyEnd`; cap one API request at 366 closed days and require separate jobs for larger ranges.
- Treat archive 404 as `source_pending` only for the two most recently closed UTC days; older missing objects fail the job and create a gap.
- Treat REST and metadata as optional, explicitly degraded capabilities in the current network. Never synthesize `exchangeInfo` filters.
- Keep BTCUSDT and canonical 1000PEPEUSDT state, profile, gaps, and eligibility completely separate; accept PEPE/PEPEUSDT only as input aliases.
- Do not implement backtests, strategies, paper orders, signal notifications, Hermes calls, or AI in this plan.
- Each task ends with its focused tests, relevant regression tests, lint, a requirements review, and a quality review before commit.

## Task 1: Persistent Symbol, Job, Catalog, Gap, and Stream State

**Files:**

- Modify: `services/api/pyproject.toml`
- Modify: `services/api/src/crypto_research/config.py`
- Replace: `services/api/src/crypto_research/database.py`
- Create: `services/api/alembic.ini`
- Create: `services/api/migrations/env.py`
- Create: `services/api/migrations/script.py.mako`
- Create: `services/api/migrations/versions/20260721_0001_phase1_data_state.py`
- Create: `services/api/src/crypto_research/db/__init__.py`
- Create: `services/api/src/crypto_research/db/base.py`
- Create: `services/api/src/crypto_research/db/models.py`
- Create: `services/api/src/crypto_research/db/repositories.py`
- Create: `services/api/tests/db/test_models.py`
- Create: `services/api/tests/db/test_repositories.py`
- Modify: `deploy/api-entrypoint.py`

**Step 1: Write failing persistence tests**

Cover normalized uppercase symbol identity, idempotent add/disable, independent BTC/PEPE rows, legal job transitions, immutable approved partition versions, unique source object URL/checksum versions, gap open/repair history, stream heartbeat upsert, and audit-event creation. Assert database URLs and proxy URLs are redacted from model representations.

Run:

```bash
cd services/api
.venv/bin/pytest -q tests/db
```

Expected: fail because the database models and repositories do not exist.

**Step 2: Add dependencies and configuration**

Add runtime dependencies `alembic>=1.14,<2`, `duckdb>=1.4,<2`, `polars>=1.30,<2`, `pyarrow>=19,<24`, `httpx2>=2.7,<3`, and `websockets>=15,<17`. Add validated settings for `archive_base_url`, `rest_base_url`, routed WebSocket bases, `history_max_days=366`, `live_stale_after_seconds`, `market_worker_id`, `proxy_mode`, and optional loopback-only `http_proxy_url`.

Production validation must reject credentials embedded in Binance URLs and reject a configured proxy whose hostname is not loopback.

**Step 3: Implement schema and repository boundaries**

Create tables for `symbols`, `symbol_metadata_snapshots`, `ingestion_jobs`, `source_objects`, `data_partitions`, `data_gaps`, `stream_states`, `worker_heartbeats`, and `audit_events`. Use UTC-aware timestamps, database uniqueness constraints, check constraints for state values, and JSON only for bounded details/reason arrays.

The repository surface must expose transactions rather than ORM rows:

```python
class DataStateRepository(Protocol):
    async def add_symbol(self, command: AddSymbolCommand) -> SymbolState: ...
    async def create_backfill(self, command: BackfillCommand) -> IngestionJob: ...
    async def approve_partition(self, candidate: PartitionCandidate) -> DataPartition: ...
    async def record_gap(self, gap: GapRecord) -> DataGap: ...
    async def update_stream(self, state: StreamState) -> None: ...
```

`deploy/api-entrypoint.py` must run `alembic upgrade head` before starting the API and fail the container if migration fails.

**Step 4: Verify and commit**

```bash
cd services/api
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q tests/db tests/test_config.py tests/repository/test_api_entrypoint.py
.venv/bin/ruff check src tests migrations
```

Commit: `feat: add Phase 1 market data state`

## Task 2: Phase 1 API Contracts and Generated TypeScript

**Files:**

- Modify: `services/api/src/crypto_research/contracts/manifest.py`
- Create: `services/api/src/crypto_research/contracts/data.py`
- Modify: `services/api/src/crypto_research/contracts/__init__.py`
- Modify: `services/api/scripts/export_schemas.py`
- Create: `services/api/tests/contracts/test_data.py`
- Modify: `services/api/tests/contracts/test_runtime_contracts.py`
- Generate: `contracts/jsonschema/*.schema.json`
- Generate: `contracts/types/*.ts`
- Modify: `contracts/types/index.ts`

**Step 1: Write failing contract tests**

Test `AddSymbolRequest`, `BackfillRequest`, `SymbolView`, `IngestionJobView`, `DataPartitionView`, `DataGapView`, `SymbolProfileView`, `EligibilityView`, `StreamStateView`, and `MarketDataHealthView`. Reject lowercase/invalid symbols, non-UTC/future/end-before-start ranges, ranges over 366 days, aggregate-trade history without explicit opt-in, unknown fields, impossible counts, and non-finite metrics.

Extend `DataManifest` with source kind, source object URL, raw/normalized relative paths, source checksum, normalized checksum, row count, validation state, and deterministic primary-key/deduplication metadata. Keep backwards-invalid changes explicit by moving its schema version to `2.0.0`.

**Step 2: Implement strict Pydantic contracts**

Use enums for every externally visible state. Eligibility is a list of machine-readable reason codes plus human-readable Chinese/English presentation in the Web layer; it is never a single inferred Boolean without reasons.

**Step 3: Regenerate schemas and types**

```bash
cd services/api
.venv/bin/pytest -q tests/contracts
.venv/bin/python scripts/export_schemas.py
cd ../..
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:types
npm run contracts:test-generation
npm run contracts:check-types
git diff --check
```

Commit: `feat: define market data contracts`

## Task 3: Official Archive Planning, Download, Verification, and Normalization

**Files:**

- Create: `services/api/src/crypto_research/market/__init__.py`
- Create: `services/api/src/crypto_research/market/binance/__init__.py`
- Create: `services/api/src/crypto_research/market/binance/archive.py`
- Create: `services/api/src/crypto_research/market/binance/archive_paths.py`
- Create: `services/api/src/crypto_research/market/binance/normalization.py`
- Create: `services/api/src/crypto_research/market/storage.py`
- Create: `services/api/src/crypto_research/market/validation.py`
- Create: `services/api/tests/market/fixtures/*.csv`
- Create: `services/api/tests/market/fixtures/*.zip`
- Create: `services/api/tests/market/test_archive_paths.py`
- Create: `services/api/tests/market/test_archive_download.py`
- Create: `services/api/tests/market/test_normalization.py`
- Create: `services/api/tests/market/test_storage.py`

**Step 1: Write failing archive-path and adversarial ZIP tests**

Cover monthly-first/daily-tail planning, month/day boundaries, uppercase symbols, kline/mark-price/funding/aggTrades URLs, sibling `.CHECKSUM` parsing, bad hash, truncated ZIP, multiple members, absolute paths, `..` traversal, decompression size/ratio limits, unexpected headers/column counts, out-of-range rows, unsorted rows, duplicate IDs/timestamps, and source decimal preservation.

Network tests must use injected transports and local fixtures; CI must not depend on Binance.

**Step 2: Implement safe download and atomic storage**

The adapter API is source-agnostic:

```python
@dataclass(frozen=True)
class ArchiveObject:
    dataset: DatasetKind
    symbol: str
    start: datetime
    end: datetime
    url: str
    checksum_url: str

async def fetch_archive(obj: ArchiveObject, target: Path, client: HttpClient) -> VerifiedArchive: ...
```

Stream the ZIP to a `.partial` file with byte limits, fsync it, verify the official SHA-256, validate the sole member without extracting arbitrary paths, and retain the raw verified object by source checksum. Write normalized Parquet to a temporary file in the destination partition and rename only after validation.

**Step 3: Implement dataset-specific normalizers**

Normalize kline and mark-price kline primary keys by open time, funding by funding time, and aggregate trades by aggregate trade ID. Validate UTC range semantics and kline close-time relationships. Store source timestamps in milliseconds and Parquet decimal columns at adequate scale.

**Step 4: Verify and commit**

```bash
cd services/api
.venv/bin/pytest -q tests/market/test_archive_paths.py tests/market/test_archive_download.py tests/market/test_normalization.py tests/market/test_storage.py
.venv/bin/ruff check src tests
```

Commit: `feat: ingest verified Binance archives`

## Task 4: Backfill Orchestration, Catalog, Gaps, Profiles, and Eligibility

**Files:**

- Create: `services/api/src/crypto_research/market/backfill.py`
- Create: `services/api/src/crypto_research/market/catalog.py`
- Create: `services/api/src/crypto_research/market/gaps.py`
- Create: `services/api/src/crypto_research/market/profile.py`
- Create: `services/api/src/crypto_research/market/eligibility.py`
- Create: `services/api/src/crypto_research/market/rest.py`
- Create: `services/api/tests/market/test_backfill.py`
- Create: `services/api/tests/market/test_catalog.py`
- Create: `services/api/tests/market/test_gaps.py`
- Create: `services/api/tests/market/test_profile.py`
- Create: `services/api/tests/market/test_eligibility.py`
- Create: `services/api/tests/market/test_rest_degradation.py`

**Step 1: Write failing orchestration and fail-closed tests**

Prove idempotent retries, restart recovery from the last durable object, pending-recent versus missing-old archive handling, upstream checksum replacement versioning, partition publication only after all validators, explicit gaps, deterministic DuckDB reads restricted to approved catalog paths, and exact UTC coverage.

Profile fixtures must show BTC and PEPE produce different volatility, jump, spread, funding, and liquidity metrics. Eligibility must remain false for each independent reason and must include `metadata_unverified` while REST `exchangeInfo` is unavailable.

**Step 2: Implement backfill state machine and catalog**

Use this durable sequence for every object:

```text
planned -> downloading -> checksum_verified -> normalized
-> validated -> catalog_approved | source_pending | failed
```

The task runner leases jobs using PostgreSQL row locking and a lease expiry so one restarted worker can resume safely. All relative data paths must resolve beneath `CRYPTO_DATA_ROOT`; DuckDB receives only approved paths queried from PostgreSQL.

**Step 3: Implement gap/profile/eligibility calculations**

Detect missing 1-minute open times, event-ID discontinuities, source-unknown disconnect windows, and partition coverage gaps. Compute profiles only from approved partitions and emit sample counts/coverage with each metric. Centralize freshness and eligibility policy; API/UI must not reimplement it.

The REST adapter classifies 451, timeout, DNS/TLS/connect, 429, and server errors. It may repair or fetch metadata only after successful validation; failure records capability health and never blocks archive/live ingestion globally.

**Step 4: Verify and commit**

```bash
cd services/api
.venv/bin/pytest -q tests/market
.venv/bin/ruff check src tests
```

Commit: `feat: orchestrate verified market backfills`

## Task 5: Routed WebSocket Capture and Market Worker

**Files:**

- Create: `services/api/src/crypto_research/market/binance/streams.py`
- Create: `services/api/src/crypto_research/market/live_storage.py`
- Create: `services/api/src/crypto_research/market/worker.py`
- Create: `services/api/src/crypto_research/market/__main__.py`
- Create: `services/api/tests/market/test_stream_routes.py`
- Create: `services/api/tests/market/test_stream_messages.py`
- Create: `services/api/tests/market/test_stream_reconnect.py`
- Create: `services/api/tests/market/test_live_storage.py`
- Create: `services/api/tests/market/test_worker.py`

**Step 1: Write failing deterministic stream tests**

Use a fake WebSocket/session clock to cover `/public` and `/market` route selection, lowercase stream names, combined-event unwrapping, kline completed/in-progress semantics, mark price/funding, aggregate trades, `bookTicker`, malformed/unknown messages, ping/pong, planned pre-24-hour rotation, direct failure then proxy fallback, no proxy use when direct works, exponential backoff+jitter bounds, graceful shutdown, subscription refresh, disconnect gaps, stale state, and idempotent replay.

**Step 2: Implement transport and parsers**

Separate pure parsers from connection management. A parsed event includes venue, market, symbol, dataset, source event time, receive time, source ID where available, and exact decimal strings. Raw newline-delimited event batches are atomically rolled into compressed Parquet partitions; normalized live partitions use the same schemas as archives.

**Step 3: Implement worker supervision**

The worker periodically reloads active symbols, runs archive/backfill leases, maintains routed stream groups beneath Binance's 1024-stream limit, updates heartbeats/stream states, and closes resources on SIGTERM. `proxy_mode=auto` logs a classified direct failure before trying the configured loopback proxy and periodically probes direct recovery.

**Step 4: Verify and commit**

```bash
cd services/api
.venv/bin/pytest -q tests/market
.venv/bin/ruff check src tests
```

Commit: `feat: capture routed Binance live data`

## Task 6: Symbol and Data API

**Files:**

- Create: `services/api/src/crypto_research/routes/__init__.py`
- Create: `services/api/src/crypto_research/routes/symbols.py`
- Create: `services/api/src/crypto_research/routes/data.py`
- Create: `services/api/src/crypto_research/routes/operations.py`
- Modify: `services/api/src/crypto_research/api.py`
- Create: `services/api/tests/routes/test_symbols.py`
- Create: `services/api/tests/routes/test_data.py`
- Create: `services/api/tests/routes/test_operations.py`
- Modify: `services/api/tests/test_api.py`

**Step 1: Write failing API tests with repository/service fakes**

Cover:

- `GET/POST /api/symbols`, `GET/DELETE /api/symbols/{symbol}`;
- `POST /api/symbols/{symbol}/backfills` and `GET /api/backfills/{job_id}`;
- `GET /api/symbols/{symbol}/partitions`, `/gaps`, `/profile`, `/eligibility`, `/streams`;
- `GET /api/operations/market-data`.

Assert idempotency, 409 conflicts, 404s, UTC/range validation, aggregate-trade opt-in, stable ordering/pagination, independent BTC/PEPE responses, audit events, redaction, and 503 behavior when persistent state is unavailable.

**Step 2: Implement dependency-injected routes**

Keep HTTP mapping thin; domain services own rules and repositories own transactions. `DELETE` disables only. No endpoint deletes files or history. OpenAPI must contain only public market-data controls and no trading/credential operation.

**Step 3: Verify and commit**

```bash
cd services/api
.venv/bin/pytest -q tests/routes tests/test_api.py
.venv/bin/ruff check src tests
.venv/bin/python scripts/export_schemas.py --check
```

Commit: `feat: expose market data control API`

## Task 7: Chinese-First Symbol and Data Console

**Files:**

- Modify: `apps/web/src/App.tsx`
- Modify: `apps/web/src/App.test.tsx`
- Modify: `apps/web/src/i18n.ts`
- Modify: `apps/web/src/styles.css`
- Create: `apps/web/src/apiClient.ts`
- Create: `apps/web/src/useSymbols.ts`
- Create: `apps/web/src/SymbolsPage.tsx`
- Create: `apps/web/src/SymbolsPage.test.tsx`
- Create: `apps/web/src/components/DataStatus.tsx`
- Create: `apps/web/src/components/AddSymbolForm.tsx`
- Create: `apps/web/src/components/SymbolCard.tsx`

**Step 1: Write failing interaction and accessibility tests**

Cover empty/loading/error/retry states, BTC/PEPE cards, add form with explicit UTC range and aggTrades warning, disable confirmation, backfill progress, coverage, source mode, last-event freshness, gaps, profile sample counts, eligibility reason list, Chinese default, English switch, keyboard focus, accessible status text, small viewport layout, and reduced motion.

Mock API responses only; do not couple component tests to a running backend.

**Step 2: Implement the Phase 1 surface**

Enable the Symbols navigation item and keep later-phase items disabled. Use a full-width responsive grid with a clear page title/filter/action row; avoid the previously rejected sparse, tiny centered dashboard layout. Present `proxy` as a source-path fact, not as an error when data is fresh. Present REST/metadata degradation separately from archive/live freshness.

**Step 3: Verify and commit**

```bash
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:check-types
npm run web:test -- --run
npm run web:build
```

Commit: `feat: add Phase 1 data console`

## Task 8: Deployment, Recovery Runbook, CI, and Remote Acceptance

**Files:**

- Modify: `deploy/compose.yaml`
- Modify: `deploy/api.Dockerfile`
- Modify: `deploy/crypto-research.service`
- Modify: `.env.example`
- Modify: `.github/workflows/ci.yml`
- Modify: `AGENTS.md`
- Modify: `README.md`
- Modify: `docs/architecture/system-overview.md`
- Modify: `docs/runbooks/market-data-recovery.md`
- Create: `docs/runbooks/binance-data-operations.md`
- Create: `services/api/tests/repository/test_phase1_operations.py`
- Modify: `docs/roadmap.md`

**Step 1: Write failing repository/deployment tests**

Assert a supervised `market-worker`, shared data volume, loopback-only PostgreSQL host port for the host-network worker, loopback-only Web binding, migration command, health checks, validated proxy settings, no secrets/API keys, and documented backup/recovery/acceptance commands. Extend CI with migration/import checks and container configuration validation.

**Step 2: Implement safe deployment wiring**

Run `market-worker` with `network_mode: host` only on the server profile. Bind PostgreSQL to `127.0.0.1:55432:5432` for that worker while retaining the private Compose connection for API. Mount `/srv/crypto-research/data` into API and worker. Configure `CRYPTO_HTTP_PROXY_URL=http://127.0.0.1:17891`, `CRYPTO_PROXY_MODE=auto`, and no generic process-wide proxy environment variable.

Document archive recheck, pending-day behavior, source replacement, disconnect gap triage, manual bounded backfill, metadata degradation, worker restart, Parquet/catalog backup order, and rollback.

**Step 3: Run the complete local verification matrix**

```bash
cd services/api
.venv/bin/pytest -q
.venv/bin/ruff check src tests migrations ../../deploy/api-entrypoint.py
.venv/bin/python scripts/export_schemas.py --check
cd ../..
source /Users/kyle/.nvm/nvm.sh && nvm use
npm ci
npm run contracts:test-generation
npm run contracts:check-types
npm run contracts:types
git diff --exit-code contracts
npm run web:test -- --run
npm run web:build
POSTGRES_PASSWORD=test-password CRYPTO_SESSION_SECRET=test-secret-that-is-at-least-32-characters docker compose -f deploy/compose.yaml config --quiet
docker build --no-cache -f deploy/api.Dockerfile .
docker build --no-cache -f deploy/web.Dockerfile .
git diff --check
```

**Step 4: Run bounded server acceptance**

Deploy to the existing loopback-only smoke location on `keyubin@192.168.1.4`. Preserve its current runtime configuration and data before changing it. Then:

1. migrate PostgreSQL and start API/Web/market worker;
2. preflight every official `.CHECKSUM`, then add BTCUSDT and 1000PEPEUSDT with separate complete UTC month ranges;
3. backfill kline, mark-price kline, monthly-only funding, and one separately bounded opt-in aggTrades day;
4. compare downloaded ZIP SHA-256 values with official `.CHECKSUM` files;
5. verify approved Parquet manifests, exact row ranges, no hidden gaps, and independent profiles;
6. observe at least one live message of each required type and confirm source mode reports proxy after a recorded direct failure;
7. verify metadata/REST capability is degraded with `metadata_unverified` eligibility while archive/live stay healthy;
8. restart market worker and API, prove job/partition/live idempotency, and verify the UI through the existing SSH tunnel;
9. confirm `ss -lntp` exposes no non-loopback application/database port.

Record sanitized commands, timestamps, row counts, manifest IDs, checksums, source modes, restart results, and remaining limitations in `docs/superpowers/plans/2026-07-21-phase-1-binance-data-foundation.md`. Never commit proxy logs, server secrets, chat IDs, or large data.

**Step 5: Complete roadmap and commit**

Change Phase 1 to `complete` only after every acceptance item passes. Otherwise leave it `in progress` and list the exact failed gate.

Commit: `docs: complete Phase 1 data foundation`

## Phase 1 Completion Gate

Before merge, invoke `superpowers:requesting-code-review` over the complete Phase 1 diff, resolve findings with tests, then invoke `superpowers:verification-before-completion` and rerun the full matrix from a clean checkout. Merge and push only when:

- every focused and full test passes;
- generated schemas/types have no drift;
- container configuration and builds pass;
- server archive and routed WebSocket acceptance passes for BTCUSDT and 1000PEPEUSDT;
- restart recovery and loopback-only exposure pass;
- REST/metadata degradation is visible and fail-closed;
- no Phase 2+ behavior, credentials, source books, or market data are committed.
