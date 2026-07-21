# Task 1 Report: Persistent Symbol, Job, Catalog, Gap, and Stream State

## Status

DONE

## Implemented work

- Added runtime dependencies for Alembic, DuckDB, Polars, PyArrow, `httpx2`, and WebSockets.
- Added validated Phase 1 network/worker settings with the agreed defaults:
  `archive_base_url=https://data.binance.vision`,
  `rest_base_url=https://fapi.binance.com`,
  `public_ws_base_url=wss://fstream.binance.com/public`,
  `market_ws_base_url=wss://fstream.binance.com/market`,
  `history_max_days=366`, `live_stale_after_seconds=120`,
  `market_worker_id=market-worker`, `proxy_mode=auto`, and no default proxy.
- Production settings reject source-URL userinfo and non-loopback proxy hosts. URL fields require absolute network URLs. `database_url` and `http_proxy_url` are excluded from settings representations.
- Added SQLAlchemy models and immutable domain values for independent symbol state, metadata snapshots, ingestion jobs, source objects, approved partitions, gaps, stream state, worker heartbeats, and audit events.
- Added the `DataStateRepository` protocol and `SqlAlchemyDataStateRepository`, which returns domain values rather than ORM rows. It supports idempotent symbol add/disable, bounded backfill creation, legal job transitions, immutable partition approval, gap repair history, stream upserts, and audit-event creation.
- Added Alembic async migration support and the initial Phase 1 migration. It creates the nine required tables with UTC-aware timestamp columns, state checks, and catalog/source uniqueness constraints.
- Updated the API image to include Alembic files; the entrypoint invokes `alembic upgrade head` before `execvp`, and a migration failure stops container startup.

## Files changed

- `services/api/pyproject.toml`
- `services/api/src/crypto_research/config.py`
- `services/api/src/crypto_research/database.py`
- `services/api/src/crypto_research/db/__init__.py`
- `services/api/src/crypto_research/db/base.py`
- `services/api/src/crypto_research/db/models.py`
- `services/api/src/crypto_research/db/repositories.py`
- `services/api/alembic.ini`
- `services/api/migrations/env.py`
- `services/api/migrations/script.py.mako`
- `services/api/migrations/versions/20260721_0001_phase1_data_state.py`
- `services/api/tests/db/test_models.py`
- `services/api/tests/db/test_repositories.py`
- `services/api/tests/test_config.py`
- `deploy/api-entrypoint.py`
- `deploy/api.Dockerfile`

## TDD evidence

1. Initial database tests were added before persistence production code. After creating the required Python 3.12 virtual environment, this command produced the expected RED collection failure because the modules did not exist:

   ```bash
   cd services/api && .venv/bin/pytest -q tests/db tests/test_config.py
   ```

   Result: `ModuleNotFoundError: No module named 'crypto_research.db'` for both DB test modules.

2. After the first implementation, a focused legal-transition test was added before `transition_job` existed:

   ```bash
   .venv/bin/pytest -q tests/db/test_repositories.py::test_job_transitions_accept_only_legal_next_states
   ```

   Result: RED with `AttributeError: 'DataStateRepository' object has no attribute 'transition_job'`.

3. URL validation was added test-first:

   ```bash
   .venv/bin/pytest -q tests/test_config.py::test_market_data_urls_require_an_absolute_network_url
   ```

   Result: RED, three expected `Failed: DID NOT RAISE ValidationError` failures before the validator was implemented.

4. Async Alembic support was added test-first:

   ```bash
   .venv/bin/pytest -q tests/db/test_models.py::test_alembic_environment_uses_an_async_database_engine
   ```

   Result: RED because `migrations/env.py` used synchronous `engine_from_config`; GREEN after conversion to `async_engine_from_config`.

## Verification

```bash
cd services/api
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q tests/db tests/test_config.py tests/repository/test_api_entrypoint.py
.venv/bin/ruff check src tests migrations
.venv/bin/alembic -c alembic.ini upgrade head --sql > /tmp/phase1-task1-migration.sql
.venv/bin/pytest -q
```

Results:

- Focused task regression: `28 passed`.
- Alembic SQL generation succeeded and emitted `CREATE TABLE` for `symbols`, `source_objects`, `data_partitions`, `stream_states`, and `audit_events`; `alembic heads` reports `20260721_0001 (head)`.
- Full Python suite: `142 passed in 9.17s`.
- Ruff: `All checks passed!`.

## Self-review

- Verified no unrelated Phase 0 contract/test formatting edits remain in the worktree.
- Confirmed all requested state tables are represented in both metadata and migration, with source URL/checksum and partition-version uniqueness plus state check constraints.
- Confirmed the Alembic environment uses SQLAlchemy's async engine factory, matching the `postgresql+asyncpg` runtime URL.
- Confirmed the API Dockerfile copies both Alembic configuration and migrations so the entrypoint migration runs in the image.
- Did not access live Binance, the LAN server, a live PostgreSQL database, or any credentials.

## Concerns

- Repository behavior is exercised with an explicitly injected fake async session, as requested to avoid Docker/testcontainers. PostgreSQL constraint execution is validated through the generated Alembic SQL rather than a live database integration test.
