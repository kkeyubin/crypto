# Task 8 Report: Phase 1 Deployment and Operations Preparation

## Status

Local deployment wiring, CI gates, repository policy tests, and operator documentation are implemented. Phase 1 remains **in progress** because this Mac has no Docker CLI and the bounded server acceptance has not run. This report does not claim container builds or remote acceptance passed.

## TDD evidence

Repository/deployment tests were written and observed failing before implementation. The initial RED run had 11 failures: no `market-worker`, no server profile/host network, no loopback PostgreSQL host port, no worker database/proxy configuration, no worker healthcheck, fixed API-only entrypoint behavior, no Phase 1 CI gates, and no Binance operations runbook. Additional RED tests proved that unsafe proxy URL shapes were accepted and that API/worker database endpoint and migration mode could be mismatched.

The resulting controls are:

- server-profile `market-worker` with host networking, shared data root, graceful stop, restart policy, and PostgreSQL/API health dependencies;
- PostgreSQL only at `127.0.0.1:55432`, Web only at `127.0.0.1:8088`, and no API host port;
- API `postgres:5432 + migrations=true` and worker `127.0.0.1:55432 + migrations=false` enforced as exact pairs;
- raw database password removed from the exec'd API/worker process, with no password in command arguments or error output;
- worker health based on its exact database endpoint and a `running`/`degraded` heartbeat no older than 120 seconds;
- direct-first scoped proxy configuration at `http://127.0.0.1:17891`, with no generic proxy variable or exchange credential;
- CI Alembic-head/import checks, server-profile Compose validation, real migration invocation, image builds, and `always()` cleanup;
- documented bounded onboarding, official `.CHECKSUM` verification, `source_pending`, source replacement, disconnect gaps, metadata degradation, restart/idempotency, backup/restore/rollback, SSH tunnel, and exposure checks.

## Independent review remediation

The post-implementation review found that the original acceptance examples assumed a nonexistent daily `fundingRate` archive and used `PEPEUSDT`, which is not the Binance USDⓈ-M venue contract. Direct official `.CHECKSUM` probes on 2026-07-22 established the replacement evidence:

- BTCUSDT June 2026 monthly kline, mark-price kline, and funding objects were published;
- BTCUSDT `2026-06-14` daily aggTrades was published;
- 1000PEPEUSDT May and June 2026 monthly kline, mark-price kline, and funding objects were published;
- the tested daily funding object and PEPEUSDT objects returned 404.

The implementation now plans funding from complete monthly archives only and rejects a partial-month funding request before creating any job/source object. Multi-type backfills preplan every dataset before mutation. Initial onboarding and the Chinese/English UI require complete UTC calendar months because funding is always included. `PEPE`, `PEPEUSDT`, and `1000PEPEUSDT` all operate on stored canonical `1000PEPEUSDT` state, and the Web backfill uses the API-returned canonical identity. Intermediate Phase 1 builds that could create noncanonical alias rows were never deployed, so no partial legacy compatibility path is implemented or claimed.

The runbooks now require live `.CHECKSUM` preflight for every candidate before any POST, parameterize checkout/environment/data paths, and separate a fresh installation from an existing smoke upgrade. Downloaded-object verification uses `set -euo pipefail`, requires a nonempty successful catalog query, and aborts on any official download, local hash, or comparison failure. The upgrade gate discovers and stops the full server profile, preserves the existing mode-`0600` `runtime.env` and `POSTGRES_PASSWORD`, dumps PostgreSQL, archives the data root, records sanitized volume/config/source identity, verifies all artifacts and checksums, and creates `VERIFIED` only after validation. Phase 1 recovery now covers data, gaps, audit evidence, and eligibility only; it makes no later-phase operational claims.

A later read-only Compose gate confirmed the exact smoke layout: installation root `/home/keyubin/crypto-research-phase0-smoke`, checkout `/home/keyubin/crypto-research-phase0-smoke/repo`, mode-`0600` environment `/home/keyubin/crypto-research-phase0-smoke/config/runtime.env`, data `/home/keyubin/crypto-research-phase0-smoke/data`, and Compose file `$CRYPTO_CHECKOUT/deploy/compose.yaml`. All smoke upgrade examples keep the installation root distinct from the checkout. The Phase 1 candidate was staged separately.

The backup command attempted during this gate failed before any stop action. All old containers remained healthy, no `VERIFIED` marker was created, and an empty timestamped backup directory may remain. It is not a usable backup and was not removed. The existing stack remained untouched.

## Local verification

Executed on 2026-07-22 in `/Users/kyle/Documents/crypto/.worktrees/phase-1-binance-data`:

- focused Task 8 repository/config/entrypoint/healthcheck tests — passed;
- `cd services/api && .venv/bin/pytest -q` — `615 passed, 1 skipped` after re-review remediation;
- `.venv/bin/ruff check src tests migrations ../../deploy/api-entrypoint.py ../../deploy/market-worker-healthcheck.py` — passed after the final hardening;
- `.venv/bin/python scripts/export_schemas.py --check` — passed;
- `source /Users/kyle/.nvm/nvm.sh && nvm use` — Node `v24.15.0`, npm `11.12.1`;
- `npm ci` — passed, 180 packages audited, 0 vulnerabilities;
- contract generation/check/type drift gates — passed;
- `npm run web:test -- --run` — 4 files, 60 tests passed;
- `npm run web:build` — passed, 56 modules transformed;
- `git diff --check` — passed.

Re-review focused verification:

- canonical alias/control and operations documentation — 29 tests passed;
- SymbolsPage/useSymbols canonical runtime fixtures — 46 tests passed.

Not executed locally:

- `docker compose ... config --quiet` — blocked because `docker` is not installed on this Mac (`zsh: command not found: docker`);
- API/Web image builds — same missing Docker CLI;
- server deployment/acceptance — deliberately deferred until this local implementation is independently reviewed.

## Read-only server baseline

At `2026-07-22T02:03+08:00`, the existing `/home/keyubin/crypto-research-phase0-smoke` stack was inventoried without mutation. PostgreSQL/API/Web were healthy with `unless-stopped`; Web alone was bound to `127.0.0.1:8088`; API and PostgreSQL had no host port. The stack used `crypto-research_default`, volume `crypto-research_postgres-data`, and the API bind `/home/keyubin/crypto-research-phase0-smoke/data` → `/srv/crypto-research/data`. Data size was `4.0K`, database size `7518kB`, no `alembic_version` table existed, free space was `688G`, and `/home/keyubin/crypto-research-phase0-smoke/config/runtime.env` was mode `0600` with only three key names recorded. The copied source identifies SHA `0226feba8bbefec207c7eee5b40c93c68a22e922`. Existing containers/data/configuration were not changed.

## Pending completion gates

Run Compose config and both image builds in a Docker-capable environment. Then complete the verified backup gate and run bounded BTCUSDT/1000PEPEUSDT server acceptance with the preflighted complete-month objects: archive checksums/rows/manifests, one BTC aggTrades day, all four live types, recorded direct failure plus proxy source mode, `metadata_unverified`, restart idempotency, UI tunnel, and loopback-only listeners. Only then update the implementation plan/roadmap and use `docs: complete Phase 1 data foundation`.
