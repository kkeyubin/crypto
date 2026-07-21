# Task 8 Report: Phase 1 Deployment and Operations Preparation

## Status

Phase 1 implementation and bounded server acceptance are complete. The accepted deployment is loopback-only and records upstream/network incompleteness instead of treating it as tradable data. Phase 2 remains blocked until the recorded gaps and `metadata_unverified` state are repaired or resolved.

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

A subsequent runtime gate completed a `VERIFIED` backup, then exposed a sanitized ownership failure. Host data `/home/keyubin/crypto-research-phase0-smoke/data` was mode `0750`, owner `1000:1000`, while the API/worker image process was root. The worker failed `_open_secure_root` with `ValueError: data root owner does not match the effective user`. This intermediate failure led to the non-root runtime fix and was superseded by the accepted deployment below.

## Bounded server acceptance

Acceptance ran on `keyubin@192.168.1.4` on 2026-07-22. The verified pre-upgrade backup is `/home/keyubin/crypto-research-backups/phase0-20260721T193438Z`. The accepted application source is API/worker commit `4672152` plus Web protocol-display fix `b7676c2`; all four containers were healthy. PostgreSQL and Web listened only on `127.0.0.1:55432` and `127.0.0.1:8088`; API had no host port and the worker opened no listener.

Seven bounded jobs succeeded and reached `catalog_approved`: BTC funding `0bf1175e-7d97-595f-ab14-e949a726412c`, kline `ecdce6ba-3c85-56ad-85ad-116df2ce3330`, mark price `fec7bfa7-7986-5dbb-b7c3-6a6996105f2f`, aggTrades `394e33e3-289c-59ea-a75d-a7c4df88a14b`; 1000PEPE funding `780c682d-bc5d-5795-8ee0-3842951ef7c2`, kline `9b6c6bb7-9997-5ed6-b12b-a3428bd35a51`, and mark price `9033d888-2dce-56fb-b7fd-b974731fd96c`.

Published archive SHA-256 values were preflighted and then compared with both downloaded ZIPs and normalized Parquet objects; all 14 comparisons passed:

| Dataset | SHA-256 | Rows |
|---|---|---:|
| BTCUSDT June kline | `9b214199eb5063585c7ed0f59ba19323326d68ac024b85106713989399204490` | 43,200 |
| BTCUSDT June mark price | `5d5d8b86efbe709e034409634347a161183e7c0df3854f8d96c9778dc1c923ba` | 41,760 |
| BTCUSDT June funding | `cff97ce688329592bccbbf5873b5c7021649e093f5f5806e332c5b4fb7fd6a00` | 90 |
| BTCUSDT 2026-06-14 aggTrades | `afcb1fbd4240c0d4f03e98e58817f73857da78f9260887431016c30a4513cdea` | 940,863 |
| 1000PEPEUSDT May kline | `b4d7ccd09a324fb5aa9743f819ab3e0f4c922468412f408888b00850bfabf5e0` | 44,640 |
| 1000PEPEUSDT May mark price | `1b0d8cd6acfe522eb37740acdcffed2a79be16ee105b884e7b05b6b0cba1ad06` | 44,640 |
| 1000PEPEUSDT May funding | `a926a86a73eb373e920052bbc0c86103dc659b481f3e080fb512912c82ac0493` | 93 |

The official BTC mark-price archive lacks 2026-06-29 and produced one explicit `missing_minute_open_time` gap; normalization did not discard hidden rows. Profiles remained independent: BTC had 43,199 return samples and realized volatility about `0.15369`; 1000PEPE had 44,639 samples and realized volatility about `0.19383`.

Live acceptance observed canonical `aggTrade`, `bookTicker`, `kline_1m`, and `markPrice@1s` events for both symbols. Direct connection attempts timed out and the scoped proxy succeeded; the API reported `source_mode=proxy`, archive healthy, live healthy at the acceptance snapshot, and REST unhealthy. Browser verification through an SSH tunnel showed Chinese by default, separate BTC/1000PEPE evidence, and `4/4` required streams on both cards. A browser-found old-lowercase comparison defect was reproduced by a failing test and fixed in `b7676c2`.

Restart checks preserved 7 jobs, 7 approved source objects, 7 manifests, and 7 archive partitions. Live shards continued increasing, while duplicate live IDs remained zero. The 2026-07-21T20:36Z snapshot contained 10,636 live partitions and explicit open gaps: each symbol had 26 `source_unknown_disconnect` and 13 `worker_restart` gaps; BTC also had the one upstream missing-minute gap. Proxy reconnects can transiently mark `bookTicker` disconnected, so eligibility correctly remains false with `unrepaired_gap`, `metadata_unverified`, and `data_not_ready`. These limitations are visible and are a hard gate before Phase 2, not an acceptance failure hidden by the UI.

## Local verification

Executed on 2026-07-22 in `/Users/kyle/Documents/crypto/.worktrees/phase-1-binance-data`:

- focused Task 8 repository/config/entrypoint/healthcheck tests — passed;
- `cd services/api && .venv/bin/pytest -q` — `620 passed, 1 skipped` after real-archive and manifest JSON-boundary remediation;
- `.venv/bin/ruff check src tests migrations ../../deploy/api-entrypoint.py ../../deploy/market-worker-healthcheck.py` — passed after the final hardening;
- `.venv/bin/python scripts/export_schemas.py --check` — passed;
- `source /Users/kyle/.nvm/nvm.sh && nvm use` — Node `v24.15.0`, npm `11.12.1`;
- `npm ci` — passed, 180 packages audited, 0 vulnerabilities;
- contract generation/check/type drift gates — passed;
- `npm run web:test -- --run` — 4 files, 60 tests passed, including canonical routed stream casing;
- `npm run web:build` — passed, 56 modules transformed;
- `git diff --check` — passed.

Re-review focused verification:

- canonical alias/control and operations documentation — 29 tests passed;
- SymbolsPage/useSymbols canonical runtime fixtures — 46 tests passed;
- runtime-user/deployment documentation policy — 14 tests passed.

Docker Compose validation and API/Web builds passed on the Docker-capable server. A direct npm image rebuild later stalled at registry access; the one retry used the documented loopback proxy only for the build and did not add runtime proxy variables.

## Read-only server baseline

At `2026-07-22T02:03+08:00`, the existing `/home/keyubin/crypto-research-phase0-smoke` stack was inventoried without mutation. PostgreSQL/API/Web were healthy with `unless-stopped`; Web alone was bound to `127.0.0.1:8088`; API and PostgreSQL had no host port. The stack used `crypto-research_default`, volume `crypto-research_postgres-data`, and the API bind `/home/keyubin/crypto-research-phase0-smoke/data` → `/srv/crypto-research/data`. Data size was `4.0K`, database size `7518kB`, no `alembic_version` table existed, free space was `688G`, and `/home/keyubin/crypto-research-phase0-smoke/config/runtime.env` was mode `0600` with only three key names recorded. The copied source identifies SHA `0226feba8bbefec207c7eee5b40c93c68a22e922`. Existing containers/data/configuration were not changed.

## Remaining operational gate before Phase 2

Phase 1 may be merged, but its output is intentionally not eligible for research replay. Repair or resolve all accepted upstream/restart/disconnect gaps, obtain verifiable instrument metadata, and demonstrate a sustained observation window without unexplained reconnect churn before any Phase 2 backtest consumes these partitions.
