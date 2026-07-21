# Phase 1 Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the final Phase 1 review gaps without weakening immutable provenance, fail-closed eligibility, or the Phase 2 stability gate.

**Architecture:** Keep archive validation authoritative: detected holes become immutable manifest evidence and approved coverage excludes them. Add explicit bounded operator actions for retry/recheck/repair, with all external sources expressed as structured discriminated unions and all state transitions audited. Expand the profile contract to carry evidence per metric and render it independently per symbol.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy/Alembic, PostgreSQL, PyArrow/DuckDB, React 19, TypeScript, Vitest, Docker Compose.

## Global Constraints

- No Phase 2 backtest, simulated trading, notification, AI, account API, exchange credential, or public port.
- Use direct access first and only the validated loopback proxy after classified restriction.
- Preserve old raw objects, normalized partitions, manifests, repair history, and audit history; never mutate prior evidence into a replacement.
- Every behavior change follows RED → GREEN → focused regression → full verification.
- BTCUSDT and canonical 1000PEPEUSDT evidence remain independent.

---

### Task 1: Manifest Missing Intervals and Safe Repair Coverage

**Files:**
- Modify: `services/api/src/crypto_research/market/backfill.py`
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Test: `services/api/tests/market/test_backfill_stages.py`
- Test: `services/api/tests/db/test_postgres_integration.py`

**Interfaces:**
- Produces: `ArchiveBackfillStages.validate()` evidence containing serialized detected gaps; `DataManifest.missing_intervals` populated from that evidence; `SqlAlchemyApprovedCoverageResolver.resolve()` returning only continuous subranges outside manifest holes.
- Consumes: existing `DetectedGap`, `MissingInterval`, `DataManifest`, `ApprovedCoverage`, and catalog approval APIs.

- [x] **Step 1: Write failing archive-manifest tests**

Create a minute archive fixture with one interior day/minute missing. Assert validation records the gap and publish emits `missing_intervals=(MissingInterval(start=..., end=..., reason="missing_minute_open_time"),)` while complete archives emit `()`.

- [x] **Step 2: Run the focused tests and observe RED**

Run: `cd services/api && .venv/bin/pytest -q tests/market/test_backfill_stages.py -k 'missing_interval or content_gap'`

Expected: the approved manifest currently has an empty `missing_intervals` tuple.

- [x] **Step 3: Persist immutable missing-interval evidence**

Return detected gap boundaries from `validate()` as JSON-safe evidence, reconstruct only validated `MissingInterval` values in `publish()`, and pass them to `DataManifest`. Reject malformed, overlapping, or out-of-range evidence through the existing Pydantic contract.

- [x] **Step 4: Write the PostgreSQL fail-open regression**

Store an approved manifest covering `[00:00, 03:00)` with `missing_intervals=[01:00,02:00)`. Assert the resolver emits `[00:00,01:00)` plus `[02:00,03:00)`, and `reconcile_gap()` leaves the interior gap open with a `partial` history entry.

- [x] **Step 5: Implement coverage subtraction and run GREEN**

Split the manifest outer range by every declared missing interval before constructing `ApprovedCoverage`. Run:

```bash
cd services/api
.venv/bin/pytest -q tests/market/test_backfill_stages.py tests/db/test_postgres_integration.py -k 'gap or coverage or manifest'
.venv/bin/ruff check src tests
```

### Task 2: Executable Retry, Source Recheck, and Replacement Versioning

**Files:**
- Modify: `services/api/src/crypto_research/contracts/data.py`
- Modify: `services/api/src/crypto_research/market/backfill.py`
- Modify: `services/api/src/crypto_research/market/control.py`
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Modify: `services/api/src/crypto_research/routes/data.py`
- Test: `services/api/tests/routes/test_control.py`
- Test: `services/api/tests/db/test_postgres_integration.py`
- Test: `services/api/tests/market/test_worker.py`

**Interfaces:**
- Produces: explicit `source_pending` API state; bounded idempotent `POST /api/backfills/{job_id}/retry`; bounded `POST /api/backfills/{job_id}/recheck` which retains version N when unchanged and creates an immutable N+1 attempt only after a changed official checksum is verified.
- Consumes: deterministic job/source identities, `retry_backfill_object()`, archive `.CHECKSUM` verification, catalog versioning, and audit APIs.

- [x] **Step 1: Write RED lifecycle/API tests**

Assert pending jobs return `source_pending`, failed/pending jobs can be retried without a duplicate active attempt, approved jobs reject ordinary retry, unchanged recheck is idempotent, and changed-checksum recheck produces a new object/manifest/partition version while version 1 remains queryable as history.

- [x] **Step 2: Expose exact source state**

Add `source_pending` to the generated job-status enum or add a bounded per-object state collection to `IngestionJobView`; do not map it to `queued`. Regenerate TypeScript and add Chinese/English copy.

- [x] **Step 3: Wire retry and recheck commands**

Add control-service methods and typed routes that lock the job, validate its symbol/range, audit the operator action, and call repository retry/recheck primitives. Recheck must obtain the sibling official checksum and compare it with the approved object before any mutation.

- [x] **Step 4: Implement immutable N+1 replacement**

When the same approved URL has a different verified checksum, derive a new attempt/object identity including the checksum, run the normal download/normalize/validate/publish pipeline, and rely on catalog versioning for partition N+1. Never reset or overwrite the old approved object.

- [x] **Step 5: Run focused GREEN**

```bash
cd services/api
.venv/bin/pytest -q tests/routes/test_control.py tests/db/test_postgres_integration.py tests/market/test_worker.py -k 'pending or retry or recheck or replacement'
.venv/bin/ruff check src tests
```

### Task 3: Executable Catalog Reconciliation and Honest REST Boundary

**Files:**
- Modify: `services/api/src/crypto_research/contracts/data.py`
- Modify: `services/api/src/crypto_research/market/control.py`
- Modify: `services/api/src/crypto_research/db/repositories.py`
- Modify: `services/api/src/crypto_research/routes/data.py`
- Modify: `docs/roadmap.md`
- Modify: `docs/runbooks/binance-data-operations.md`
- Test: `services/api/tests/routes/test_data.py`
- Test: `services/api/tests/db/test_postgres_integration.py`

**Interfaces:**
- Produces: bounded `POST /api/gaps/{gap_id}/reconcile` that only closes a gap after catalog-approved partition evidence excludes all missing intervals; documentation that the unreachable REST adapter is inactive and `rest_healthy=false` is intentional.
- Consumes: Task 1 safe coverage resolver, immutable Task 2 replacement partitions, and the existing gap repair history.

- [x] **Step 1: Write RED reconciliation route tests**

Submit one existing gap ID with a bounded tuple of approved partition UUIDs. Prove matching complete evidence repairs it, a manifest hole records `partial` and leaves it open, foreign-symbol/data-type evidence cannot repair it, unknown gaps/partitions fail, and arbitrary URLs/ranges are not accepted by the contract.

- [x] **Step 2: Add the typed bounded command**

Add `GapReconcileRequest(partition_ids: tuple[UUID, ...])` with 1–100 unique IDs and no extra fields. Add the control method and route; lock/read the existing gap, call `reconcile_gap()` with source `catalog_reconcile`, commit its immutable history, and return `DataGapView`.

- [x] **Step 3: Keep the REST capability fail-closed**

Do not instantiate repeated REST probes while both measured server paths return timeout/451. Retain the validated `PublicBinanceRestAdapter` as an inactive adapter boundary, keep `rest_healthy=false` and `metadata_unverified`, and document that activation requires a reachable public endpoint plus a new acceptance gate.

- [x] **Step 4: Correct roadmap and runbook scope**

Replace the inaccurate “REST repair complete” claim with “catalog-approved repair/reconcile; REST adapter inactive while endpoint is unreachable.” Explain that Task 2 retry/recheck produces repair evidence and this route evaluates it; neither operation invents data or closes a manifest hole.

- [x] **Step 5: Run focused GREEN**

```bash
cd services/api
.venv/bin/pytest -q tests/routes/test_data.py tests/db/test_postgres_integration.py -k 'reconcile or repair or gap'
.venv/bin/ruff check src tests
```

### Task 4: Per-Metric Profile Evidence and Chinese-First Console

**Files:**
- Modify: `services/api/src/crypto_research/contracts/data.py`
- Modify: `services/api/src/crypto_research/market/control.py`
- Modify: `apps/web/src/contracts.ts`
- Modify: `apps/web/src/apiClient.ts`
- Modify: `apps/web/src/components/SymbolCard.tsx`
- Modify: `apps/web/src/i18n.ts`
- Test: `services/api/tests/contracts/test_data.py`
- Test: `services/api/tests/routes/test_data.py`
- Test: `apps/web/src/SymbolsPage.test.tsx`

**Interfaces:**
- Produces: `ProfileMetricView(value, sample_count, coverage_fraction)` for realized volatility, jump frequency, median spread bps, median hourly volume, and funding-rate mean; terminal `metadata_status` derived from profile completion plus eligibility.
- Consumes: existing `MetricEstimate` fields from `SymbolProfile` and centralized `EligibilityView`.

- [x] **Step 1: Write RED contract and UI tests**

Assert each metric preserves its own sample count/coverage, `sample_count=0` may carry `value=null`, BTC and 1000PEPE render different values/evidence, insufficient evidence is explicit, and terminal metadata state becomes `eligible` or `ineligible` only after verified metadata/profile completion.

- [x] **Step 2: Implement nested metric contracts**

Replace five scalar fields plus shared evidence with five typed metric objects. Map every `MetricEstimate` independently in `_profile_view()`, regenerate JSON Schema/TypeScript, and make the Web decoder reject impossible combinations.

- [x] **Step 3: Render all profile evidence**

Add a compact profile section to each symbol card with localized labels, values/units, sample counts, and coverage. Do not infer PEPE values from BTC or show a genuine zero as “no data”.

- [x] **Step 4: Fix terminal metadata state**

Keep `metadata_unverified` when no validated snapshot exists; otherwise use `profile_building` until all required profile estimates exist, then derive `eligible`/`ineligible` from the central eligibility decision.

- [x] **Step 5: Run focused GREEN**

```bash
cd services/api
.venv/bin/pytest -q tests/contracts/test_data.py tests/routes/test_data.py
cd ../..
source /Users/kyle/.nvm/nvm.sh && nvm use
npm run contracts:test-generation
npm run contracts:check-types
npm run web:test -- --run src/SymbolsPage.test.tsx
npm run web:build
```

### Task 5: Review, Server Migration, and Re-Acceptance

**Files:**
- Modify: `.superpowers/sdd/task-8-report.md`
- Modify: `docs/roadmap.md`
- Modify: `docs/runbooks/binance-data-operations.md`
- Modify: `docs/superpowers/plans/2026-07-21-phase-1-binance-data-foundation.md`

**Interfaces:**
- Consumes: Tasks 1–4 and the existing verified backup/runtime installation.
- Produces: reviewed merge decision and sanitized acceptance evidence.

- [ ] **Step 1: Run the full clean matrix**

Run the API, Ruff, schema, generated contracts, Web tests/build, Compose config, and both image builds recorded in the parent Phase 1 plan.

- [ ] **Step 2: Request independent review**

Review `eeda0400..HEAD` plus the four Important findings. Fix every Critical/Important finding before proceeding.

- [ ] **Step 3: Deploy with a new verified backup**

Preserve runtime secrets/data, migrate with API ownership, start all services as the validated non-root UID/GID, and verify loopback listeners.

- [ ] **Step 4: Re-run bounded acceptance**

Prove an incomplete manifest declares its hole and cannot self-repair it; pending/retry/recheck/replacement behavior is visible and immutable; REST failure is capability-scoped; both symbols show independent metric evidence and canonical 4/4 live streams; restart IDs remain unique.

- [ ] **Step 5: Update completion state and merge only on evidence**

If any Important gate fails, keep Phase 1 `in progress` and record the exact blocker. Otherwise commit the sanitized evidence, rerun `git diff --check`, merge to `main`, and push both branches.
