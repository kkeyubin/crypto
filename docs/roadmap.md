# Roadmap

This file preserves both committed and deferred work. A deferred item is not permission to implement it; each phase requires its own approved specification and plan.

## Phase 0 — Repository and Contracts

**Status:** complete (2026-07-21)

- Unified Nison/Volman/Aronson research Skill.
- StrategySpec, MarketSnapshot, AIAssessment, and data-manifest schemas.
- Application skeleton, CI, configuration validation, and security defaults.
- Deployment and backup conventions for `192.168.1.4`.

## Phase 1 — Binance Data Foundation

**Status:** in progress (2026-07-22; local implementation complete, server acceptance pending)

- Dynamic USDⓈ-M symbol watchlist.
- Independent symbol onboarding, empirical volatility/liquidity/cost profile, and eligibility state.
- Historical 1m klines, mark price, monthly-only funding, and selected aggTrades; onboarding uses complete UTC calendar months.
- Live WebSocket klines, aggTrades, and best bid/ask.
- Parquet partitioning, checksums, data catalog, gap detection, and REST repair.
- Liquidity eligibility and stale-data gate.

Remaining server acceptance gate: verify bounded BTCUSDT/1000PEPEUSDT archive checksums and required live stream types, restart idempotency, explicit metadata degradation, and no non-loopback listener. Acceptance uses published complete-month funding objects; `PEPE`/`PEPEUSDT` must resolve to canonical `1000PEPEUSDT`. Phase 1 stays in progress until the sanitized acceptance record passes and is committed.

## Phase 2 — Research and Backtesting

**Status:** planned

- Shared event engine and deterministic replay.
- BB and RB time-bar/event-bar strategy families.
- Conservative fill, fee, funding, spread, and slippage models.
- Parameter ledger, benchmarks, walk-forward testing, and Aronson evidence report.
- Per-symbol frozen strategy versions, evidence conclusions, and cost/risk calibration.
- Web experiment comparison.

## Phase 3 — Live Paper Trading and Feishu

**Status:** planned

- Long/short USDⓈ-M PaperBroker, default `1x` leverage.
- Persistent accounts, orders, fills, positions, PnL, funding, and risk controls.
- PostgreSQL Notification Outbox.
- Hermes `crypto` profile Feishu delivery, retries, deduplication, and templates.
- Restart recovery and reconciliation drills.

## Phase 4 — AI Shadow and Personal Console

**Status:** planned

- Deploy the unified Skill to Hermes.
- Frozen-snapshot structured assessments.
- AI-versus-deterministic outcome dataset.
- Observation-only DD/FB/SB/IRB/ARB labels and human review.
- Full single-user LAN dashboard.

## Phase 5 — Range-Family Expansion

**Status:** deferred

- Evaluate IRB and ARB definitions using the validated range/block primitives.
- Promote only after label agreement, nonredundancy, sufficient sample, and preregistered evidence plan.

## Phase 6 — Trend-Family Expansion

**Status:** deferred

- Build trend, pullback, first-attempt, and follow-through primitives.
- Evaluate DD, SB, and FB separately.
- Keep Nison context filters as separately tested hypotheses.

## Phase 7 — Venue and Data Expansion

**Status:** deferred

- OKX adapter and cross-venue comparison.
- Broader order-book history, volume bars, and dollar bars.
- Public Cloudflare access only after a separate identity/access design.

## Explicitly Unapproved

- Real-money trading or `LiveBroker`.
- AI order creation, veto, cancellation, or autonomous strategy activation.
- News/social-sentiment trading.
- Multi-user service or native mobile app.

These require new specifications, ADRs, security review, paper-only validation, and explicit owner approval.
