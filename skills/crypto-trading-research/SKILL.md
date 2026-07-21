---
name: crypto-trading-research
description: Use when a cryptocurrency strategy idea, backtest claim, symbol qualification, cross-symbol transfer request, or frozen market snapshot needs research review.
---

# Crypto Trading Research

## Purpose

Treat Volman setups as falsifiable hypotheses, Nison as optional context, and
Aronson evidence controls as mandatory. A favorable sample never proves future
profitability or authorizes an order.

Use [strategy-workflow.md](references/strategy-workflow.md) for specification,
audit, and symbol-qualification work. Use [runtime-ai.md](references/runtime-ai.md)
for shadow assessment. Trace book-derived statements through
[source-map.md](references/source-map.md).

## Operating Modes

1. **Specify:** produce a `StrategySpec`-oriented proposal only when all known
   facts can be represented by the current contract; otherwise report
   `insufficient_evidence` and request the missing facts.
2. **Audit:** review chronology, available-information timing, costs, parameter
   ledger, benchmark, joint inference, holdout, and failure evidence.
3. **Compare:** compare research results without transferring evidence,
   parameters, costs, conclusions, or paper permission between symbols.
4. **Shadow:** accept a schema-valid frozen `MarketSnapshot` and emit only a
   schema-valid `AIAssessment`.

## StrategySpec Contract

`StrategySpec` is the author/input contract: callers omit `content_hash`, while
the Python object may compute it as a non-serialized convenience property.
`StrategySpecRecord` is the persisted/output contract: it keeps the same flat
payload, requires the canonical SHA-256 `content_hash`, and rejects a hash that
does not match the payload. Convert validated input to a record explicitly;
never invent or independently supply the hash.

All seven families—BB, RB, DD, FB, SB, IRB, and ARB—may use `mode: observation`.
Only BB and RB may use `mode: executable`. Executable mode requires both `execution` and `risk`.
Observation mode rejects `execution`, `risk`, and `identity.state: paper_enabled`.

For executable BB/RB, `execution` must state
the completed-bar signal source, next executable fill timing, order type,
collision policy, maker/taker fees, spread, slippage, latency, and funding
inclusion. `risk` must state risk fraction, leverage ceiling, daily-loss and
drawdown limits, and stale-data behavior.

Every `StrategySpec` requires `identity` (lowercase hyphenated name, semantic
version, state, author), at least two `provenance` references, one `instrument`
(`BINANCE`, `USD_M_PERPETUAL`, one uppercase contract symbol), one `bar`,
`volman`, `parameters.fixed`, `parameters.search_space`, and ordered
`evidence.train_end`, `validation_end`, and `test_end`, plus benchmark and
multiple-testing method.

Nison context is optional. Nison conditions, if used, are separate
`nison_context` hypotheses with `name`, `expression`, and `source_section`;
they never turn an observation family into an executable one. Aronson evidence
controls are mandatory.

The output conclusion is exactly `candidate`, `rejected`, or
`insufficient_evidence`. Do not label a proposal `candidate` until the symbol
profile, eligibility, declared costs, preregistered search, independent
backtest/holdout, and evidence gate support that conclusion.

## Non-Negotiable Boundaries

- Freeze venue, market, one symbol, data-manifest UUID/version and coverage,
  bar construction, and information cutoff before searching.
- A completed-bar signal fills only at the next executable event; use event data
  or declared conservative handling when intrabar order matters.
- Keep every tried configuration, ambiguous classification, rejected candidate,
  and failed experiment. Apply a position-bias-matched or detrended benchmark
  and joint correction to the complete search universe.
- BTCUSDT evidence, parameter values, cost model, conclusion, and paper permission do not qualify PEPEUSDT. Each target symbol requires an independent symbol profile, data-quality and liquidity eligibility check, symbol-specific cost model, preregistered parameter family, independent backtest, untouched holdout, evidence conclusion, risk configuration, and explicit owner approval.
- Never activate or mutate a strategy; access credentials; or create, cancel,
  resize, veto, or otherwise affect an order. This Skill produces research
  artifacts only.

## Shadow Output Boundary

For research inputs that are incomplete, return `insufficient_evidence` and
the missing inputs. For shadow work, accept no prose substitute: the input must
validate as a frozen `MarketSnapshot`. If it does not, request a valid snapshot
and do not emit an `AIAssessment`.

For a valid snapshot, return only the closed `AIAssessment` object:
`assessment_id`, `snapshot_id`, `opinion` (`SUPPORT`, `OPPOSE`, or
`UNCERTAIN`), non-empty `reasons`, non-empty principle `citations`,
`risk_notes`, `market_data_cutoff` equal to the snapshot `cutoff`, `model_id`,
`prompt_version`, and `skill_version` (with optional default
`schema_version`). Missing evidence requires `UNCERTAIN`, not an invented
`SUPPORT` or `OPPOSE`. Do not add action, position, leverage, stop, cancellation,
or credential fields.
