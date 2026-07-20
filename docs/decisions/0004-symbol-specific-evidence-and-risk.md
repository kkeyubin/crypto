# ADR 0004: Share Strategy Semantics, Not Symbol Approval

**Status:** Accepted

**Date:** 2026-07-21

## Context

BTC and PEPE can differ materially in volatility, jumps, liquidity, spread, funding, precision, history, and market participation. One fixed parameter set may misrepresent those differences, while unrestricted optimization per symbol creates a large overfitting surface.

## Decision

BB/RB definitions, chronology, execution semantics, and evidence rules are shared strategy-family contracts. Every executable MVP `StrategySpec` targets exactly one venue, market, and contract symbol.

Each added symbol receives an empirical profile, data-quality and liquidity eligibility check, declared cost model, preregistered parameter family, independent backtest and holdout, evidence conclusion, and risk configuration. Thresholds should use normalized units such as bps, local volatility, volume, or event-bar measures where appropriate. Symbol-specific choices must be frozen before the final holdout is read.

A symbol starts paper trading only after its own evidence gate and explicit owner approval. Evidence, paper-enabled state, and tuned parameters do not transfer between symbols.

## Consequences

- Adding `PEPEUSDT` does not reuse `BTCUSDT` approval.
- The same BB/RB concept can yield different parameters or conclusions by symbol.
- Higher volatility may reduce position size or eligibility; it does not merely widen stops.
- Cross-symbol tests are robustness evidence, not a shared activation switch.
- Research storage and UI keys include venue, market, symbol, strategy version, data version, and cost-model version.
