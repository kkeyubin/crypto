# Strategy Workflow

## Contract-First Specification

Freeze one `InstrumentRef`: venue `BINANCE`, market `USD_M_PERPETUAL`, and one
uppercase contract symbol. Record a real `DataManifest` UUID, data type,
checksum/version, coverage, quality limits, and the available-information
cutoff; a human label such as "meme coin" is not an empirical profile.

Choose `BarSpec.kind: time` with an interval and no event threshold, or
`BarSpec.kind: event` with exactly one positive `trade_count`, `volume`, or
`dollar_value`. Do not call a crypto event bar a Volman 70-tick equivalent
without calibration from the symbol's own trade feed.

Use the following order before any outcome is read:

1. Build the symbol profile: realized volatility and jumps, spread and
   slippage by time, volume/liquidity, funding, precision, gaps, outages, and
   abnormal-market frequency. Pause or reject when data or liquidity cannot
   support the stated experiment.
2. State a falsifiable hypothesis, null hypothesis, output/position states,
   chronology, and available-information boundary. Write `volman.chronology`
   as at least three causal events, then freeze signal line, trigger, clear
   path, and invalidation.
3. Select BB or RB for a possible executable specification. DD, FB, SB, IRB,
   and ARB may be recorded only as `mode: observation` research.
4. Add any Nison signal solely as a separately searchable `nison_context`
   variable. Its source section and quantitative expression must be frozen
   before validation data is read.
5. Declare execution and risk: fill timing, fees, spread, slippage, latency,
   funding, liquidation/precision assumptions, stop/target state machine,
   maximum hold, circuit breakers, and collision handling.
6. Preregister bounded `parameters.fixed` and `parameters.search_space`; retain
   every searched configuration and resulting series. Freeze choices before
   final holdout.
7. Set ordered train/validation/test endpoints, a position-bias-matched or
   detrended benchmark, and a joint method such as WRC or multi-rule MCP that
   replays the complete search.
8. Report `candidate`, `rejected`, or `insufficient_evidence`. Only the owner
   can separately approve a symbol for paper trading after its own evidence
   gate.

## Cross-Symbol Gate

Cross-symbol work may compare normalized bps, local volatility, volume, or
event measures as robustness evidence. It cannot transfer a BTCUSDT profile,
costs, parameters, result, holdout, `candidate` conclusion, risk configuration,
or paper-enabled approval to PEPEUSDT. Start the target symbol at step 1 and
obtain explicit owner approval after its independent evidence gate.

## Source Discipline

Volman-derived BB/RB claims must follow the chronological setup rules in
[Volman BB](../../volman-forex-price-action-scalping/chapters/ch10-block-break.md)
or [Volman RB](../../volman-forex-price-action-scalping/chapters/ch11-range-break.md).
Nison context is optional and must be traceable to
[Nison market context](../../nison-beyond-candlesticks/chapters/ch04-market-context.md).
Objective rules, benchmarks, holdouts, and search correction are mandatory
Aronson controls from [objective rules](../../aronson-evidence-based-technical-analysis/chapters/ch01-objective-rules.md)
and [data-mining bias](../../aronson-evidence-based-technical-analysis/chapters/ch06-data-mining-bias.md).
