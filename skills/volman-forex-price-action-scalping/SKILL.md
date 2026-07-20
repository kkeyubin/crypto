---
name: volman-forex-price-action-scalping
description: "Use when studying or operationalizing Bob Volman's Forex Price Action Scalping; distinguishing Double Doji Break, First Break, Second Break, Block Break, Range Break, Inside Range Break, or Advanced Range Break; analyzing pre-breakout tension, clear path, signal bars, or tipping-point exits; or adapting these discretionary EUR/USD 70-tick concepts for auditable crypto research."
---

# Volman Forex Price Action Scalping

## Purpose

Translate Bob Volman's seven discretionary price-action setups into faithful explanations, chart-review checklists, or falsifiable research specifications. Treat every setup as a hypothesis, not evidence of profitability.

## Operating Rules

1. Establish context before naming a setup: pressure, trend/range state, and clear path to the target.
2. Identify the structure using only information available at that moment.
3. Freeze the signal line before the breakout. The entry bar must actually take it out; do not anticipate.
4. Reject an attractive pattern when nearby clustering, a defended barrier, or inadequate space blocks the trade.
5. Define the initial tipping point and each permitted update before evaluating results. Never move risk farther from the target.
6. Separate Volman's original EUR/USD parameters from any translated crypto parameters.
7. Report assumptions, ambiguous classifications, costs, and failure cases. Do not promise returns.

## Original Reference Environment

- EUR/USD 70-tick chart; a bar closes after 70 transactions, not 70 seconds.
- 20 EMA is the only indicator and is a visual guide, not a mechanical barrier.
- Fixed 10-pip target; protective risk no more than 10 pips; typical technical stop about 6–7 pips.
- Small bars in DD are commonly no more than about 3 pips; an entry/invalidity break is commonly one pip.
- Round-number zones ending in 00 or 50 mattered in that market.

These are historical anchors. Tick definitions vary by provider, and none of the pip thresholds transfer directly to crypto.

## Seven-Setup Map

| Setup | Required structure | Trigger | Common confusion |
|---|---|---|---|
| DD | Trend pullback, usually 40–60%, with 2+ adjacent small bars/dojis | Break of the trend-side extreme | A pair of dojis without trend context |
| FB | Strong new surge and its first substantial pullback | First pullback bar taken out with the trend | Any first EMA touch |
| SB | Pullback, failed/lacking first with-trend break, renewed counterpressure | Second with-trend break | Two adjacent bars |
| BB | Compact minirange with visible top and bottom | Break on the path-of-least-resistance side | Trading either box edge mechanically |
| RB | Mature range plus pressure built at its boundary | Break of the original range barrier | Direct false break or premature tease |
| IRB | Small block inside a sufficiently wide established range | Boomerang toward the opposite edge, or BB-like break inside it | An early bet on the range's external breakout |
| ARB | Price already outside the old range, then clusters or pulls back | New signal line outside the old range | Standard RB at the original boundary |

For full distinctions, read [patterns.md](patterns.md) and the relevant chapter file.

## Analysis Workflow

### 1. Normalize the observation

State venue, instrument, spot/perpetual, trade-feed definition, bar construction, timezone, fees, spread/slippage model, and whether decisions occur intrabar or on completed bars.

### 2. Describe context without setup labels

Record swing direction, trend age, pullback depth, compression, nearby barriers, round-number effects, and available target space. If "favorable" or "clear path" cannot be expressed reproducibly, mark the case discretionary.

### 3. Classify and freeze

Choose one primary setup, list the required events in chronological order, and timestamp when each boundary or signal bar became knowable. Do not redraw a box after seeing the outcome.

### 4. Specify execution and validity

Define breakout increment, order type, fill assumption, initial tipping point, update rule, target, maximum holding period, and collision handling when stop and target occur in the same bar.

### 5. Test as a hypothesis

Use untouched data, realistic costs, and multiple-testing controls when searching thresholds. Compare against simple benchmarks and retain rejected/ambiguous signals. Pair this Skill with the Aronson evidence-based Skill for inference.

## Crypto Translation Guardrails

- Compare time, fixed-trade-count, volume, and dollar bars; do not call a crypto bar "70-tick equivalent" without calibration.
- Express pip distances as candidates in ticks, basis points, or local volatility units.
- Re-estimate round-number zones empirically by instrument and price regime.
- Use event data when intrabar order matters. OHLC bars alone cannot reveal all entry/stop/target ordering.
- Include maker/taker fees, spread, slippage, latency, funding, liquidation, and exchange outages.
- Pre-register parameter families and correct for data-mining bias.

## Output Contract

Return: observation specification; setup and chronological evidence; veto checks; frozen signal line; execution/exit state machine; original-versus-translated parameters; ambiguities; test design; and a conclusion of `candidate`, `rejected`, or `insufficiently specified`.

## Reference Index

- Quick decisions: [cheatsheet.md](cheatsheet.md)
- Reusable state patterns: [patterns.md](patterns.md)
- Terminology: [glossary.md](glossary.md)
- Foundations: [Ch. 1](chapters/ch01-trading-currencies.md), [Ch. 2](chapters/ch02-tick-chart.md), [Ch. 3](chapters/ch03-scalping-as-business.md), [Ch. 4](chapters/ch04-target-stop-orders.md), [Ch. 5](chapters/ch05-probability-principle.md), [Ch. 6](chapters/ch06-setups.md)
- Setups: [DD](chapters/ch07-double-doji-break.md), [FB](chapters/ch08-first-break.md), [SB](chapters/ch09-second-break.md), [BB](chapters/ch10-block-break.md), [RB](chapters/ch11-range-break.md), [IRB](chapters/ch12-inside-range-break.md), [ARB](chapters/ch13-advanced-range-break.md)
- Management and cautions: [Ch. 14](chapters/ch14-tipping-point.md), [Ch. 15](chapters/ch15-unfavorable-conditions.md), [Ch. 16](chapters/ch16-trade-volume.md), [Ch. 17](chapters/ch17-words-of-caution.md)
