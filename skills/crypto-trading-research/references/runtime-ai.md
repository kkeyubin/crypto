# Runtime AI Contract

## Input

Accept only a schema-valid frozen `MarketSnapshot`: UUID `snapshot_id`,
`BINANCE`/`USD_M_PERPETUAL` one-symbol instrument, UTC `cutoff`, UUID
`data_manifest_id`, 64-character lowercase `strategy_spec_hash`, valid OHLCV
`bars`, and optional `best_bid_ask`, `funding`, and `deterministic_signal_id`.
Only `bars`, `best_bid_ask`, and `funding` are timestamped market observations in the current contract.
`deterministic_signal_id` is an optional UUID identifier only; it carries no signal payload or timestamp.
A signal payload or signal timestamp requires a future contract extension.

Each timestamped observation must be no later than `cutoff`. Treat `cutoff` as
the information boundary; do not infer or use later market knowledge.

If this object is absent or invalid, request it and emit no assessment. Do not
turn a user assertion, a live price, or an order request into a snapshot.

## Output

Allowed `AIAssessment` fields (and no others): `schema_version`, `assessment_id`, `snapshot_id`, `opinion`, `reasons`, `citations`, `risk_notes`, `market_data_cutoff`, `model_id`, `prompt_version`, `skill_version`.

Emit only a schema-valid object from that field set. `assessment_id` is a UUID;
`snapshot_id` is copied from the input; `opinion` is `SUPPORT`, `OPPOSE`, or
`UNCERTAIN`; `reasons` and `{skill, section}` citations are non-empty;
`market_data_cutoff` equals the snapshot `cutoff`; and model, prompt, and Skill
versions are recorded. The closed schema admits no action fields.

If the snapshot cannot support the claimed conclusion, use `UNCERTAIN` and name
the evidence gap. A valid object is not an order authorization.

## Authority

The deterministic system creates historical and paper orders. The AI is
shadow-only: it cannot call broker, risk, lifecycle, order, notification, or
credential APIs; create, cancel, resize, veto, or manage an order; mutate a
strategy; or make a credential-bearing request. Store the assessment only after
deterministic processing with its model, prompt, and Skill versions.

The AI must not create, cancel, resize, or veto orders and must not access credentials.

## Evidence Framing

Use [Volman chronology](../../volman-forex-price-action-scalping/chapters/ch10-block-break.md)
and optional [Nison confirmation](../../nison-beyond-candlesticks/chapters/ch03-candlestick-patterns.md)
as cited context, while retaining [Aronson's available-information and
executable-timing control](../../aronson-evidence-based-technical-analysis/chapters/ch01-objective-rules.md).
