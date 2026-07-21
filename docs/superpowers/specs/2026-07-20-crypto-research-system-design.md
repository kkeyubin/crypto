# Crypto Research System Design

**Status:** Approved

**Date:** 2026-07-20

**Last revised:** 2026-07-21

**Audience:** Single owner/operator

**Deployment target:** `keyubin@192.168.1.4`

## 1. Purpose

Build a personal, auditable cryptocurrency research system that can add monitored symbols, acquire historical and real-time market data, evaluate versioned strategies, run historical backtests and live paper trading, issue Feishu notifications, and compare deterministic signals with asynchronous AI shadow assessments.

The system must optimize for reproducibility and falsification rather than the appearance of profitability.

## 2. Confirmed Boundaries

- Binance USDⓈ-M perpetual futures are the first market-data source.
- OKX is a planned adapter and cross-venue validation source, not an MVP dependency.
- The first release supports backtesting, historical replay, paper trading, alerts, and AI shadow analysis.
- The first release contains no exchange credentials, `LiveBroker`, or real-order path.
- Services run continuously on `192.168.1.4`; the Mac is the research and management terminal.
- Network proxy port `17891` is used only when direct exchange access is restricted.
- The Web UI is single-user and LAN-only by default.
- Project documentation, decisions, deferred work, strategy specifications, and experiment manifests are versioned in Git. Large market data remains outside Git.

## 3. Book-Knowledge Roles

The three books are not weighted equally:

| Source | Role |
|---|---|
| Bob Volman | Primary source of short-term price-action hypotheses and trade-management candidates |
| Steve Nison | Optional market-context, confirmation, support/resistance, and chart-representation variables |
| David Aronson | Mandatory scientific-method, bias-control, benchmark, and evidence gate |

MVP executable strategies are limited to Volman Block Break (BB) and Range Break (RB). DD, FB, SB, IRB, and ARB remain in the unified Skill and are collected as observation-only shadow labels. They cannot create paper orders in the first release.

This scope is scientific rather than technical: implementing seven overlapping, subjective strategy families at once would create a large search space, unclear classification boundaries, low per-family sample sizes, and data-mining bias.

## 4. Architecture

Use a modular monolith with independently supervised processes:

```text
Docker Compose
├── api                 FastAPI control/query surface
├── market-worker       REST backfill, WebSocket capture, gap repair
├── research-worker     Backtests, walk-forward tests, statistical analysis
├── paper-worker        Deterministic signals, risk checks, PaperBroker
├── ai-worker           Asynchronous unified-Skill shadow assessments
├── notify-worker       PostgreSQL outbox to Hermes/Feishu
├── web                 React/TypeScript personal console
└── postgres            Operational and audit state
```

Historical bulk data is stored as partitioned Parquet and queried with Polars/DuckDB. PostgreSQL owns configuration, strategy versions, data manifests, jobs, signals, paper-account state, AI assessments, alerts, and audit events. The MVP does not require Redis, Kafka, Kubernetes, or microservices.

## 5. Data Design

The first adapter captures:

- historical and live 1-minute klines;
- mark price and funding rate;
- live aggregate trades and best bid/ask;
- historical aggregate trades for selected symbols and experiments.

Data is stored under an operator-configurable root, recommended as:

```text
/srv/crypto-research/data/
├── raw/<venue>/<market>/<symbol>/<type>/<date>/
├── normalized/<venue>/<market>/<symbol>/<type>/<date>/
├── features/<strategy-family>/<feature-version>/
└── reports/<experiment-id>/
```

Every dataset has a manifest containing source, venue, contract, time range, retrieval time, schema version, checksums, missing intervals, repair history, and normalization version. Timestamps are UTC internally; the UI may display Asia/Shanghai.

A WebSocket disconnect triggers reconnection and REST gap repair. Stale or unverified data blocks new paper positions. Time bars and event bars are separate preregistered research families; no crypto bar is called a “70-tick equivalent” without calibration.

### 5.1 Symbol Onboarding and Adaptation

Adding a symbol starts an independent research pipeline; it never inherits another symbol's evidence or paper-trading approval:

```text
add symbol
→ verify contract metadata and data availability
→ backfill and validate history
→ build empirical symbol profile
→ run preregistered strategy experiments
→ candidate | rejected | insufficient evidence
→ explicit owner approval before paper trading
```

The profile records realized volatility and jumps, spread, estimated slippage, volume and liquidity by time of day, funding, price/quantity precision, data gaps, and abnormal-market frequency. Classification is based on measured properties rather than a manually assigned label such as “meme coin.”

Strategy-family semantics remain shared across symbols, but bar construction, cost assumptions, normalized thresholds, position risk, leverage ceiling, and circuit breakers may be symbol-specific. Those adaptations must be declared before the final holdout is read. BTC results never qualify PEPE, and a profitable PEPE search never changes the frozen BTC version.

## 6. Strategy Contract

The unified trading-research Skill produces or reviews a machine-validated `StrategySpec`. It does not place orders or mutate runtime state.

Each specification includes:

- identity, semantic version, state, author, and content hash;
- book/Skill/section provenance;
- venue, market, symbol, data type, and bar construction; an MVP executable version targets exactly one contract symbol;
- Nison context variables when used;
- Volman setup chronology, frozen signal line, trigger, clear-path test, and invalidation;
- order, fill, fee, spread, slippage, funding, target, and tipping-point rules;
- position sizing, leverage ceiling, drawdown limits, and circuit breakers;
- fixed parameters and the complete preregistered parameter-search family;
- benchmark, train/validation/test split, walk-forward protocol, and Aronson evidence criteria.

Lifecycle:

```text
draft → reviewed → frozen → backtested → paper_enabled → retired
```

Only an unchanged `frozen` specification may run. Any material edit creates a new version and content hash; prior experiments continue to reference the old version.

## 7. Shared Event Engine

Historical backtest, historical replay, and live paper trading share the same strategy, risk, broker, and portfolio components:

```text
MarketEvent → Strategy → Signal → RiskEngine → Order
→ PaperBroker → Fill → Portfolio → NotificationOutbox
```

Only the clock and market-data source differ. A signal derived from a completed candle cannot fill at that candle's close. The next executable event, order type, spread, slippage, latency, fees, funding, and partial-fill model determine the fill. Ambiguous same-bar target/stop collisions use event data or a declared conservative outcome.

## 8. Paper Trading

Paper trading models Binance USDⓈ-M perpetuals with long/short positions, configurable leverage, margin, funding, maker/taker costs, slippage, stop/target orders, partial fills, rejects, realized/unrealized PnL, equity, drawdown, and liquidation estimates.

Default leverage is `1x`; account-level configuration caps strategy leverage. Each strategy can have an isolated paper account. Account state is event-sourced and persisted so process restart reconstructs and reconciles orders, fills, positions, balances, and risk state.

Operator controls include pause, reduce-only mode, simulated flatten, per-strategy circuit breaker, and global circuit breaker. There is no `LiveBroker` class in the MVP.

## 9. Evidence Gate

Every backtest report includes full trades, equity, drawdown, Sharpe/Sortino, turnover, win/loss distribution, cost sensitivity, regime breakdown, all searched configurations, and comparison with simple and randomized benchmarks.

The workflow separates training, validation, and untouched final test data. Searches are replayed as complete procedures during walk-forward testing. Appropriate multiple-testing or maximum-statistic correction is required when selecting among rules. Results end with `candidate`, `rejected`, or `insufficient evidence`; profitable in-sample output alone cannot enable paper trading.

Evidence conclusions are scoped to the complete tuple of venue, market, contract symbol, data version, strategy version, parameter-search family, and cost model. Cross-symbol comparisons may test robustness, but cannot silently pool approvals or transfer a conclusion from one symbol to another.

## 10. Deterministic Core and AI Shadow

Real-time order decisions never depend on an LLM in the MVP. The deterministic engine remains available during model, network, or Hermes failure.

On selected events, the AI worker creates a frozen `MarketSnapshot` and invokes the server's Hermes `crypto` profile with the repository-owned unified Skill. Output must validate against an `AIAssessment` schema:

```text
SUPPORT | OPPOSE | UNCERTAIN
+ reasons
+ cited Nison/Volman/Aronson principles
+ risk notes
+ market-data cutoff
+ model, prompt, and Skill versions
```

Assessments cannot create, cancel, resize, or veto paper orders. The system stores deterministic outcome versus AI opinion so Aronson-style tests can later determine whether AI adds stable, cost-effective information. Only a separately approved future specification may let AI affect paper trading.

The five non-MVP Volman setups are recorded in this shadow path with candidate label, chronology, conflicts, model version, and optional human review. They remain non-trading observations.

## 11. Notifications

The notification path is:

```text
PostgreSQL Outbox → HermesFeishuNotifier
→ hermes -p crypto send --to feishu → Feishu
```

Message categories are `INFO`, `SIGNAL`, `TRADE`, `RISK`, `SYSTEM`, and `AI`. Signals carry unique IDs for deduplication. High-risk messages send immediately; low-priority messages may batch. Delivery attempts, sanitized response metadata, retry count, and final state are persisted. Notification failure never blocks market ingestion or paper execution.

The `crypto` Hermes gateway is installed as enabled user service `hermes-gateway-crypto.service`, with systemd linger enabled. A real Feishu test was successful on 2026-07-20 after resolving Feishu error `[230002] Bot/User can NOT be out of the chat` by restoring bot membership in the target chat. Chat and message identifiers are not retained in project documentation.

## 12. Personal Console

The React console contains:

- overview of service health, data freshness, paper equity, positions, and alerts;
- symbol management, backfill progress, data gaps, and liquidity eligibility;
- strategy versions, provenance, parameters, hashes, state transitions, and controls;
- experiments, reports, parameter ledgers, comparisons, and evidence conclusions;
- paper accounts, orders, fills, positions, funding, and risk actions;
- deterministic signals, AI assessments, disagreements, and human reviews;
- notification outbox, system incidents, decision records, and roadmap links.

Destructive controls require explicit confirmation and create audit events.

The detailed visual, information-architecture, i18n, and motion contract is defined in [Personal Console UX Design](./2026-07-21-personal-console-ux-design.md).

## 13. Security and Operations

- No exchange-account secrets or wallet material are needed.
- Hermes owns Feishu credentials; the application invokes only its send command.
- Config is validated at startup and secrets remain outside Git.
- The LAN UI still requires authentication.
- Public exposure through Cloudflare is deferred until a separate access-control design is approved.
- Health checks cover source latency, sequence gaps, worker heartbeat, paper-ledger consistency, database backup, outbox age, and Hermes delivery.

## 14. MVP Acceptance

The release is accepted only when it demonstrates:

```text
add BTCUSDT
→ download and verify historical data
→ backtest BB and RB time/event-bar families
→ produce an Aronson audit report
→ freeze one evidence-qualified strategy version
→ enable live paper trading
→ create deterministic signal/order/fill/position events
→ send Hermes/Feishu notifications
→ record AI shadow opinion without changing the order
→ restart services and recover paper/data/notification state
```

The acceptance suite also adds a second, materially different contract such as canonical `1000PEPEUSDT` and proves that it receives an independent profile, cost/risk configuration, experiment ledger, evidence conclusion, and paper-enable decision. The two symbols are allowed—and expected—to produce different outcomes.

Automated tests cover bar construction, no-lookahead timing, deterministic replay, fee/funding accounting, risk rejection, restart recovery, data-gap blocking, outbox idempotency, and schema/version validation.

## 15. Explicit Non-Goals and Roadmap Preservation

MVP excludes real trading, OKX, multi-user/social functions, news/sentiment, native mobile apps, AI strategy activation, and executable DD/FB/SB/IRB/ARB strategies. These are preserved in [the roadmap](../../roadmap.md), with prerequisites and promotion gates rather than silently forgotten.

## 16. External Conversation Reconciliation

The shared ChatGPT discussion agreed with the present design on context-before-pattern, Volman's discretionary nature, the need to translate rather than copy 70-tick/pip parameters, realistic crypto costs, and Aronson's role in controlling data-mining bias.

It also improved this design in two ways:

1. Volman is the primary hypothesis source, Nison is optional rather than a mandatory prefilter, and Aronson is mandatory.
2. The first executable strategy scope is BB and RB only; the other five setups are retained as observation-only shadow labels until objective definitions, adequate samples, nonredundancy, and out-of-sample evidence justify promotion.

Source reviewed: <https://chatgpt.com/share/6a5e43e4-c454-83ea-b052-93c0c3b1535a>
