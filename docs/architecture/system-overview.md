# System Overview

## Deployment

The system runs through Docker Compose on `192.168.1.4`. A systemd unit supervises the Compose application. The operator accesses the LAN Web console from a Mac.

## Components

| Component | Responsibility |
|---|---|
| `api` | Authentication, commands, queries, schemas, and audit events |
| `market-worker` | Binance REST/WebSocket adapters, normalization, manifests, and gap repair |
| `research-worker` | Backtests, replay, walk-forward procedures, and statistical reports |
| `paper-worker` | Deterministic signals, risk controls, PaperBroker, and portfolio projections |
| `ai-worker` | Frozen snapshots and non-authoritative AI assessments |
| `notify-worker` | Idempotent PostgreSQL Outbox delivery through Hermes |
| `web` | Personal monitoring and research console |
| `postgres` | Operational, versioning, ledger, and audit state |

## Storage

PostgreSQL stores small structured state. Partitioned Parquet under `/srv/crypto-research/data` stores historical bulk data and generated feature/report artifacts. Each artifact is referenced by a manifest, checksum, and version rather than copied into Git.

## Main Data Flow

```text
Binance REST/WebSocket
→ raw data + manifest
→ normalization and quality checks
→ empirical per-symbol profile and eligibility
→ historical or live MarketEvent
→ frozen StrategySpec
→ Signal → RiskEngine → PaperBroker
→ portfolio/ledger + outbox
→ Web console and Hermes/Feishu
```

The AI path receives a copy of a frozen market snapshot after deterministic processing. It cannot call broker or risk-control mutations.

Strategy families are reusable, but an executable MVP `StrategySpec` is bound to one venue, market, and contract symbol. Profiles, evidence conclusions, paper accounts, and risk limits are independently keyed; no symbol inherits another symbol's approval.

## Failure Semantics

- Market-data uncertainty blocks new entries.
- Notification failure queues retries but does not block trading state.
- AI failure is informational and does not block deterministic processing.
- Paper-ledger inconsistency blocks orders until reconciled.
- Every automatic recovery emits an audit event.
