# Paper Trading Product Contract

## Purpose

Paper trading validates real-time strategy behavior and operating reliability without risking funds. It is distinct from fast historical backtesting and timed historical replay.

## Account Model

- Binance USDⓈ-M perpetual semantics.
- Long and short positions.
- Configurable initial USDT balance.
- Default `1x` leverage with an account-level maximum.
- Isolated paper account per strategy version unless explicitly grouped.
- Realized/unrealized PnL, margin, funding, fees, slippage, equity, and drawdown.

## Order Model

Support market, limit, stop, and take-profit orders; partial fills; rejects; reduce-only behavior; and timeouts. Signal time, order creation, fill, cancellation, stop, target, and funding events are separate persisted records.

A completed-bar signal may fill only on a later executable event. Same-bar ordering uses event data or the declared conservative collision policy.

## Risk Controls

- Per-trade risk budget.
- Strategy and account leverage ceilings.
- Maximum daily loss and drawdown.
- Consecutive-loss and stale-data circuit breakers.
- Pause, reduce-only, simulated flatten, and global halt.
- No new positions while required data is incomplete or stale.

## Recovery

The paper ledger is event-sourced. On restart, the worker reconstructs orders, fills, positions, balances, and risk state, then compares the projection with stored snapshots. A mismatch blocks new orders and raises a `RISK`/`SYSTEM` alert.

## Hard Boundary

The MVP has a `PaperBroker` only. It contains no `LiveBroker`, exchange-order credentials, or configuration switch that can route an order to Binance.
