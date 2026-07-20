# ADR 0002: Binance First, OKX Later

**Status:** Accepted

**Date:** 2026-07-20

## Context

The personal system needs open historical data and real-time perpetual-futures streams. Supporting multiple exchanges in the MVP would multiply normalization, contract, funding, and data-quality behavior before the core pipeline is proven.

## Decision

Use Binance USDⓈ-M perpetual futures as the first adapter. Use public market-data APIs only; no exchange account key is stored. Add OKX after the Binance data, replay, backtest, paper, and recovery workflows pass acceptance.

Direct networking is the default. Proxy port `17891` is enabled only after a direct-access health check shows restriction.

## Consequences

- A backtest uses data from one named venue; Binance and OKX candles are never silently merged.
- Venue, contract, source endpoint family, schema, and checksum are part of every data manifest.
- Exchange interfaces remain adapter-based so later OKX integration does not change strategy contracts.
