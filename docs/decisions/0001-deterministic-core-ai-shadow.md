# ADR 0001: Deterministic Core with AI Shadow Analysis

**Status:** Accepted

**Date:** 2026-07-20

## Context

Nison and Volman contain valuable qualitative judgments, but an LLM in the order path introduces nondeterminism, latency, provider failure, model drift, and poor reproducibility. A purely mechanical system is reliable but may underuse qualitative knowledge.

## Decision

All historical and live paper orders are produced by deterministic, versioned code. An asynchronous AI worker may evaluate frozen snapshots with the unified Skill and store `SUPPORT`, `OPPOSE`, or `UNCERTAIN` assessments. It cannot affect orders in the MVP.

## Consequences

- Market monitoring and PaperBroker remain available during AI outages.
- Prompts, model versions, Skill hashes, inputs, and outputs must be retained.
- AI value can be tested against deterministic outcomes before any authority is considered.
- A future AI veto or sizing role requires a new ADR, paper-only experiment, and Aronson evidence gate.
