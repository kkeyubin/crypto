# ADR 0003: BB/RB Trade, Other Volman Setups Observe

**Status:** Accepted

**Date:** 2026-07-20

## Context

Implementing all seven Volman setups in the first study would create overlapping labels, arbitrary proxies for qualitative terms, a combinatorial parameter search, and small samples per family. These conditions undermine the Aronson evidence standard.

## Decision

Only Block Break (BB) and Range Break (RB) may become deterministic order-producing MVP strategies. Time-bar and event-bar versions are distinct strategy families.

DD, FB, SB, IRB, and ARB stay documented in the unified Skill and may be recorded by AI/human review as observation-only shadow labels. They cannot create paper orders.

Nison features are optional research variables rather than a mandatory filter. Aronson controls are mandatory for every strategy.

## Promotion Gate

A shadow setup may enter deterministic research only when:

1. Its causal definition is reproducible without future information.
2. Independent reviewers can classify examples consistently.
3. It is not merely a duplicate of an existing family.
4. The available sample supports a preregistered test.
5. Search bounds, costs, benchmarks, and final holdout are frozen.

Preferred dependency order is `BB/RB → IRB/ARB → DD/SB/FB`, but evidence can stop any branch permanently.

## Source Reconciliation

This decision incorporates the useful priority and scope recommendations in the reviewed shared conversation: <https://chatgpt.com/share/6a5e43e4-c454-83ea-b052-93c0c3b1535a>.
