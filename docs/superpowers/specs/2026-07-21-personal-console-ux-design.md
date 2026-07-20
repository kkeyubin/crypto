# Personal Console UX Design

**Status:** Approved in design review; pending written-spec review

**Date:** 2026-07-21

## 1. Product Character

The console uses the **Evidence Lab** direction: a calm research instrument that keeps live state readable while making evidence quality and uncertainty visible. It must not resemble a casino interface or imply that a signal is proven.

Design tokens:

| Role | Color |
|---|---|
| Page background | `#0B1220` |
| Primary/raised surface | `#101A2C` / `#151F32` |
| Border | `#253148` |
| Primary blue | `#5EA2FF` / `#68B5FF` |
| Positive/healthy | `#60D6A7` |
| Candidate/caution | `#F3BD68` |
| Loss/risk | `#EF7186` |
| Text/muted text | `#E8EEF7` / `#8FA1BD` |

Color is never the only state indicator; labels and icons accompany it. Financial numbers use tabular numerals.

## 2. Navigation and Default Views

The default home page is **指挥台 (Command Center)**. It balances a selected symbol, paper equity and drawdown, current position, signal/event chronology, data health, and service health.

Two secondary home views remain available:

- **市场墙 (Market Wall):** compare all monitored symbols and surface abnormal states.
- **研究桌 (Research Desk):** prioritize experiments, parameter ledgers, evidence gates, and AI-shadow comparisons.

Primary navigation is `总览 / 币种 / 策略 / 回测 / 模拟盘 / 运行`. Each monitored-symbol row shows its own eligibility, evidence state, paper state, data freshness, and risk state; no aggregate badge may imply that all symbols share one approval.

## 3. Symbol Detail

The symbol detail page defaults to **实时驾驶舱** with tabs:

```text
实时监控 | 结构分析 | 历史数据 | 回测 | 模拟账户
```

The live tab combines price/chart, structure and frozen signal line, deterministic signal, risk decision, paper position, data health, and a timestamped event chain. AI shadow opinion is visually subordinate and explicitly non-authoritative.

The **回测** tab contains the Evidence Workbench: strategy/data/cost versions, out-of-sample metrics, benchmark, complete parameter ledger, regime and cost sensitivity, Aronson checks, and a plain-language `candidate / rejected / insufficient evidence` conclusion.

Adding a symbol opens onboarding progress rather than a trading screen: metadata → history → quality → empirical profile → experiments → evidence result → owner-controlled paper activation.

## 4. Language, Responsive Layout, and Motion

Default locale is `zh-CN`; `en` is included from the first release. All text, dates, numbers, currency, validation errors, alerts, and status labels use translation/formatting resources. Stable internal enums and event payloads are never localized.

Desktop layouts use bounded CSS grid regions with explicit minimum widths. Cards must stack at narrower breakpoints; content containers must not inherit horizontal comparison-card flex rules. Navigation tabs may scroll horizontally, but core values and risk banners cannot be clipped.

Motion follows **克制精确 (Calm Precision)**:

- page/view transition: approximately `160ms` fade;
- price update: one `600ms` directional tint, never continuous flashing;
- normal signal: badge plus a single halo;
- risk alert: persistent banner requiring acknowledgement;
- `prefers-reduced-motion: reduce`: disable decorative motion and update state directly.

## 5. Acceptance Checks

- Chinese is the default after first load; locale choice persists.
- BTC and PEPE can simultaneously display different profiles, evidence states, risk limits, and paper states.
- No AI result is visually presented as an order decision.
- Loss, stale data, insufficient evidence, and risk halt remain understandable without color.
- At desktop, tablet, and narrow viewport sizes, charts, tabs, values, and alerts remain legible without whole-page horizontal compression.
