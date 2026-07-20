---
name: aronson-evidence-based-technical-analysis
description: "Use when evaluating technical trading rules, backtests, statistical significance, data-mining bias, multiple-testing risk, White's Reality Check, Monte Carlo permutation, detrending, walk-forward evidence, or claims of predictive power through David Aronson's Evidence-Based Technical Analysis framework."
---

# Evidence-Based Technical Analysis

**Author**: David R. Aronson | **Pages**: 528 | **Chapters**: 9 | **Generated**: 2026-07-20

## How to Use This Skill

- 无参数：使用下方 EBTA 核心框架审计研究证据。
- 指定主题：从 Topic Index 定位后读取对应章节。
- 指定章节：读取 `chapters/chNN-*.md`。
- 设计或审计策略：同时读取相关章节和 `cheatsheet.md`。
- 区分三种内容：作者原框架、基于框架的推导、针对现代加密市场的项目扩展。

## Core Frameworks & Mental Models

### 1. 先判断命题是否可检验

只评价具有 cognitive content 的命题。把“走势看起来强”“形态很漂亮”改写为可重复规则：数据、运算、参数、信号、执行、持有期和结果指标均须明确。若无法预先说清什么观察会推翻主张，就不能形成循证结论。

### 2. 客观规则是输入—变换—输出系统

把每条规则写成确定性函数：

```text
available inputs at t -> operators and parameters -> position state -> executable return
```

明确输出是 `long/short`、`long/neutral`、`short/neutral` 还是三态。不要为了方便回测而强迫策略永久持仓；输出状态本身也是研究假设。

### 3. 选择与规则主张相匹配的基准

绝对回测收益没有独立含义。规则的仓位偏置与市场净趋势会共同制造收益。若目标是检验预测力，使用去趋势市场收益或与规则具有相同多空偏置的随机基准；若主张是“优于某现有策略”，直接用该策略作基准。

### 4. 用可证伪逻辑组织研究

从假设演绎出事前预测，用未知结果的数据检验。未出现预测结果可以否证假设；出现相符结果只能让假设暂时存活，不能证明唯一解释为真。优先寻找反例、替代机制和失效条件。

### 5. 把样本统计量与总体参数分开

回测均值、Sharpe 或胜率是样本统计量，不是未来总体参数。用抽样分布量化其随机变化。解释 `p-value` 时说：“若零假设成立，观察到至少同样极端统计量的概率”，不要说“零假设为真的概率”。同时报告 I 类错误、II 类错误、功效和置信区间。

### 6. 单规则推断与搜索后推断不可混用

事先固定一条规则时，单规则 bootstrap 或 permutation test 可能合适。从 N 条规则中挑出最佳者时，测试统计量已经变为 `max(statistic_1 ... statistic_N)`。必须重建整个搜索过程的零假设分布，而不是只检验最终赢家。

### 7. 识别数据挖掘偏差的五个驱动因素

赢家的观察表现通常高于期望表现。偏差随以下条件变化：

| 因素 | 偏差方向 |
|---|---|
| 测试规则数量增加 | 增大 |
| 每条规则观测数量增加 | 减小 |
| 规则收益相关性降低 | 增大 |
| 正向极端收益增强 | 通常增大 |
| 规则真实期望收益差异减小 | 增大 |

将人工试错、参数调整、样本起止选择和指标筛选都计入搜索过程。

### 8. 使用与搜索过程匹配的校正

- **Walk-forward / untouched holdout**：在训练区间发现规则，在未参与选择的数据上评价；测试数据使用后即失去纯净性。
- **White's Reality Check (WRC)**：保存所有规则的逐期收益，用联合 bootstrap 构造“无效规则宇宙中最大均值”的分布。
- **Multi-rule Monte Carlo permutation (MCP)**：保存所有规则输出，对全部规则使用同一收益排列以保留相关结构，每轮记录最大表现。
- **Markowitz–Xu shrinkage**：只作为近似敏感性检查，不作为主要证据。

### 9. 消除前视偏差与不可实现成交

若信号依赖本周期收盘，不能假定在确认前以同一收盘成交；作者日线案例使用下一日开盘。对滞后发布或后续修订的数据，必须使用当时真实可得版本。把手续费、点差、滑点、资金费率和延迟用于可交易性评价。

### 10. 人提出候选，机器负责一致评价

人的优势是领域知识、指标构造和目标定义；机器擅长一致地组合变量、保存全部实验和淘汰弱候选。不要把机器搜索出的漂亮结果交回直觉裁决，也不要让机器在未记录的无限空间中搜索。

## Research Audit Contract

每次策略研究至少输出：

1. 可证伪命题与零假设；
2. 数据版本、时间边界与可得性；
3. 完整候选宇宙和搜索账本；
4. 基准与去趋势/超额收益定义；
5. 信号到成交的时间映射；
6. 成本前、成本后统计量；
7. 与搜索过程匹配的联合推断；
8. 冻结后的样本外或 walk-forward 结果；
9. 失败结果、局限和不可归因部分。

## Chapter Index

| # | Title | Key Frameworks |
|---|---|---|
| [ch01](chapters/ch01-objective-rules.md) | Objective Rules and Their Evaluation | 客观规则、基准、去趋势、前视偏差 |
| [ch02](chapters/ch02-illusory-validity.md) | The Illusory Validity of Subjective TA | 认知偏差、随机图形、虚假确信 |
| [ch03](chapters/ch03-scientific-method.md) | The Scientific Method and TA | 假设、证伪、演绎与归纳 |
| [ch04](chapters/ch04-statistical-analysis.md) | Statistical Analysis | 总体、样本、抽样分布、相关性 |
| [ch05](chapters/ch05-hypothesis-tests.md) | Hypothesis Tests and Confidence Intervals | H0、p 值、错误、bootstrap、MCP |
| [ch06](chapters/ch06-data-mining-bias.md) | Data-Mining Bias | 五因素、walk-forward、WRC、多规则 MCP |
| [ch07](chapters/ch07-nonrandom-price-motion.md) | Theories of Nonrandom Price Motion | EMH、行为金融、反馈、信息扩散 |
| [ch08](chapters/ch08-sp500-case-design.md) | S&P 500 Case Study Design | 6,402 条规则、趋势/极值/背离 |
| [ch09](chapters/ch09-results-and-future.md) | Results and the Future of TA | 联合显著性、人机协作、局限 |

## Topic Index

- **Behavioral finance / 行为金融** → ch02, ch07
- **Benchmark / 基准** → ch01, ch09
- **Bootstrap** → ch05, ch06
- **Cognitive content / 认知内容** → ch02, ch03
- **Confidence interval / 置信区间** → ch05, ch06
- **Data-mining bias / 数据挖掘偏差** → ch06, ch09
- **Detrending / 去趋势** → ch01, ch05, ch09
- **Efficient market hypothesis** → ch07
- **Falsification / 证伪** → ch03, ch05
- **Look-ahead bias / 前视偏差** → ch01, ch09
- **Monte Carlo permutation** → ch05, ch06, ch09
- **Multiple testing / 多重检验** → ch06, ch09
- **Objective rule / 客观规则** → ch01, ch08
- **p-value** → ch05, ch09
- **Position bias / 仓位偏置** → ch01
- **Random walk / 随机游走** → ch02, ch07
- **Walk-forward** → ch06
- **White's Reality Check** → ch05, ch06, ch09

## Supporting Files

- [glossary.md](glossary.md) — 关键术语
- [patterns.md](patterns.md) — 可复用研究模式
- [cheatsheet.md](cheatsheet.md) — 审计顺序、决策规则与案例数字

## Scope & Limits

本 Skill 提炼 2007 年原书的方法论，不代表任何策略已经有效。原书未覆盖现代加密永续、purged/embargo cross-validation、deflated Sharpe ratio 或 probability of backtest overfitting。迁移到加密市场时，应保留作者的证据原则，并补充交易所、资金费率、24/7 K 线、合约生命周期和制度漂移。
