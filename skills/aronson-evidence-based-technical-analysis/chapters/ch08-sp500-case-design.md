# Chapter 8: Case Study of Rule Data Mining for the S&P 500

## Core Idea

案例把前七章变成一项可审计实验：预先定义数据、目标参数、规则生成语言、完整 6,402 条候选宇宙和联合检验，再让结果决定是否存在证据。

## Frameworks Introduced

- **Statistical case-study contract**
  - When to use: 设计大规模策略搜索。
  - How: 写明总体、参数、样本、统计量、H0/H1、显著性水平和实用意义。
- **Combinatorial enumeration**
  - When to use: 避免只测试既往成功参数造成 data snooping。
  - How: 预先固定操作符和参数集合，生成集合内所有组合并保留总数。
- **Indicator scripting language (ISL)**
  - When to use: 系统表达指标候选。
  - How: 用可嵌套时间序列算子组合原始数据，再由规则算子映射为仓位。
- **Theme-to-rule translation**
  - When to use: 将传统 TA 思想客观化。
  - How: 把趋势、极值、背离分别转成明确的时间序列、窗口、阈值和输出。

## Case Design

| 元素 | 定义 |
|---|---|
| Target market | S&P 500 指数相关日线数据 |
| Rule universe | 6,402 条简单规则 |
| Themes | 趋势、极值、背离 |
| Output | 多/空反转状态 |
| Statistic | 去趋势数据上的平均年化规则收益 |
| H0 | 所有 6,402 条规则均无预测力 |
| H1 | 至少一条规则有正预测力 |
| Alpha | 0.05 |
| Execution | 收盘生成状态，下一日开盘执行 |

## Key Operators

- **n-period breakout**: 与过去 n 期极值比较，表达趋势突破。
- **Moving averages**: 平滑输入但引入滞后；简单、加权等版本是不同操作符。
- **Channel normalization**: 依据滚动高低区间把序列缩放到统一范围。
- **Indicator expressions**: 嵌套算子形成不同指标时间序列。
- **Threshold logic**: 将连续指标转成多/空状态。

## Worked Example

趋势规则不是挑一个著名回看期，而是在预先列出的时间序列与全部回看参数上组合。例如 11 个突破回看值与 39 条时间序列产生 429 个候选。这样能记录真实搜索规模，也避免只因既往研究说某参数成功就偷偷缩小宇宙。所有候选都进入相同数据、执行和联合检验流程。

## Research Choices and Limits

- 不计交易成本，因为主要命题是信号是否含预测信息，而非规则能否独立实盘；可交易性不能沿用该省略。
- 仅测试简单规则，未测试更可能表达复杂市场的组合模型。
- 仅测试多空反转，未允许中性状态。
- 仅使用 S&P 500，不能外推到其他资产。

## Anti-patterns

- **只枚举自己喜欢的参数**: 隐藏搜索边界并增加先验筛选偏差。
- **更换市场因为听说更有效**: 使用既往结果选标的属于 prior-research snooping。
- **不同规则用不同执行假设**: 破坏公平比较。
- **没有保存逐期输出/收益**: 无法执行 WRC 或多规则 MCP。

## Key Takeaways

1. 先定义研究宇宙，再看结果。
2. 参数网格属于假设族，而不是单一规则。
3. 操作符的平滑收益伴随滞后代价。
4. 统计显著与实际交易成本是两个命题。
5. 案例的限制必须进入结果解释。

## Connects To

- **Ch 1**: 使用去趋势和可实现执行。
- **Ch 6**: 用完整候选信息校正数据挖掘偏差。
- **Ch 9**: 报告联合检验结果和局限。
