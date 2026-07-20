# Chapter 5: Hypothesis Tests and Confidence Intervals

## Core Idea

假设检验先把“规则无预测力”设为默认，再判断观察表现对该默认是否足够不相容；置信区间表达参数估计的不确定范围，两者都依赖正确的抽样分布。

## Frameworks Introduced

- **Null-versus-alternative test**
  - When to use: 评价一条事前固定规则。
  - How: 定义互斥且穷尽的 H0/H1；选择统计量和显著性阈值；在 H0 下生成抽样分布；计算 p 值；拒绝或保留 H0。
- **Indirect proof under skepticism**
  - When to use: 避免把盈利样本直接当证据。
  - How: 默认 H0，只有结果在 H0 下足够罕见才拒绝。
- **Bootstrap sampling distribution**
  - When to use: 有逐期规则收益且希望检验均值。
  - How: 将收益零中心化以符合 H0；有放回抽取与原样本等长的重采样；计算每轮统计量。
- **Monte Carlo permutation**
  - When to use: 有规则输出和未来市场收益，检验两者配对是否含信息。
  - How: 无放回随机重配输出与市场变化，比较真实配对表现与随机配对分布。

## Key Concepts

- **H0**: 规则期望表现不高于基准，或输出与未来收益随机配对。
- **H1**: 规则表现高于基准。
- **Significance level (alpha)**: 事前容许的 I 类错误率。
- **p-value**: H0 下得到至少同样极端统计量的概率。
- **Type I error**: 无效规则被错误宣称有效。
- **Type II error**: 有效规则未被识别。
- **Power**: 有效规则被正确识别的概率。
- **Confidence interval**: 通过重复抽样程序按规定概率覆盖总体参数的区间。

## Bootstrap versus MCP

| 维度 | Bootstrap | MCP |
|---|---|---|
| 输入 | 逐期规则收益 | 规则输出 + 市场收益 |
| 抽样 | 有放回 | 无放回排列 |
| 单规则 H0 | 规则期望收益为零/以下 | 输出与未来收益配对无信息 |
| 零中心 | 通常需要 | 随机配对自然形成基准 |
| 可否直接处理搜索赢家 | 不可；需 WRC 扩展 | 不可；需多规则最大值扩展 |

## Worked Example

将 1,231 个去趋势日收益先减去规则样本均值，使 H0 下均值为零。每轮有放回抽取 1,231 个收益并计算均值，重复数千次形成 bootstrap 分布。观察均值右侧的尾部面积即 p 值。这个程序只回答“这条预先指定规则是否异常”，若规则是从 6,000 条中挑出，必须转为第 6 章的最大统计量程序。

## Why It Works / Failure Mode

检验把“令人印象深刻”转化为“在明确 H0 下有多罕见”。失败常来自错误 H0、忽略时间依赖、样本过少、查看结果后改变 alpha，或把单规则分布错误用于搜索赢家。

## Anti-patterns

- **p 值误读**: p 值不是 H0 为真的概率，也不是未来盈利概率。
- **未拒绝即证明无效**: 可能只是功效不足。
- **统计显著即经济显著**: 成本后优势可能不足。
- **事后选择单尾/双尾**: 检验方向必须预先确定。

## Key Takeaways

1. H0 是受审目标，但拒绝 H0 不会证明唯一机制。
2. alpha 控制的是长期程序错误率。
3. Bootstrap 和 MCP 检验不同版本的 H0。
4. 置信区间比单一通过/失败提供更多信息。
5. 数据挖掘后的规则必须使用联合校正。

## Connects To

- **Ch 6**: 将随机化检验扩展到搜索后的最佳规则。
- **Ch 9**: 单规则和联合检验在案例中给出相反结论。
