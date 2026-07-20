# Chapter 9: Case Study Results and the Future of TA

## Core Idea

6,402 条规则中观察最佳者在普通单规则检验下极其显著，但在重放完整搜索的 WRC 和 MCP 下完全不显著；技术分析的未来应由人提出信息丰富的候选、机器客观组合与淘汰。

## Results

| 项目 | 数值 |
|---|---|
| 规则数量 | 6,402 |
| 最佳规则 | E-12-28-10-30 |
| 最佳去趋势年化均值 | 10.25% |
| 错误单规则 p 值 | 0.0005 |
| WRC p 值 | 0.8164 |
| MCP p 值 | 0.8194 |
| 普通 0.05 检验下看似显著 | 约 320 条 |
| 联合校正后显著 | 0 条 |

## Frameworks Introduced

- **Correct null center for the winner**
  - When to use: 解读搜索赢家。
  - How: 最大均值分布通常位于正值；本案例 6,402 条无效规则中赢家的 H0 中心约为 11%，不是 0%。
- **Research critique matrix**
  - When to use: 报告实验结果。
  - How: 分别列出基准、去趋势、前视、挖掘偏差、snooping 控制，以及规则复杂度、状态空间和市场范围局限。
- **Human-computer partnership**
  - When to use: 构建下一代 TA 研究流程。
  - How: 人负责目标、领域知识、指标构造；计算机负责一致搜索、变量组合、记录和统计淘汰。

## Worked Example

若忽略搜索过程，最佳 10.25% 收益相对零中心单规则分布的 `p=0.0005`，看起来几乎不可能由运气造成。但研究实际问的是“6,402 条无效规则中最大收益能有多高”。WRC 与 MCP 的最大值分布中心约 11%，10.25% 甚至低于中心，因此 `p≈0.82`。同一数字的证据意义完全由正确的研究问题和抽样分布决定。

## Positive Attributes

- 使用与无预测力规则匹配的基准；
- 通过去趋势控制市场净趋势与仓位偏置；
- 收盘信号在下一日开盘执行，避免前视；
- 用 WRC 与 MCP 控制完整数据挖掘偏差；
- 通过预先组合枚举降低 prior-research snooping。

## Negative Attributes

- 未测试复杂规则；
- 只测试永久多/空反转，未允许中性状态；
- 只研究 S&P 500；
- 指标构造可能没有充分表达趋势、极值和背离；
- 研究预测信息时未计成本，因此结果不能直接回答可交易性。

## Future-of-TA Mental Model

人擅长发明、提出问题和构造具有领域意义的特征，却易受信念与情绪影响；计算机不擅长原创问题，但能稳定处理高维组合、删除弱变量和保存一致证据。两者的正确分工是：

```text
human: problem + target + candidate features
machine: exhaustive accountable evaluation + inference
human: interpret within preregistered limits
```

## Anti-patterns

- **把无显著规则写成技术分析永远无效**: 结论仅覆盖该市场、样本和候选宇宙。
- **因结果不理想改用已知成功市场**: 会引入 prior-research snooping。
- **把复杂模型当自动解决方案**: 维度灾难要求观测数随维度快速增加。
- **让专家事后覆盖模型**: 主观判断在重复预测任务中往往不如简单统计模型一致。

## Key Takeaways

1. 搜索赢家的零假设中心可以显著高于零。
2. 普通 p 值会把预期数量的假阳性误报为发现。
3. 失败实验也能强力证明研究方法的重要性。
4. 结论必须严格限制在研究宇宙内。
5. 人机协作的核心是创意与客观裁决分工。

## Connects To

- **Ch 6**: 最大值分布和联合校正的实证应用。
- **Ch 7**: 理论指导未来指标构造，但仍需证据。
