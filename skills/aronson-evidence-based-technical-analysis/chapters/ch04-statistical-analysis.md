# Chapter 4: Statistical Analysis

## Core Idea

统计推断是在无法观察完整总体时，用样本统计量估计总体参数并量化抽样不确定性；单次回测数字没有自己的可靠度，必须放进抽样分布理解。

## Frameworks Introduced

- **Six elements of a statistical investigation**
  - When to use: 设计策略实证研究。
  - How: 明确总体、总体参数、样本、抽样方法、样本统计量和抽样分布。
- **Sample-to-population inference**
  - When to use: 从历史规则收益推断未来期望表现。
  - How: 把历史收益视为来自目标总体的样本，估计统计量的偏差、标准误差与分布。
- **Frequency-area equivalence**
  - When to use: 解读概率分布、p 值和置信区间。
  - How: 将分布某区域的面积理解为重复抽样中统计量落入该区域的相对频率。

## Key Concepts

- **Population**: 研究希望推断的全部可能观察，而非数据库中的全部行。
- **Parameter**: 总体的固定但未知特征，例如规则未来期望均值。
- **Sample**: 从总体获得的有限观察。
- **Statistic**: 由样本计算的量，例如回测平均收益。
- **Sampling distribution**: 重复取样时统计量的可能分布。
- **Standard error**: 统计量抽样分布的离散程度。
- **Central limit effect**: 在一定条件和足够样本量下，均值的抽样分布趋近正态。
- **Dependence**: 金融收益相关性会减少有效独立观察数并改变抽样分布。

## Worked Example

作者用盒中灰色和非灰色珠子的比例说明推断。同一总体反复抽取 20 颗珠子，每个样本的灰珠比例不同；单次得到 0.65 不等于总体比例就是 0.65。把许多样本比例画成分布，才能理解某个观察值偏离真实总体比例多远。交易研究中，历史平均收益相当于一次样本比例，未来重复市场历史不可直接获得，因此需理论或重采样近似抽样分布。

## Reference Checklist

| 元素 | 交易规则示例 |
|---|---|
| Population | 冻结规则在目标制度下所有可能未来期间的净收益 |
| Parameter | 期望平均超额收益 |
| Sample | 指定历史区间的逐期可实现收益 |
| Sampling method | 时间序列产生过程或重采样假设 |
| Statistic | 均值、Sharpe、最大回撤等 |
| Sampling distribution | H0 或估计模型下统计量的分布 |

## Anti-patterns

- **把数据库等同于总体**: 历史只是可能市场路径的一次实现。
- **只报点估计**: 没有标准误差或区间，无法判断随机波动。
- **假定每根 K 线独立**: 自相关和波动聚集会让样本量看起来比实际更大。
- **先选统计量再解释**: 看到结果后挑最好看的指标增加搜索偏差。

## Key Takeaways

1. 样本统计量会随机波动。
2. 推断质量依赖样本产生机制，而不只依赖样本大小。
3. 分布尾部面积提供“结果有多异常”的尺度。
4. 金融时间序列依赖必须进入抽样模型。
5. 在回测前定义目标总体和总体参数。

## Connects To

- **Ch 5**: 使用抽样分布进行假设检验和区间估计。
- **Ch 6**: 搜索最大值需要不同于单均值的抽样分布。
