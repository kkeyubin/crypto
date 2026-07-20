# 《Forex Price Action Scalping》拆解分析

## 书籍定位

- **作者**：Bob Volman
- **结构**：17 章，分为基础、入场、交易管理、交易筛选和账户管理五部分
- **原始市场**：EUR/USD
- **原始图表**：70-tick chart，单图约显示 1.5 小时价格活动，唯一指标为 20 EMA
- **原始交易模板**：固定 10 pip 目标；初始保护上限 10 pip，技术止损通常约 6–7 pip；止损只能向目标方向调整
- **本项目用途**：提供可客观化的超短线价格结构候选，不把作者的经验性判断直接视为已验证策略

## 中心思想

Volman 的方法不是“识别七个形态就交易”，而是一个三层决策系统：

1. **Context**：多空哪一方正在施压，目标方向是否有 clear path；
2. **Setup and trigger**：等待价格压缩、回调或区间结构形成，并只在 signal line 被实际突破时入场；
3. **Trade validity**：用动态 tipping point 判断持仓是否仍有效，而不是根据浮盈亏或情绪退出。

七个 setup 只是进入已判断为 favorable market 的工具。作者反复强调，setup 本身意义很小；不利背景足以否决外形标准的信号。

## 原始环境与迁移风险

70-tick bar 是每 70 笔交易形成一根 K 线，不是 70 秒。外汇为分散市场，不同数据商的 tick 数本来就不完全一致，作者允许用约 65–75 tick 形成近似图表。这说明“70”不是自然常数，而是为特定数据源和市场节奏选择的观察尺度。

迁移到 BTC 时不能照搬：

- BTC 逐笔成交密度远高于 2011 年零售 EUR/USD 数据；
- 交易所可能把撮合、聚合成交或 websocket trade 定义为不同“tick”；
- 1 pip 点差、10 pip 目标、3 pip 小 bar、6–7 pip 止损、00/50 round-number zone 均失去原尺度含义；
- 1 分钟 OHLC 无法可靠决定同根内入场、止损、目标的先后。

因此应先比较时间 bar、固定成交笔数 bar、固定成交量 bar 和 dollar bar，再用 tick/bps/局部波动率单位重新定义几何阈值。

## 七类入场 Setup

### 1. Double Doji Break（DD）

用于趋势或新生趋势的回调末端。价格通常回撤近期 swing 的约 40%–60%，来到 20 EMA 区域，出现至少两根相邻小 bar/doji 表达犹豫。作者把通常不超过 3 pip 的小 candle 也视为 doji。趋势方向极值最远的 bar 是 signal bar；后续 entry bar 突破该极值时顺势入场。

关键过滤：趋势仍有效、回调最好斜向且较单向、setup 后至 10 pip 目标没有近端聚集阻力。若小 bar 的趋势侧极值相距过大，弱趋势中应跳过。

### 2. First Break（FB）

用于新生强趋势中的第一次大幅回调。三个条件：

1. 趋势是快速、单向、最好从盘整区射出的 surge；
2. 第一次回调本身明确而连续，通常回撤约 40%–60%，靠近 20 EMA 区域；
3. 这是该新趋势的第一次回调。

入场是回调中第一根被趋势方向突破的 signal bar。作者明确指出多数 FB 应跳过，只有强 surge + 有力首次回调的 fishhook 结构才允许激进介入。

### 3. Second Break（SB）

用于普通趋势回调。第一次顺势 break 通常不交易；若其缺乏延续，反向交易者再次推动回调，但整体趋势压力仍在，第二次顺势攻击突破新的 signal bar，则形成 SB。

SB 是事件序列，不是单根形态：`trend → pullback → first break → renewed counterpressure → second break`。回调最好有序、斜向，前方阻力有限。

### 4. Block Break（BB）

多用途 setup：数根 bar 被压在狭窄垂直范围，顶部和底部通常各有多次触碰，形成微型 range/box。方向不能由 box 单独决定；只在既有价格压力所指的 path of least resistance 一侧突破 box signal line 时入场。

BB 可出现在趋势回调、剧烈突破后无回调的延续、区间内部、顶部/底部反转背景。理想状态下边界前还有更小的 pre-breakout tension。

### 5. Range Break（RB）

针对较长的横盘区间，理想边界至少有两个近似相等高点和两个低点。区间最终必然破裂，但不能交易任何首次越界。优质 RB 要在边界前积累 pre-breakout tension：小 bar 逐渐贴近 signal line，另一方无法推回。

主要陷阱：

- **False break**：价格从区间另一侧直接冲过边界，几乎没有 buildup，容易耗尽后反转；
- **Tease break**：轻微探出边界但没有充分准备，诱使过早入场。

00/50 round-number zones 在原市场常参与区间形成，但迁移时必须重新定义。

### 6. Inside Range Break（IRB）

IRB 并不只是“提前押注区间突破”。它是在大 range 内出现的小 block，可有三种用途：

- 在区间上沿形成向下 break，利用 boomerang 回到下沿；
- 在区间下沿形成向上 break，利用 boomerang 回到上沿；
- 在足够宽的区间中部，把小 block 当普通 BB 交易。

做边界间 boomerang 时，大区间扣除 setup 宽度后仍需为 10 pip 目标留出 clear path，原书通常意味着约 20 pip 或更宽的区间。

### 7. Advanced Range Break（ARB）

用于区间已经越界、但没有形成标准 RB 的情况，有两个版本：

1. **Post-break cluster**：数根 bar 停滞在已破边界附近但未证明突破失败，形成新 box；其 signal line 位于区间外；
2. **Breakout pullback**：初次突破后回踩原边界，边界成功转换为支撑/阻力，再顺突破方向击穿 signal bar。

ARB 与 RB 的区别主要是入场位置和压力形成过程：RB 的 signal line 就是原区间边界；ARB 的触发线位于区间外，来自越界后的压缩或回踩。

## 共同价格行为框架

### Signal bar / Entry bar

Signal bar 是 setup 中趋势或交易方向极值最关键的 bar；后续 entry bar 真正越过该极值才触发。提前预测突破、看到即下单违反原方法。

### 20 EMA

20 EMA 是视觉辅助，不是机械支撑/阻力。强趋势中它可能滞后而够不到，快速回调也可能明显穿越。回测应把它作为候选上下文变量，而不是未经检验的硬门槛。

### Pre-breakout tension

突破前的小 bar、相等极值、逐步贴近边界和回撤受限，表达一方无法把价格推离 signal line。它降低直接冲撞边界后耗尽的风险，但不能保证突破成功。

### Clear path / Chart resistance

入场到目标之间不能被近端价格聚集、显著前高低、range barrier 或 round-number zone 阻挡。作者建议大部分时间观察，只有背景和路径均支持时参与。

## Trade Management：Tipping Point

作者把持仓有效性绑定到一个技术价位：初始 tipping point 通常在 signal bar 或结构高低点外一 pip。若被突破，立即 scratch；未突破则继续持有。随着交易朝目标发展，可用新的结构价位替代旧 tipping point，但止损只能向目标方向移动，不能扩大。

目标固定 10 pip，不因贪婪调整。这个非对称设计让策略避免因情绪任意止盈，又能在结构失效时缩小平均损失。迁移回测必须明确：哪个已完成 bar 产生新 tipping point、何时生效、同一事件内 target 与 stop 的排序。

## Unfavorable Conditions

交易否决优先于 setup 识别。主要不利条件包括：

- 入场后立即面对明显聚集区或强支撑/阻力；
- setup 与总体方向压力不一致；
- 没有 pre-breakout tension 的直接冲撞；
- narrow range 没有足够目标空间；
- round-number zone 被多次明确守住；
- 信号结构过宽，使技术止损或 scratch 成本过大；
- 市场混乱，无法说明 path of least resistance。

跳过后价格仍可能到达目标，这不证明跳过错误；作者以概率过程而非单笔结果评价决定。

## Account Management

原书举例以 10 pip 最大止损对应约 2% 账户风险，并强调按账户变化调整交易量以复利，同时警告新手在技术和心理尚未稳定时应极小仓位。书中关于快速倍增账户的算术是情景演示，不是收益承诺；对加密实盘应采用更保守的风险预算、日内损失上限、杠杆与爆仓约束。

## 客观化与回测建议

### 数据对象

固定交易所、spot/perpetual、trade 聚合定义、事件 bar 生成算法、手续费等级和数据版本。事件 bar 只在第 N 笔成交到达后完成；不得使用最终 bar 极值提前下单。

### 尺度无关特征

将 3/6/7/10 pip、1 pip break 和 round-number 距离转成候选尺度：tick size、bps、rolling median true range 或短期 ATR。所有阈值应预注册并由 Aronson Skill 管理搜索偏差。

### 状态机

每个 setup 分为：

```text
market context -> structure formation -> signal line frozen
-> breakout event -> executable fill -> tipping-point updates -> exit
```

不能事后框选范围或重画边界；边界在每个时点只能由过去数据产生。

### 标签与人工判断

原书包含“strong trend”“orderly pullback”“clear path”“favorable”等经验概念。可采用两阶段研究：先制定多人盲标注协议测一致性，再训练/编码代理特征。低一致性概念不能假装成唯一客观规则。

## 与其他书的组合价值

- **尼森 Skill**：提供 K 线背景、确认、止损和替代图表框架；
- **Aronson Skill**：要求 Volman 的所有视觉概念可证伪、登记完整参数搜索并进行联合校正；
- **Volman Skill**：提供更细粒度的 setup、突破压力、路径阻力和动态 trade validity 候选。

Volman 负责提出“可能有用的微观价格行为假设”，Aronson 决定这些假设何时才有资格被称为证据。

## 局限

本书主要依赖作者经验和大量精选图表，没有提供符合现代标准的完整统计验证。其原始环境、点差、数据源和市场结构已变化。生成 Skill 的目的应是忠实保存候选框架并帮助规格化研究，而不是背书其盈利能力。
