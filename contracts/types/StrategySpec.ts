// Generated. Do not edit.

export type DollarValue = number | null;
export type Interval = string | null;
export type BarKind = "time" | "event";
export type TradeCount = number | null;
export type Volume = number | null;
export type ContentHash = string;
export type Benchmark = string;
export type MultipleTesting = string;
export type TestEnd = string;
export type TrainEnd = string;
export type ValidationEnd = string;
export type CollisionPolicy = string;
export type FillTiming = string;
export type FundingIncluded = boolean;
export type LatencyMs = number;
export type MakerFeeBps = number;
export type OrderType = string;
export type SignalSource = string;
export type SlippageBps = number;
export type SpreadBps = number;
export type TakerFeeBps = number;
export type Author = string;
export type Name = string;
export type StrategyState = "draft" | "reviewed" | "frozen" | "backtested" | "paper_enabled" | "retired";
export type Version = string;
export type Market = "USD_M_PERPETUAL";
export type Symbol = string;
export type Venue = "BINANCE";
export type StrategyMode = "executable" | "observation";
export type Expression = string;
export type Name1 = string;
export type SourceSection = string;
export type NisonContext = NisonCondition[];
/**
 * @minItems 2
 */
export type Provenance = [ProvenanceRef, ProvenanceRef, ...ProvenanceRef[]];
export type Section = string;
export type Skill = string;
export type MaxDailyLossFraction = number;
export type MaxDrawdownFraction = number;
export type MaxLeverage = number;
export type RiskFraction = number;
export type StaleDataBlocksEntries = boolean;
export type SchemaVersion = string;
/**
 * @minItems 3
 */
export type Chronology = [string, string, string, ...string[]];
export type ClearPath = string;
export type StrategyFamily = "BB" | "RB" | "DD" | "FB" | "SB" | "IRB" | "ARB";
export type FrozenSignalLine = string;
export type Invalidation = string;
export type Trigger = string;

export interface StrategySpec {
  bar: BarSpec;
  content_hash: ContentHash;
  evidence: EvidencePlan;
  execution?: ExecutionSpec | null;
  identity: StrategyIdentity;
  instrument: InstrumentRef;
  mode: StrategyMode;
  nison_context?: NisonContext;
  parameters: ParameterFamily;
  provenance: Provenance;
  risk?: RiskSpec | null;
  schema_version?: SchemaVersion;
  volman: VolmanRules;
}
export interface BarSpec {
  dollar_value?: DollarValue;
  interval?: Interval;
  kind: BarKind;
  trade_count?: TradeCount;
  volume?: Volume;
}
export interface EvidencePlan {
  benchmark: Benchmark;
  multiple_testing: MultipleTesting;
  test_end: TestEnd;
  train_end: TrainEnd;
  validation_end: ValidationEnd;
}
export interface ExecutionSpec {
  collision_policy?: CollisionPolicy;
  fill_timing?: FillTiming;
  funding_included?: FundingIncluded;
  latency_ms?: LatencyMs;
  maker_fee_bps?: MakerFeeBps;
  order_type?: OrderType;
  signal_source?: SignalSource;
  slippage_bps?: SlippageBps;
  spread_bps?: SpreadBps;
  taker_fee_bps?: TakerFeeBps;
}
export interface StrategyIdentity {
  author?: Author;
  name: Name;
  state?: StrategyState;
  version: Version;
}
export interface InstrumentRef {
  market: Market;
  symbol: Symbol;
  venue: Venue;
}
export interface NisonCondition {
  expression: Expression;
  name: Name1;
  source_section: SourceSection;
}
export interface ParameterFamily {
  fixed: Fixed;
  search_space: SearchSpace;
}
export interface Fixed {
  [k: string]: boolean | number | string;
}
export interface SearchSpace {
  [k: string]: (boolean | number | string)[];
}
export interface ProvenanceRef {
  section: Section;
  skill: Skill;
}
export interface RiskSpec {
  max_daily_loss_fraction?: MaxDailyLossFraction;
  max_drawdown_fraction?: MaxDrawdownFraction;
  max_leverage?: MaxLeverage;
  risk_fraction?: RiskFraction;
  stale_data_blocks_entries?: StaleDataBlocksEntries;
}
export interface VolmanRules {
  chronology: Chronology;
  clear_path: ClearPath;
  family: StrategyFamily;
  frozen_signal_line: FrozenSignalLine;
  invalidation: Invalidation;
  trigger: Trigger;
}
