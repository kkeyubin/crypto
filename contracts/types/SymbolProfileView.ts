// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type CalculatedAt = string;
export type CoverageEnd = string;
export type CoverageFraction = number;
export type CoverageStart = string;
export type FundingRateMean = number;
export type JumpFrequency = number;
export type MedianHourlyVolume = number;
export type MedianSpreadBps = number;
export type RealizedVolatility = number;
export type SampleCount = number;
export type Symbol = string;

interface SymbolProfileViewShape {
  calculated_at: CalculatedAt;
  coverage_end: CoverageEnd;
  coverage_fraction: CoverageFraction;
  coverage_start: CoverageStart;
  funding_rate_mean: FundingRateMean;
  jump_frequency: JumpFrequency;
  median_hourly_volume: MedianHourlyVolume;
  median_spread_bps: MedianSpreadBps;
  realized_volatility: RealizedVolatility;
  sample_count: SampleCount;
  symbol: Symbol;
}

export type SymbolProfileView = DeepReadonly<SymbolProfileViewShape>;
