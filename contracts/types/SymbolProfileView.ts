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
export type CoverageStart = string;
export type CoverageFraction = number;
export type SampleCount = number;
export type Value = number | null;
export type Symbol = string;

interface SymbolProfileViewShape {
  calculated_at: CalculatedAt;
  coverage_end: CoverageEnd;
  coverage_start: CoverageStart;
  funding_rate_mean: ProfileMetricView;
  jump_frequency: ProfileMetricView;
  median_hourly_volume: ProfileMetricView;
  median_spread_bps: ProfileMetricView;
  realized_volatility: ProfileMetricView;
  symbol: Symbol;
}
export interface ProfileMetricView {
  coverage_fraction: CoverageFraction;
  sample_count: SampleCount;
  value: Value;
}

export type SymbolProfileView = DeepReadonly<SymbolProfileViewShape>;
