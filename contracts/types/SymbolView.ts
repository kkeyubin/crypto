// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type CreatedAt = string;
export type SymbolDataStatus = "requested" | "backfilling" | "data_ready" | "degraded" | "failed" | "disabled";
export type Enabled = boolean;
export type HistoryEnd = string;
export type HistoryStart = string;
export type IncludeAggTrades = boolean;
export type MetadataStatus = "metadata_unverified" | "profile_building" | "eligible" | "ineligible";
export type Symbol = string;
export type UpdatedAt = string;

interface SymbolViewShape {
  created_at: CreatedAt;
  data_status: SymbolDataStatus;
  enabled: Enabled;
  history_end: HistoryEnd;
  history_start: HistoryStart;
  include_agg_trades: IncludeAggTrades;
  metadata_status: MetadataStatus;
  symbol: Symbol;
  updated_at: UpdatedAt;
}

export type SymbolView = DeepReadonly<SymbolViewShape>;
