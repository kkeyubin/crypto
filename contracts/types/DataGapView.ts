// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type DataType = "kline_1m" | "mark_price" | "funding" | "agg_trade" | "best_bid_ask";
export type End = string;
export type GapId = string;
export type OpenedAt = string;
export type Reason = string;
export type RepairedAt = string | null;
export type Start = string;
export type DataGapStatus = "open" | "repaired";
export type Symbol = string;

interface DataGapViewShape {
  data_type: DataType;
  end: End;
  gap_id: GapId;
  opened_at: OpenedAt;
  reason: Reason;
  repaired_at?: RepairedAt;
  start: Start;
  status: DataGapStatus;
  symbol: Symbol;
}

export type DataGapView = DeepReadonly<DataGapViewShape>;
