// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

/**
 * @minItems 1
 */
export type DataTypes = [DataType, ...DataType[]];
export type DataType = "kline_1m" | "mark_price" | "funding" | "agg_trade" | "best_bid_ask";
export type End = string;
export type IncludeAggTrades = boolean;
export type Start = string;
export type Symbol = string;

interface BackfillRequestShape {
  data_types: DataTypes;
  end: End;
  include_agg_trades?: IncludeAggTrades;
  start: Start;
  symbol: Symbol;
}

export type BackfillRequest = DeepReadonly<BackfillRequestShape>;
