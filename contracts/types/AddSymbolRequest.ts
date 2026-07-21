// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type HistoryEnd = string;
export type HistoryStart = string;
export type IncludeAggTrades = boolean;
export type Symbol = string;

interface AddSymbolRequestShape {
  history_end: HistoryEnd;
  history_start: HistoryStart;
  include_agg_trades?: IncludeAggTrades;
  symbol: Symbol;
}

export type AddSymbolRequest = DeepReadonly<AddSymbolRequestShape>;
