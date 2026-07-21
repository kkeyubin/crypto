// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

type EligibilityViewShape = {
  [k: string]: unknown;
} & {
  eligible: Eligible;
  evaluated_at: EvaluatedAt;
  reason_codes: ReasonCodes;
  symbol: Symbol;
};
export type Eligible = boolean;
export type EvaluatedAt = string;
export type EligibilityReasonCode =
  | "insufficient_history"
  | "stale_live_data"
  | "unrepaired_gap"
  | "insufficient_liquidity"
  | "metadata_unverified"
  | "insufficient_coverage"
  | "data_not_ready"
  | "source_degraded";
export type ReasonCodes = EligibilityReasonCode[];
export type Symbol = string;

export type EligibilityView = DeepReadonly<EligibilityViewShape>;
