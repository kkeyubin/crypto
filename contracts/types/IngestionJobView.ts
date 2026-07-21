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
export type DataType = "kline_1m" | "mark_price" | "funding" | "agg_trade" | "best_bid_ask";
export type JobId = string;
export type RequestedEnd = string;
export type RequestedStart = string;
export type IngestionJobStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";
export type Symbol = string;
export type UpdatedAt = string;

interface IngestionJobViewShape {
  created_at: CreatedAt;
  data_type: DataType;
  job_id: JobId;
  requested_end: RequestedEnd;
  requested_start: RequestedStart;
  status: IngestionJobStatus;
  symbol: Symbol;
  updated_at: UpdatedAt;
}

export type IngestionJobView = DeepReadonly<IngestionJobViewShape>;
