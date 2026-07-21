// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type ApprovedAt = string | null;
export type Checksum = string;
export type CreatedAt = string;
export type DataType = "kline_1m" | "mark_price" | "funding" | "agg_trade" | "best_bid_ask";
export type End = string;
export type ParquetPath = string;
export type PartitionId = string;
export type RowCount = number;
export type Start = string;
export type DataPartitionStatus = "candidate" | "approved" | "rejected";
export type Symbol = string;
export type Version = number;

interface DataPartitionViewShape {
  approved_at?: ApprovedAt;
  checksum: Checksum;
  created_at: CreatedAt;
  data_type: DataType;
  end: End;
  parquet_path: ParquetPath;
  partition_id: PartitionId;
  row_count: RowCount;
  start: Start;
  status: DataPartitionStatus;
  symbol: Symbol;
  version: Version;
}

export type DataPartitionView = DeepReadonly<DataPartitionViewShape>;
