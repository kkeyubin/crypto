// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type CheckedAt = string;
export type BackfillRecheckDisposition = "unchanged" | "replacement_planned" | "replacement_exists";
export type JobId = string;
export type ObjectId = string;
export type ObservedChecksum = string;
export type PartitionId = string;
export type PreviousChecksum = string;
export type ReplacementJobId = string | null;
export type ReplacementObjectId = string | null;
export type ArchiveCadence = "daily" | "monthly";
export type ArchiveDataset = "klines" | "mark_price_klines" | "funding_rate" | "agg_trades";
export type Interval = "1m" | null;
export type Kind = "binance_archive";
export type PeriodStart = string;
export type ResolvedUrl = string;
export type Symbol = string;

interface BackfillRecheckViewShape {
  checked_at: CheckedAt;
  disposition: BackfillRecheckDisposition;
  job_id: JobId;
  object_id: ObjectId;
  observed_checksum: ObservedChecksum;
  partition_id: PartitionId;
  previous_checksum: PreviousChecksum;
  replacement_job_id?: ReplacementJobId;
  replacement_object_id?: ReplacementObjectId;
  source: BinanceArchiveSource;
}
export interface BinanceArchiveSource {
  cadence: ArchiveCadence;
  dataset: ArchiveDataset;
  interval?: Interval;
  kind: Kind;
  period_start: PeriodStart;
  resolved_url: ResolvedUrl;
  symbol: Symbol;
}

export type BackfillRecheckView = DeepReadonly<BackfillRecheckViewShape>;
