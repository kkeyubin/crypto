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
export type DeduplicationMethod = "reject_duplicates" | "keep_first" | "keep_last";
export type DuplicatesRemoved = number;
export type End = string;
export type Market = "USD_M_PERPETUAL";
export type Symbol = string;
export type Venue = "BINANCE";
export type ManifestId = string;
export type End1 = string;
export type Start = string;
export type MissingIntervals = MissingInterval[];
export type NormalizationVersion = string;
export type NormalizedChecksum = string;
export type NormalizedPath = string;
/**
 * @minItems 1
 */
export type PrimaryKeyFields = [string, ...string[]];
export type RawPath = string;
export type CompletedAt = string;
export type RepairResult = "repaired" | "partial" | "failed" | "source_pending";
export type RepairSource = "binance_archive" | "binance_rest";
export type StartedAt = string;
export type RepairHistory = RepairRecord[];
export type RetrievedAt = string;
export type RowCount = number;
export type SchemaVersion = "2.0.0";
export type Source = BinanceArchiveSource | BinanceRestSource | BinanceWebSocketSource;
export type ArchiveCadence = "daily" | "monthly";
export type ArchiveDataset = "klines" | "mark_price_klines" | "funding_rate" | "agg_trades";
export type Interval = "1m" | null;
export type Kind = "binance_archive";
export type PeriodStart = string;
export type ResolvedUrl = string;
export type Symbol1 = string;
export type EndTime = number | null;
export type BinanceRestEndpoint = "exchange_info" | "klines" | "mark_price_klines" | "funding_rate" | "agg_trades";
export type FromId = number | null;
export type Interval1 = "1m" | null;
export type Kind1 = "binance_rest";
export type Limit = number;
export type ResolvedUrl1 = string;
export type StartTime = number | null;
export type Symbol2 = string;
export type Kind2 = "binance_websocket";
export type ResolvedUrl2 = string;
export type BinanceStream = "kline_1m" | "mark_price" | "agg_trade" | "best_bid_ask";
export type Symbol3 = string;
export type SourceChecksum = string;
export type Start1 = string;
export type ValidationState = "pending" | "validated" | "rejected";

interface DataManifestShape {
  data_type: DataType;
  deduplication_method: DeduplicationMethod;
  duplicates_removed: DuplicatesRemoved;
  end: End;
  instrument: InstrumentRef;
  manifest_id: ManifestId;
  missing_intervals?: MissingIntervals;
  normalization_version: NormalizationVersion;
  normalized_checksum: NormalizedChecksum;
  normalized_path: NormalizedPath;
  primary_key_fields: PrimaryKeyFields;
  raw_path: RawPath;
  repair_history?: RepairHistory;
  retrieved_at: RetrievedAt;
  row_count: RowCount;
  schema_version: SchemaVersion;
  source: Source;
  source_checksum: SourceChecksum;
  start: Start1;
  validation_state: ValidationState;
}
export interface InstrumentRef {
  market: Market;
  symbol: Symbol;
  venue: Venue;
}
export interface MissingInterval {
  end: End1;
  start: Start;
}
export interface RepairRecord {
  completed_at: CompletedAt;
  result: RepairResult;
  source: RepairSource;
  started_at: StartedAt;
}
export interface BinanceArchiveSource {
  cadence: ArchiveCadence;
  dataset: ArchiveDataset;
  interval?: Interval;
  kind: Kind;
  period_start: PeriodStart;
  resolved_url: ResolvedUrl;
  symbol: Symbol1;
}
export interface BinanceRestSource {
  end_time?: EndTime;
  endpoint: BinanceRestEndpoint;
  from_id?: FromId;
  interval?: Interval1;
  kind: Kind1;
  limit: Limit;
  resolved_url: ResolvedUrl1;
  start_time?: StartTime;
  symbol: Symbol2;
}
export interface BinanceWebSocketSource {
  kind: Kind2;
  resolved_url: ResolvedUrl2;
  stream: BinanceStream;
  symbol: Symbol3;
}

export type DataManifest = DeepReadonly<DataManifestShape>;
