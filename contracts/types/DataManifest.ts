// Generated. Do not edit.

export type Checksum = string;
export type DataType = "kline_1m" | "mark_price" | "funding" | "agg_trade" | "best_bid_ask";
export type End = string;
export type Market = "USD_M_PERPETUAL";
export type Symbol = string;
export type Venue = "BINANCE";
export type ManifestId = string;
export type End1 = string;
export type Start = string;
export type MissingIntervals = MissingInterval[];
export type NormalizationVersion = string;
export type CompletedAt = string;
export type Result = string;
export type Source = string;
export type StartedAt = string;
export type RepairHistory = RepairRecord[];
export type RetrievedAt = string;
export type SchemaVersion = string;
export type Start1 = string;

export interface DataManifest {
  checksum: Checksum;
  data_type: DataType;
  end: End;
  instrument: InstrumentRef;
  manifest_id: ManifestId;
  missing_intervals?: MissingIntervals;
  normalization_version: NormalizationVersion;
  repair_history?: RepairHistory;
  retrieved_at: RetrievedAt;
  schema_version: SchemaVersion;
  start: Start1;
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
  result: Result;
  source: Source;
  started_at: StartedAt;
}
