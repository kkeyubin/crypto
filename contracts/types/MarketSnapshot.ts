// Generated. Do not edit.

export type Close = number;
export type High = number;
export type Low = number;
export type Open = number;
export type Timestamp = string;
export type Volume = number;
export type Bars = OHLCVBar[];
export type Ask = number;
export type Bid = number;
export type Timestamp1 = string;
export type Cutoff = string;
export type DataManifestId = string;
export type DeterministicSignalId = string | null;
export type Rate = number;
export type Timestamp2 = string;
export type Market = "USD_M_PERPETUAL";
export type Symbol = string;
export type Venue = "BINANCE";
export type SchemaVersion = string;
export type SnapshotId = string;
export type StrategySpecHash = string;

export interface MarketSnapshot {
  bars: Bars;
  best_bid_ask?: BestBidAsk | null;
  cutoff: Cutoff;
  data_manifest_id: DataManifestId;
  deterministic_signal_id?: DeterministicSignalId;
  funding?: FundingObservation | null;
  instrument: InstrumentRef;
  schema_version?: SchemaVersion;
  snapshot_id: SnapshotId;
  strategy_spec_hash: StrategySpecHash;
}
export interface OHLCVBar {
  close: Close;
  high: High;
  low: Low;
  open: Open;
  timestamp: Timestamp;
  volume: Volume;
}
export interface BestBidAsk {
  ask: Ask;
  bid: Bid;
  timestamp: Timestamp1;
}
export interface FundingObservation {
  rate: Rate;
  timestamp: Timestamp2;
}
export interface InstrumentRef {
  market: Market;
  symbol: Symbol;
  venue: Venue;
}
