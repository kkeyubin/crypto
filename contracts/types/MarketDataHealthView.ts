// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type ArchiveHealthy = boolean;
export type CheckedAt = string;
export type RestHealthy = boolean;
export type SourceMode = "direct" | "proxy" | "degraded";
export type LastEventAt = string | null;
export type StreamStatus = "connecting" | "connected" | "degraded" | "disconnected";
export type StreamName = string;
export type Symbol = string;
export type UpdatedAt = string;
export type Streams = StreamStateView[];
export type WorkerHeartbeatAt = string | null;

interface MarketDataHealthViewShape {
  archive_healthy: ArchiveHealthy;
  checked_at: CheckedAt;
  rest_healthy: RestHealthy;
  source_mode: SourceMode;
  streams: Streams;
  worker_heartbeat_at: WorkerHeartbeatAt;
}
export interface StreamStateView {
  last_event_at: LastEventAt;
  status: StreamStatus;
  stream_name: StreamName;
  symbol: Symbol;
  updated_at: UpdatedAt;
}

export type MarketDataHealthView = DeepReadonly<MarketDataHealthViewShape>;
