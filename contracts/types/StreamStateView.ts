// Generated. Do not edit.

type DeepReadonly<T> =
  T extends (...arguments_: never[]) => unknown
    ? T
    : T extends readonly unknown[]
      ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
      : T extends object
        ? { readonly [Key in keyof T]: DeepReadonly<T[Key]> }
        : T;

export type LastEventAt = string | null;
export type StreamStatus = "connecting" | "connected" | "degraded" | "disconnected";
export type StreamName = string;
export type Symbol = string;
export type UpdatedAt = string;

interface StreamStateViewShape {
  last_event_at: LastEventAt;
  status: StreamStatus;
  stream_name: StreamName;
  symbol: Symbol;
  updated_at: UpdatedAt;
}

export type StreamStateView = DeepReadonly<StreamStateViewShape>;
