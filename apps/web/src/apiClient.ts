import type {
  DataGapView,
  DataPartitionView,
  EligibilityView,
  IngestionJobView,
  MarketDataHealthView,
  ProfileMetricView,
  StreamStateView,
  SymbolProfileView,
  SymbolView,
} from "./contracts";

export class ApiClientError extends Error {
  constructor(public readonly kind: "request" | "response", public readonly status?: number) {
    super(kind === "request" ? "API request failed" : "API response was invalid");
    this.name = "ApiClientError";
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const symbolPattern = /^[A-Z0-9]{3,32}$/;
const symbolAliases = new Map([
  ["PEPE", "1000PEPEUSDT"],
  ["PEPEUSDT", "1000PEPEUSDT"],
]);
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const dataStatuses = new Set(["requested", "backfilling", "data_ready", "degraded", "failed", "disabled"]);
const metadataStatuses = new Set(["metadata_unverified", "profile_building", "eligible", "ineligible"]);
const dataTypes = new Set(["kline_1m", "mark_price", "funding", "agg_trade", "best_bid_ask"]);
const partitionStatuses = new Set(["candidate", "approved", "rejected"]);
const gapStatuses = new Set(["open", "repaired"]);
const streamStatuses = new Set(["connecting", "connected", "degraded", "disconnected"]);
const sourceModes = new Set(["direct", "proxy", "degraded"]);
const eligibilityReasons = new Set([
  "insufficient_history",
  "stale_live_data",
  "unrepaired_gap",
  "insufficient_liquidity",
  "metadata_unverified",
  "insufficient_coverage",
  "data_not_ready",
  "source_degraded",
]);
const jobStatuses = new Set(["queued", "running", "succeeded", "failed", "cancelled"]);

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isNullableString(value: unknown): value is string | null | undefined {
  return value === null || value === undefined || isString(value);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function isNonNegativeNumber(value: unknown): value is number {
  return isFiniteNumber(value) && value >= 0;
}

function isFraction(value: unknown): value is number {
  return isFiniteNumber(value) && value >= 0 && value <= 1;
}

function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value);
  return actual.length === keys.length && actual.every((key) => keys.includes(key));
}

function isProfileMetric(value: unknown, allowNegative: boolean): value is ProfileMetricView {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, ["value", "sample_count", "coverage_fraction"]) ||
    !Number.isInteger(value.sample_count) ||
    !isFiniteNumber(value.sample_count) ||
    value.sample_count < 0 ||
    !isFraction(value.coverage_fraction)
  ) {
    return false;
  }
  if (value.sample_count === 0) {
    return value.value === null && value.coverage_fraction === 0;
  }
  return (
    isFiniteNumber(value.value) &&
    (allowNegative || value.value >= 0) &&
    value.coverage_fraction > 0
  );
}

function isSymbolView(value: unknown): value is SymbolView {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.symbol === "string" &&
    symbolPattern.test(value.symbol) &&
    typeof value.enabled === "boolean" &&
    typeof value.history_start === "string" &&
    typeof value.history_end === "string" &&
    typeof value.include_agg_trades === "boolean" &&
    typeof value.data_status === "string" &&
    dataStatuses.has(value.data_status) &&
    typeof value.metadata_status === "string" &&
    metadataStatuses.has(value.metadata_status) &&
    typeof value.created_at === "string" &&
    typeof value.updated_at === "string"
  );
}

async function getJson(path: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(path, { headers: { Accept: "application/json" }, signal });
  if (!response.ok) {
    throw new ApiClientError("request", response.status);
  }
  try {
    return await response.json() as unknown;
  } catch {
    throw new ApiClientError("response");
  }
}

async function postJson(path: string, body: unknown, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(path, {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!response.ok) {
    throw new ApiClientError("request", response.status);
  }
  try {
    return await response.json() as unknown;
  } catch {
    throw new ApiClientError("response");
  }
}

async function deleteJson(path: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(path, {
    method: "DELETE",
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new ApiClientError("request", response.status);
  }
  try {
    return await response.json() as unknown;
  } catch {
    throw new ApiClientError("response");
  }
}

export async function listSymbols(signal?: AbortSignal): Promise<readonly SymbolView[]> {
  const payload = await getJson("/api/symbols?limit=100&offset=0", signal);
  if (!Array.isArray(payload) || !payload.every(isSymbolView)) {
    throw new ApiClientError("response");
  }
  return payload;
}

function isPartition(value: unknown): value is DataPartitionView {
  if (!isRecord(value)) {
    return false;
  }
  return (
    isString(value.partition_id) &&
    isString(value.symbol) && symbolPattern.test(value.symbol) &&
    isString(value.data_type) && dataTypes.has(value.data_type) &&
    isString(value.start) && isString(value.end) &&
    isString(value.parquet_path) && !value.parquet_path.startsWith("/") &&
    !value.parquet_path.split("/").includes("..") &&
    isString(value.checksum) && /^[0-9a-f]{64}$/.test(value.checksum) &&
    Number.isInteger(value.row_count) && isFiniteNumber(value.row_count) && value.row_count > 0 &&
    Number.isInteger(value.version) && isFiniteNumber(value.version) && value.version > 0 &&
    isString(value.status) && partitionStatuses.has(value.status) &&
    isString(value.created_at) && isNullableString(value.approved_at)
  );
}

function isGap(value: unknown): value is DataGapView {
  if (!isRecord(value)) {
    return false;
  }
  return (
    isString(value.gap_id) &&
    isString(value.symbol) && symbolPattern.test(value.symbol) &&
    isString(value.data_type) && dataTypes.has(value.data_type) &&
    isString(value.start) && isString(value.end) &&
    isString(value.reason) && value.reason.length > 0 &&
    isString(value.status) && gapStatuses.has(value.status) &&
    isString(value.opened_at) && isNullableString(value.repaired_at)
  );
}

function isProfile(value: unknown): value is SymbolProfileView {
  if (!isRecord(value)) {
    return false;
  }
  return (
    hasExactKeys(value, [
      "symbol",
      "calculated_at",
      "coverage_start",
      "coverage_end",
      "realized_volatility",
      "jump_frequency",
      "median_spread_bps",
      "median_hourly_volume",
      "funding_rate_mean",
    ]) &&
    isString(value.symbol) && symbolPattern.test(value.symbol) &&
    isString(value.calculated_at) && isString(value.coverage_start) && isString(value.coverage_end) &&
    isProfileMetric(value.realized_volatility, false) &&
    isProfileMetric(value.jump_frequency, false) &&
    isProfileMetric(value.median_spread_bps, false) &&
    isProfileMetric(value.median_hourly_volume, false) &&
    isProfileMetric(value.funding_rate_mean, true)
  );
}

function isEligibility(value: unknown): value is EligibilityView {
  if (!isRecord(value) || !Array.isArray(value.reason_codes)) {
    return false;
  }
  const validReasons = value.reason_codes.every((reason) => isString(reason) && eligibilityReasons.has(reason));
  return (
    isString(value.symbol) && symbolPattern.test(value.symbol) &&
    typeof value.eligible === "boolean" &&
    validReasons &&
    ((value.eligible && value.reason_codes.length === 0) || (!value.eligible && value.reason_codes.length > 0)) &&
    isString(value.evaluated_at)
  );
}

function isStream(value: unknown): value is StreamStateView {
  if (!isRecord(value)) {
    return false;
  }
  return (
    isString(value.symbol) && symbolPattern.test(value.symbol) &&
    isString(value.stream_name) && /^[A-Za-z0-9_@.-]{1,128}$/.test(value.stream_name) &&
    isString(value.status) && streamStatuses.has(value.status) &&
    isNullableString(value.last_event_at) && isString(value.updated_at)
  );
}

function isJob(value: unknown): value is IngestionJobView {
  if (!isRecord(value)) {
    return false;
  }
  return (
    isString(value.job_id) && uuidPattern.test(value.job_id) &&
    isString(value.symbol) && symbolPattern.test(value.symbol) &&
    isString(value.data_type) && dataTypes.has(value.data_type) &&
    isString(value.status) && jobStatuses.has(value.status) &&
    isString(value.requested_start) && isString(value.requested_end) &&
    isString(value.created_at) && isString(value.updated_at)
  );
}

function decodeArray<T>(payload: unknown, guard: (value: unknown) => value is T): readonly T[] {
  if (!Array.isArray(payload) || !payload.every(guard)) {
    throw new ApiClientError("response");
  }
  return payload;
}

function decodeHealth(payload: unknown): MarketDataHealthView {
  if (
    !isRecord(payload) ||
    !isString(payload.source_mode) || !sourceModes.has(payload.source_mode) ||
    typeof payload.archive_healthy !== "boolean" ||
    typeof payload.rest_healthy !== "boolean" ||
    !isNullableString(payload.worker_heartbeat_at) ||
    !Array.isArray(payload.streams) || !payload.streams.every(isStream) ||
    !isString(payload.checked_at)
  ) {
    throw new ApiClientError("response");
  }
  return payload as unknown as MarketDataHealthView;
}

async function getOptionalProfile(path: string, signal?: AbortSignal): Promise<SymbolProfileView | null> {
  try {
    const payload = await getJson(path, signal);
    if (!isProfile(payload)) {
      throw new ApiClientError("response");
    }
    return payload;
  } catch (error) {
    if (error instanceof ApiClientError && error.status === 404) {
      return null;
    }
    throw error;
  }
}

export interface SymbolEvidence {
  readonly symbol: SymbolView;
  readonly partitions: readonly DataPartitionView[];
  readonly gaps: readonly DataGapView[];
  readonly profile: SymbolProfileView | null;
  readonly eligibility: EligibilityView;
  readonly streams: readonly StreamStateView[];
  readonly partitionsTruncated: boolean;
  readonly gapsTruncated: boolean;
  readonly streamsTruncated: boolean;
}

export type SymbolEvidenceState =
  | { readonly status: "ready"; readonly symbol: SymbolView; readonly evidence: SymbolEvidence }
  | { readonly status: "loading"; readonly symbol: SymbolView }
  | { readonly status: "error"; readonly symbol: SymbolView }
  | { readonly status: "refreshing"; readonly symbol: SymbolView }
  | { readonly status: "refresh_error"; readonly symbol: SymbolView };

export type MarketDataHealthState =
  | { readonly status: "ready"; readonly health: MarketDataHealthView }
  | { readonly status: "loading" }
  | { readonly status: "error" };

export interface SymbolsDashboard {
  readonly health: MarketDataHealthState;
  readonly items: readonly SymbolEvidenceState[];
  readonly symbolsTruncated: boolean;
}

export async function loadSymbolEvidence(symbol: SymbolView, signal?: AbortSignal): Promise<SymbolEvidence> {
  const encodedSymbol = encodeURIComponent(symbol.symbol);
  const [partitions, gaps, profile, eligibilityPayload, streams] = await Promise.all([
    getJson(`/api/symbols/${encodedSymbol}/partitions?limit=100&offset=0`, signal),
    getJson(`/api/symbols/${encodedSymbol}/gaps?limit=100&offset=0`, signal),
    getOptionalProfile(`/api/symbols/${encodedSymbol}/profile`, signal),
    getJson(`/api/symbols/${encodedSymbol}/eligibility`, signal),
    getJson(`/api/symbols/${encodedSymbol}/streams?limit=100&offset=0`, signal),
  ]);
  if (!isEligibility(eligibilityPayload) || eligibilityPayload.symbol !== symbol.symbol) {
    throw new ApiClientError("response");
  }
  const decodedPartitions = decodeArray(partitions, isPartition);
  const decodedGaps = decodeArray(gaps, isGap);
  const decodedStreams = decodeArray(streams, isStream);
  if (
    decodedPartitions.some((item) => item.symbol !== symbol.symbol) ||
    decodedGaps.some((item) => item.symbol !== symbol.symbol) ||
    decodedStreams.some((item) => item.symbol !== symbol.symbol) ||
    (profile !== null && profile.symbol !== symbol.symbol)
  ) {
    throw new ApiClientError("response");
  }
  return {
    symbol,
    partitions: decodedPartitions,
    gaps: decodedGaps,
    profile,
    eligibility: eligibilityPayload,
    streams: decodedStreams,
    partitionsTruncated: decodedPartitions.length === 100,
    gapsTruncated: decodedGaps.length === 100,
    streamsTruncated: decodedStreams.length === 100,
  };
}

export async function loadMarketDataHealth(signal?: AbortSignal): Promise<MarketDataHealthView> {
  return decodeHealth(await getJson("/api/operations/market-data", signal));
}

export interface AddSymbolInput {
  readonly symbol: string;
  readonly historyStart: string;
  readonly historyEnd: string;
  readonly includeAggTrades: boolean;
}

export async function addSymbol(
  input: AddSymbolInput,
  signal?: AbortSignal,
): Promise<SymbolView> {
  if (!symbolPattern.test(input.symbol)) {
    throw new ApiClientError("response");
  }
  const symbolPayload = await postJson("/api/symbols", {
    symbol: input.symbol,
    history_start: input.historyStart,
    history_end: input.historyEnd,
    include_agg_trades: input.includeAggTrades,
  }, signal);
  const expectedSymbol = symbolAliases.get(input.symbol) ?? input.symbol;
  if (!isSymbolView(symbolPayload) || symbolPayload.symbol !== expectedSymbol) {
    throw new ApiClientError("response");
  }
  return symbolPayload;
}

export async function createBackfills(
  input: AddSymbolInput,
  signal?: AbortSignal,
): Promise<readonly IngestionJobView[]> {
  if (!symbolPattern.test(input.symbol)) {
    throw new ApiClientError("response");
  }
  const dataTypesForBackfill = ["kline_1m", "mark_price", "funding"];
  if (input.includeAggTrades) {
    dataTypesForBackfill.push("agg_trade");
  }
  const jobsPayload = await postJson(`/api/symbols/${encodeURIComponent(input.symbol)}/backfills`, {
    symbol: input.symbol,
    data_types: dataTypesForBackfill,
    start: input.historyStart,
    end: input.historyEnd,
    include_agg_trades: input.includeAggTrades,
  }, signal);
  const jobs = decodeArray(jobsPayload, isJob);
  if (jobs.some((job) => job.symbol !== input.symbol)) {
    throw new ApiClientError("response");
  }
  return jobs;
}

export async function disableSymbol(symbol: string, signal?: AbortSignal): Promise<SymbolView> {
  if (!symbolPattern.test(symbol)) {
    throw new ApiClientError("response");
  }
  const payload = await deleteJson(`/api/symbols/${encodeURIComponent(symbol)}`, signal);
  if (!isSymbolView(payload) || payload.symbol !== symbol || payload.enabled) {
    throw new ApiClientError("response");
  }
  return payload;
}

export async function enableSymbol(symbol: SymbolView, signal?: AbortSignal): Promise<SymbolView> {
  const enabled = await addSymbol({
    symbol: symbol.symbol,
    historyStart: symbol.history_start,
    historyEnd: symbol.history_end,
    includeAggTrades: symbol.include_agg_trades,
  }, signal);
  if (!enabled.enabled) {
    throw new ApiClientError("response");
  }
  return enabled;
}

export async function getBackfill(jobId: string, signal?: AbortSignal): Promise<IngestionJobView> {
  if (!uuidPattern.test(jobId)) {
    throw new ApiClientError("response");
  }
  const payload = await getJson(`/api/backfills/${encodeURIComponent(jobId)}`, signal);
  if (!isJob(payload) || payload.job_id !== jobId) {
    throw new ApiClientError("response");
  }
  return payload;
}
