import { act, renderHook, waitFor } from "@testing-library/react";
import { expect, test, vi } from "vitest";
import type { SymbolView } from "./contracts";
import { useSymbols } from "./useSymbols";

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as Response;
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

const symbol: SymbolView = {
  symbol: "BTCUSDT",
  enabled: true,
  history_start: "2026-07-01T00:00:00Z",
  history_end: "2026-07-21T00:00:00Z",
  include_agg_trades: false,
  data_status: "data_ready",
  metadata_status: "eligible",
  created_at: "2026-07-01T00:00:00Z",
  updated_at: "2026-07-21T10:00:00Z",
};

function profile(sampleCount: number) {
  return {
    symbol: "BTCUSDT",
    calculated_at: "2026-07-21T10:00:00Z",
    coverage_start: "2026-07-01T00:00:00Z",
    coverage_end: "2026-07-21T00:00:00Z",
    sample_count: sampleCount,
    coverage_fraction: 0.98,
    realized_volatility: 0.12,
    jump_frequency: 0.01,
    median_spread_bps: 1.2,
    median_hourly_volume: 1200000,
    funding_rate_mean: 0.0001,
  };
}

function health(sourceMode = "direct") {
  return {
    source_mode: sourceMode,
    archive_healthy: true,
    rest_healthy: true,
    worker_heartbeat_at: "2026-07-21T10:00:00Z",
    streams: [],
    checked_at: "2026-07-21T10:00:01Z",
  };
}

function baseResponse(path: string): Response {
  if (path.startsWith("/api/symbols?")) {
    return jsonResponse([symbol]);
  }
  if (path === "/api/operations/market-data") {
    return jsonResponse(health());
  }
  if (path.endsWith("/profile")) {
    return jsonResponse(profile(120000));
  }
  if (path.endsWith("/eligibility")) {
    return jsonResponse({
      symbol: "BTCUSDT",
      eligible: true,
      reason_codes: [],
      evaluated_at: "2026-07-21T10:00:00Z",
    });
  }
  if (path.includes("/partitions") || path.includes("/gaps") || path.includes("/streams")) {
    return jsonResponse([]);
  }
  throw new Error(`Unexpected mock request: ${path}`);
}

function readyProfileCount(state: ReturnType<typeof useSymbols>): number | null {
  if (state.status !== "ready") {
    return null;
  }
  const item = state.dashboard.items[0];
  return item?.status === "ready" ? item.evidence.profile?.sample_count ?? null : null;
}

test("aborts and fences an older symbol retry when a newer retry wins", async () => {
  const olderProfile = deferred<Response>();
  let profileRequests = 0;
  let olderSignal: AbortSignal | undefined;
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.endsWith("/profile")) {
      profileRequests += 1;
      if (profileRequests === 2) {
        olderSignal = init?.signal ?? undefined;
        return olderProfile.promise;
      }
      if (profileRequests === 3) {
        return Promise.resolve(jsonResponse(profile(222222)));
      }
    }
    return Promise.resolve(baseResponse(path));
  }));

  const { result } = renderHook(() => useSymbols());
  await waitFor(() => expect(readyProfileCount(result.current)).toBe(120000));

  let olderRetry!: Promise<void>;
  act(() => {
    olderRetry = result.current.retrySymbol("BTCUSDT");
  });
  await waitFor(() => expect(profileRequests).toBe(2));
  await act(async () => {
    await result.current.retrySymbol("BTCUSDT");
  });

  expect(olderSignal?.aborted).toBe(true);
  expect(readyProfileCount(result.current)).toBe(222222);

  olderProfile.resolve(jsonResponse(profile(111111)));
  await act(async () => {
    await olderRetry;
  });
  expect(readyProfileCount(result.current)).toBe(222222);
});

test("aborts and fences symbol and health retries behind a newer full reload", async () => {
  const olderProfile = deferred<Response>();
  const olderHealth = deferred<Response>();
  let profileRequests = 0;
  let healthRequests = 0;
  let profileSignal: AbortSignal | undefined;
  let healthSignal: AbortSignal | undefined;
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.endsWith("/profile")) {
      profileRequests += 1;
      if (profileRequests === 2) {
        profileSignal = init?.signal ?? undefined;
        return olderProfile.promise;
      }
      if (profileRequests === 3) {
        return Promise.resolve(jsonResponse(profile(333333)));
      }
    }
    if (path === "/api/operations/market-data") {
      healthRequests += 1;
      if (healthRequests === 1) {
        return Promise.resolve(jsonResponse({ detail: "unavailable" }, 503));
      }
      if (healthRequests === 2) {
        healthSignal = init?.signal ?? undefined;
        return olderHealth.promise;
      }
      return Promise.resolve(jsonResponse(health("direct")));
    }
    return Promise.resolve(baseResponse(path));
  }));

  const { result } = renderHook(() => useSymbols());
  await waitFor(() => {
    expect(readyProfileCount(result.current)).toBe(120000);
    expect(result.current.status === "ready" && result.current.dashboard.health.status).toBe("error");
  });

  let symbolRetry!: Promise<void>;
  let healthRetry!: Promise<void>;
  act(() => {
    symbolRetry = result.current.retrySymbol("BTCUSDT");
    healthRetry = result.current.retryHealth();
  });
  await waitFor(() => {
    expect(profileRequests).toBe(2);
    expect(healthRequests).toBe(2);
  });
  act(() => result.current.reload());
  await waitFor(() => {
    expect(readyProfileCount(result.current)).toBe(333333);
    expect(result.current.status === "ready" && result.current.dashboard.health.status === "ready"
      ? result.current.dashboard.health.health.source_mode
      : null).toBe("direct");
  });

  expect(profileSignal?.aborted).toBe(true);
  expect(healthSignal?.aborted).toBe(true);

  olderProfile.resolve(jsonResponse(profile(111111)));
  olderHealth.resolve(jsonResponse(health("proxy")));
  await act(async () => {
    await Promise.all([symbolRetry, healthRetry]);
  });
  expect(readyProfileCount(result.current)).toBe(333333);
  expect(result.current.status === "ready" && result.current.dashboard.health.status === "ready"
    ? result.current.dashboard.health.health.source_mode
    : null).toBe("direct");
});

test("aborts and fences an older full list load behind a newer reload", async () => {
  const olderList = deferred<Response>();
  let listRequests = 0;
  let olderSignal: AbortSignal | undefined;
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.startsWith("/api/symbols?")) {
      listRequests += 1;
      if (listRequests === 1) {
        olderSignal = init?.signal ?? undefined;
        return olderList.promise;
      }
    }
    return Promise.resolve(baseResponse(path));
  }));

  const { result } = renderHook(() => useSymbols());
  await waitFor(() => expect(listRequests).toBe(1));
  act(() => result.current.reload());
  await waitFor(() => expect(readyProfileCount(result.current)).toBe(120000));

  expect(olderSignal?.aborted).toBe(true);
  olderList.resolve(jsonResponse([]));
  await act(async () => {
    await olderList.promise;
  });
  expect(readyProfileCount(result.current)).toBe(120000);
});

test("aborts every in-flight initial dashboard request on unmount", async () => {
  const pendingProfile = deferred<Response>();
  let pendingSignal: AbortSignal | undefined;
  const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.endsWith("/profile")) {
      pendingSignal = init?.signal ?? undefined;
    }
    return path.endsWith("/profile") ? pendingProfile.promise : Promise.resolve(baseResponse(path));
  });
  vi.stubGlobal("fetch", fetchMock);

  const { result, unmount } = renderHook(() => useSymbols());
  await waitFor(() => expect(pendingSignal).toBeDefined());
  const refreshAfterUnmount = result.current.refreshSymbol;
  unmount();

  expect(pendingSignal?.aborted).toBe(true);
  const requestsAtUnmount = fetchMock.mock.calls.length;
  await expect(refreshAfterUnmount(symbol)).resolves.toBe(false);
  expect(fetchMock).toHaveBeenCalledTimes(requestsAtUnmount);
  pendingProfile.resolve(jsonResponse(profile(120000)));
});
