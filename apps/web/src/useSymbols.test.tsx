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

const disabledSymbol: SymbolView = {
  ...symbol,
  enabled: false,
  data_status: "disabled",
  updated_at: "2026-07-21T10:01:00Z",
};

const pepeSymbol: SymbolView = {
  ...symbol,
  symbol: "PEPEUSDT",
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

function firstSymbolState(state: ReturnType<typeof useSymbols>) {
  return state.status === "ready" ? state.dashboard.items[0] : undefined;
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

test("keeps a committed disabled override when a stale reload resolves after the mutation", async () => {
  const staleReload = deferred<Response>();
  const committedProfile = deferred<Response>();
  let listRequests = 0;
  let profileRequests = 0;
  let committedProfileSignal: AbortSignal | undefined;
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.startsWith("/api/symbols?")) {
      listRequests += 1;
      return listRequests === 2 ? staleReload.promise : Promise.resolve(jsonResponse([symbol]));
    }
    if (path.endsWith("/profile")) {
      profileRequests += 1;
      if (profileRequests === 2) {
        committedProfileSignal = init?.signal ?? undefined;
        return committedProfile.promise;
      }
      return Promise.resolve(jsonResponse(profile(profileRequests === 1 ? 120000 : 333333)));
    }
    return Promise.resolve(baseResponse(path));
  }));

  const { result } = renderHook(() => useSymbols());
  await waitFor(() => expect(readyProfileCount(result.current)).toBe(120000));

  act(() => result.current.reload());
  await waitFor(() => expect(listRequests).toBe(2));
  expect(result.current.status).toBe("ready");

  let committedRefresh!: Promise<boolean>;
  act(() => {
    committedRefresh = result.current.refreshSymbol(disabledSymbol);
  });
  await waitFor(() => {
    const item = firstSymbolState(result.current);
    expect(item?.status).toBe("refreshing");
    expect(item?.symbol.enabled).toBe(false);
  });

  staleReload.resolve(jsonResponse([symbol]));
  await waitFor(() => {
    const item = firstSymbolState(result.current);
    expect(item?.status).toBe("ready");
    expect(item?.symbol.enabled).toBe(false);
    expect(item?.status === "ready" ? item.evidence.profile?.sample_count : null).toBe(333333);
  });
  expect(committedProfileSignal?.aborted).toBe(true);
  committedProfile.resolve(jsonResponse(profile(111111)));
  await expect(committedRefresh).resolves.toBe(false);
});

test("lets a committed disabled mutation win when the stale reload resolves first", async () => {
  const staleReload = deferred<Response>();
  const staleProfile = deferred<Response>();
  let listRequests = 0;
  let profileRequests = 0;
  let staleProfileSignal: AbortSignal | undefined;
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.startsWith("/api/symbols?")) {
      listRequests += 1;
      return listRequests === 2 ? staleReload.promise : Promise.resolve(jsonResponse([symbol]));
    }
    if (path.endsWith("/profile")) {
      profileRequests += 1;
      if (profileRequests === 2) {
        staleProfileSignal = init?.signal ?? undefined;
        return staleProfile.promise;
      }
      return Promise.resolve(jsonResponse(profile(profileRequests === 1 ? 120000 : 444444)));
    }
    return Promise.resolve(baseResponse(path));
  }));

  const { result } = renderHook(() => useSymbols());
  await waitFor(() => expect(readyProfileCount(result.current)).toBe(120000));

  act(() => result.current.reload());
  await waitFor(() => expect(listRequests).toBe(2));
  expect(result.current.status).toBe("ready");
  staleReload.resolve(jsonResponse([symbol]));
  await waitFor(() => expect(staleProfileSignal).toBeDefined());

  await act(async () => {
    await result.current.refreshSymbol(disabledSymbol);
  });
  const item = firstSymbolState(result.current);
  expect(item?.status).toBe("ready");
  expect(item?.symbol.enabled).toBe(false);
  expect(item?.status === "ready" ? item.evidence.profile?.sample_count : null).toBe(444444);
  expect(staleProfileSignal?.aborted).toBe(true);
  staleProfile.resolve(jsonResponse(profile(111111)));
});

test("aborts sibling evidence requests after one endpoint fails and retries without old work", async () => {
  const neverSettlingProfile = deferred<Response>();
  let evidenceLoads = 0;
  let firstProfileSignal: AbortSignal | undefined;
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.startsWith("/api/symbols?")) {
      return Promise.resolve(jsonResponse([symbol]));
    }
    if (path.includes("/partitions")) {
      evidenceLoads += 1;
      return evidenceLoads === 1
        ? Promise.resolve(jsonResponse({ detail: "unavailable" }, 503))
        : Promise.resolve(jsonResponse([]));
    }
    if (path.endsWith("/profile")) {
      if (evidenceLoads === 1) {
        firstProfileSignal = init?.signal ?? undefined;
        return neverSettlingProfile.promise;
      }
      return Promise.resolve(jsonResponse(profile(555555)));
    }
    return Promise.resolve(baseResponse(path));
  }));

  const { result } = renderHook(() => useSymbols());
  await waitFor(() => expect(firstSymbolState(result.current)?.status).toBe("error"));
  expect(firstProfileSignal?.aborted).toBe(true);

  await act(async () => {
    await result.current.retrySymbol("BTCUSDT");
  });
  expect(readyProfileCount(result.current)).toBe(555555);
  expect(evidenceLoads).toBe(2);
  neverSettlingProfile.resolve(jsonResponse(profile(111111)));
});

test("keeps failed and successful committed mutations isolated across a stale reload", async () => {
  let mutating = false;
  const partitionLoads = new Map<string, number>();
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
    const path = String(input);
    const symbolName = path.includes("PEPEUSDT") ? "PEPEUSDT" : "BTCUSDT";
    if (path.startsWith("/api/symbols?")) {
      return Promise.resolve(jsonResponse([symbol, pepeSymbol]));
    }
    if (path === "/api/operations/market-data") {
      return Promise.resolve(jsonResponse(health()));
    }
    if (path.includes("/partitions")) {
      const loads = (partitionLoads.get(symbolName) ?? 0) + 1;
      partitionLoads.set(symbolName, loads);
      if (mutating && symbolName === "BTCUSDT" && loads === 2) {
        return Promise.resolve(jsonResponse({ detail: "unavailable" }, 503));
      }
      return Promise.resolve(jsonResponse([]));
    }
    if (path.endsWith("/profile")) {
      return Promise.resolve(jsonResponse({ ...profile(symbolName === "BTCUSDT" ? 120000 : 300), symbol: symbolName }));
    }
    if (path.endsWith("/eligibility")) {
      return Promise.resolve(jsonResponse({
        symbol: symbolName,
        eligible: !mutating,
        reason_codes: mutating ? ["data_not_ready"] : [],
        evaluated_at: "2026-07-21T10:00:00Z",
      }));
    }
    if (path.includes("/gaps") || path.includes("/streams")) {
      return Promise.resolve(jsonResponse([]));
    }
    throw new Error(`Unexpected mock request: ${path}`);
  }));

  const { result } = renderHook(() => useSymbols());
  await waitFor(() => {
    expect(result.current.status === "ready" ? result.current.dashboard.items.every((item) => item.status === "ready") : false).toBe(true);
  });
  mutating = true;

  await act(async () => {
    await result.current.refreshSymbol(disabledSymbol);
  });
  expect(result.current.status === "ready"
    ? result.current.dashboard.items.find((item) => item.symbol.symbol === "BTCUSDT")?.status
    : null).toBe("refresh_error");

  await act(async () => {
    await result.current.refreshSymbol({
      ...pepeSymbol,
      enabled: false,
      data_status: "disabled",
      updated_at: "2026-07-21T10:01:00Z",
    });
  });
  expect(result.current.status === "ready"
    ? result.current.dashboard.items.find((item) => item.symbol.symbol === "BTCUSDT")?.status
    : null).toBe("refresh_error");
  expect(result.current.status === "ready"
    ? result.current.dashboard.items.find((item) => item.symbol.symbol === "PEPEUSDT")?.symbol.enabled
    : null).toBe(false);

  act(() => result.current.reload());
  await waitFor(() => {
    if (result.current.status !== "ready") {
      throw new Error("dashboard is not ready");
    }
    const btc = result.current.dashboard.items.find((item) => item.symbol.symbol === "BTCUSDT");
    const pepe = result.current.dashboard.items.find((item) => item.symbol.symbol === "PEPEUSDT");
    expect(btc?.status).toBe("ready");
    expect(btc?.symbol.enabled).toBe(false);
    expect(pepe?.status).toBe("ready");
    expect(pepe?.symbol.enabled).toBe(false);
  });
});

test("keeps a committed enable override when a reload still lists the symbol disabled", async () => {
  const staleDisabled = {
    ...disabledSymbol,
    updated_at: "2026-07-21T10:00:00Z",
  };
  const committedEnabled = {
    ...symbol,
    updated_at: "2026-07-21T10:01:00Z",
  };
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
    const path = String(input);
    if (path.startsWith("/api/symbols?")) {
      return Promise.resolve(jsonResponse([staleDisabled]));
    }
    return Promise.resolve(baseResponse(path));
  }));

  const { result } = renderHook(() => useSymbols());
  await waitFor(() => expect(firstSymbolState(result.current)?.status).toBe("ready"));
  expect(firstSymbolState(result.current)?.symbol.enabled).toBe(false);

  await act(async () => {
    await result.current.refreshSymbol(committedEnabled);
  });
  expect(firstSymbolState(result.current)?.symbol.enabled).toBe(true);

  act(() => result.current.reload());
  await waitFor(() => {
    expect(firstSymbolState(result.current)?.status).toBe("ready");
    expect(firstSymbolState(result.current)?.symbol.enabled).toBe(true);
  });
});

test.each([
  {
    name: "newer opposite server state supersedes",
    committed: disabledSymbol,
    listed: { ...symbol, updated_at: "2026-07-21T10:02:00Z" },
    pendingStatus: "loading",
    expectedEnabled: true,
  },
  {
    name: "older opposite server state is retained locally",
    committed: disabledSymbol,
    listed: { ...symbol, updated_at: "2026-07-21T10:00:00Z" },
    pendingStatus: "refreshing",
    expectedEnabled: false,
  },
  {
    name: "equal current server state acknowledges the mutation",
    committed: disabledSymbol,
    listed: { ...disabledSymbol },
    pendingStatus: "ready",
    expectedEnabled: false,
  },
  {
    name: "invalid server ordering fails closed",
    committed: disabledSymbol,
    listed: { ...symbol, updated_at: "not-a-date" },
    pendingStatus: "refreshing",
    expectedEnabled: false,
  },
  {
    name: "invalid committed ordering fails closed",
    committed: { ...disabledSymbol, updated_at: "not-a-date" },
    listed: { ...symbol, updated_at: "2026-07-21T10:02:00Z" },
    pendingStatus: "refreshing",
    expectedEnabled: false,
  },
] as const)("reconciles committed mutation ordering: $name", async ({ committed, listed, pendingStatus, expectedEnabled }) => {
  const reloadProfile = deferred<Response>();
  let listRequests = 0;
  let profileRequests = 0;
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
    const path = String(input);
    if (path.startsWith("/api/symbols?")) {
      listRequests += 1;
      return Promise.resolve(jsonResponse(listRequests === 1 ? [symbol] : [listed]));
    }
    if (path.endsWith("/profile")) {
      profileRequests += 1;
      return profileRequests === 3
        ? reloadProfile.promise
        : Promise.resolve(jsonResponse(profile(120000 + profileRequests)));
    }
    return Promise.resolve(baseResponse(path));
  }));

  const { result } = renderHook(() => useSymbols());
  await waitFor(() => expect(firstSymbolState(result.current)?.status).toBe("ready"));
  await act(async () => {
    await result.current.refreshSymbol(committed);
  });

  act(() => result.current.reload());
  await waitFor(() => expect(profileRequests).toBe(3));
  expect(firstSymbolState(result.current)?.status).toBe(pendingStatus);
  expect(firstSymbolState(result.current)?.symbol.enabled).toBe(expectedEnabled);

  reloadProfile.resolve(jsonResponse(profile(999999)));
  await waitFor(() => expect(firstSymbolState(result.current)?.status).toBe("ready"));
  expect(firstSymbolState(result.current)?.symbol.enabled).toBe(expectedEnabled);
});
