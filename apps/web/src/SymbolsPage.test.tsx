import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { SymbolsPage } from "./SymbolsPage";
import { toArchiveUtcRange } from "./components/AddSymbolForm";
import i18n from "./i18n";

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

const baseSymbol = {
  enabled: true,
  history_start: "2026-07-01T00:00:00Z",
  history_end: "2026-07-21T00:00:00Z",
  include_agg_trades: false,
  data_status: "data_ready",
  metadata_status: "eligible",
  created_at: "2026-07-01T00:00:00Z",
  updated_at: "2026-07-21T10:00:00Z",
};

function dashboardResponse(path: string): Response {
  const isBtc = path.includes("BTCUSDT");
  if (path.startsWith("/api/symbols?")) {
    return jsonResponse([
      { ...baseSymbol, symbol: "BTCUSDT" },
      {
        ...baseSymbol,
        symbol: "1000PEPEUSDT",
        data_status: "degraded",
        metadata_status: "ineligible",
      },
    ]);
  }
  if (path === "/api/operations/market-data") {
    return jsonResponse({
      source_mode: "proxy",
      archive_healthy: true,
      rest_healthy: false,
      worker_heartbeat_at: "2026-07-21T10:00:00Z",
      streams: [],
      checked_at: "2026-07-21T10:00:01Z",
    });
  }
  if (path.includes("/partitions")) {
    return jsonResponse(isBtc ? [{
      partition_id: "00000000-0000-0000-0000-000000000201",
      symbol: "BTCUSDT",
      data_type: "kline_1m",
      start: "2026-07-01T00:00:00Z",
      end: "2026-07-21T00:00:00Z",
      parquet_path: "normalized/binance/usdm/BTCUSDT/klines/part.parquet",
      checksum: "a".repeat(64),
      row_count: 28800,
      version: 1,
      status: "approved",
      created_at: "2026-07-21T00:00:00Z",
      approved_at: "2026-07-21T01:00:00Z",
    }] : []);
  }
  if (path.includes("/gaps")) {
    return jsonResponse(isBtc ? [] : [{
      gap_id: "00000000-0000-0000-0000-000000000301",
      symbol: "1000PEPEUSDT",
      data_type: "kline_1m",
      start: "2026-07-20T03:00:00Z",
      end: "2026-07-20T03:01:00Z",
      reason: "source_unknown",
      status: "open",
      opened_at: "2026-07-20T03:02:00Z",
      repaired_at: null,
    }]);
  }
  if (path.endsWith("/profile")) {
    return jsonResponse({
      symbol: isBtc ? "BTCUSDT" : "1000PEPEUSDT",
      calculated_at: "2026-07-21T10:00:00Z",
      coverage_start: "2026-07-01T00:00:00Z",
      coverage_end: "2026-07-21T00:00:00Z",
      sample_count: isBtc ? 120000 : 300,
      coverage_fraction: isBtc ? 0.98 : 0.42,
      realized_volatility: isBtc ? 0.12 : 0.91,
      jump_frequency: isBtc ? 0.01 : 0.2,
      median_spread_bps: isBtc ? 1.2 : 8.5,
      median_hourly_volume: isBtc ? 1200000 : 1800,
      funding_rate_mean: 0.0001,
    });
  }
  if (path.endsWith("/eligibility")) {
    return jsonResponse({
      symbol: isBtc ? "BTCUSDT" : "1000PEPEUSDT",
      eligible: isBtc,
      reason_codes: isBtc ? [] : ["insufficient_coverage", "unrepaired_gap"],
      evaluated_at: "2026-07-21T10:00:00Z",
    });
  }
  if (path.includes("/streams")) {
    const symbol = isBtc ? "BTCUSDT" : "1000PEPEUSDT";
    const suffixes = ["aggTrade", "bookTicker", "kline_1m", "markPrice@1s"];
    return jsonResponse(suffixes.slice(0, isBtc ? 4 : 3).map((suffix, index) => ({
      symbol,
      stream_name: `${symbol.toLowerCase()}@${suffix}`,
      status: "connected",
      last_event_at: `2026-07-21T09:59:${String(56 + index).padStart(2, "0")}Z`,
      updated_at: "2026-07-21T10:00:00Z",
    })));
  }
  throw new Error(`Unexpected mock request: ${path}`);
}

beforeEach(async () => {
  await i18n.changeLanguage("zh-CN");
});

afterEach(() => {
  vi.useRealTimers();
});

test("allows exactly 366 inclusive UTC days and converts the inclusive end to the next midnight", () => {
  expect(toArchiveUtcRange("2024-01-01", "2024-12-31", new Date("2026-07-21T12:00:00Z"))).toEqual({
    ok: true,
    start: "2024-01-01T00:00:00.000Z",
    end: "2025-01-01T00:00:00.000Z",
  });
});

test("rejects history that is not one or more complete UTC calendar months", () => {
  expect(toArchiveUtcRange("2026-07-14", "2026-07-16", new Date("2026-07-21T12:00:00Z"))).toEqual({
    ok: false,
    reason: "incomplete_month",
  });
  expect(toArchiveUtcRange("2026-06-01", "2026-06-29", new Date("2026-07-21T12:00:00Z"))).toEqual({
    ok: false,
    reason: "incomplete_month",
  });
});

test("announces the initial data-console loading state", () => {
  vi.stubGlobal("fetch", vi.fn(() => new Promise<Response>(() => undefined)));

  render(<SymbolsPage />);

  expect(screen.getByRole("status", { name: "正在加载币种与数据" })).toBeInTheDocument();
});

test("renders an actionable empty state after the API returns no symbols", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.startsWith("/api/symbols")) {
        return jsonResponse([]);
      }
      return jsonResponse({
        source_mode: "direct",
        archive_healthy: true,
        rest_healthy: true,
        worker_heartbeat_at: "2026-07-21T10:00:00Z",
        streams: [],
        checked_at: "2026-07-21T10:00:01Z",
      });
    }),
  );

  render(<SymbolsPage />);

  expect(await screen.findByText("尚未监控任何币种")).toBeInTheDocument();
  expect(screen.getByText("添加第一个币种后，这里会显示独立的数据证据与启用条件。")).toBeInTheDocument();
});

test("shows a redacted error and retries without rendering server HTML, URLs, or paths", async () => {
  const user = userEvent.setup();
  let attempts = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.startsWith("/api/symbols?")) {
        attempts += 1;
      }
      if (attempts === 1) {
        return jsonResponse(
          { detail: '<img src=x onerror=alert(1)> https://secret.invalid /srv/private/data' },
          503,
        );
      }
      if (path.startsWith("/api/symbols?")) {
        return jsonResponse([]);
      }
      return jsonResponse({
        source_mode: "direct",
        archive_healthy: true,
        rest_healthy: true,
        worker_heartbeat_at: "2026-07-21T10:00:00Z",
        streams: [],
        checked_at: "2026-07-21T10:00:01Z",
      });
    }),
  );

  render(<SymbolsPage />);

  const alert = await screen.findByRole("alert");
  expect(alert).toHaveTextContent("暂时无法加载币种数据。请检查服务后重试。");
  expect(alert).not.toHaveTextContent("secret.invalid");
  expect(alert).not.toHaveTextContent("/srv/private/data");
  expect(document.querySelector("img")).toBeNull();

  await user.click(screen.getByRole("button", { name: "重试加载" }));

  expect(await screen.findByText("尚未监控任何币种")).toBeInTheDocument();
  expect(attempts).toBe(2);
});

test("keeps BTC and PEPE evidence independent and separates data-health dimensions", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => dashboardResponse(String(input))),
  );

  render(<SymbolsPage />);

  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  const pepe = await screen.findByRole("article", { name: "1000PEPEUSDT 数据证据" });

  expect(within(btc).getByText("符合数据启用条件")).toBeInTheDocument();
  expect(within(btc).getByText("120,000")).toBeInTheDocument();
  expect(within(btc).getByText("98%")).toBeInTheDocument();
  expect(within(btc).getByText("无未修复缺口")).toBeInTheDocument();
  expect(within(btc).queryByText("覆盖不足")).not.toBeInTheDocument();

  expect(within(pepe).getByText("不符合数据启用条件")).toBeInTheDocument();
  expect(within(pepe).getByText("300")).toBeInTheDocument();
  expect(within(pepe).getByText("42%")).toBeInTheDocument();
  expect(within(pepe).getByText("1 个未修复缺口")).toBeInTheDocument();
  expect(within(pepe).getByText("覆盖不足")).toBeInTheDocument();
  expect(within(pepe).getByText("存在未修复缺口")).toBeInTheDocument();

  const health = screen.getByRole("region", { name: "数据来源与健康" });
  expect(within(health).getByText("代理路径")).toBeInTheDocument();
  expect(within(health).getByText("归档下载正常")).toBeInTheDocument();
  expect(within(health).getByText("实时采集正常")).toBeInTheDocument();
  expect(within(health).getByText("REST / 元数据受限")).toBeInTheDocument();
  expect(within(health).queryByText(/错误|故障/)).not.toBeInTheDocument();

  expect(within(btc).getByText("4/4 条必需实时流已连接")).toBeInTheDocument();
  expect(within(btc).getByText(/最旧必需事件/)).toHaveTextContent("2026");
  expect(within(pepe).getByText("缺少 1 条必需实时流（3/4）")).toBeInTheDocument();
  expect(within(pepe).getByText("必需流不完整，无法确认新鲜度")).toBeInTheDocument();
});

test("publishes health and ready symbol evidence while another symbol request is still pending", async () => {
  const pepeProfile = deferred<Response>();
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL) => {
      const path = String(input);
      return path.endsWith("1000PEPEUSDT/profile")
        ? pepeProfile.promise
        : Promise.resolve(dashboardResponse(path));
    }),
  );

  render(<SymbolsPage />);

  expect(await screen.findByRole("region", { name: "数据来源与健康" })).toBeInTheDocument();
  expect(await screen.findByRole("article", { name: "BTCUSDT 数据证据" })).toBeInTheDocument();
  expect(screen.getByRole("article", { name: "正在加载 1000PEPEUSDT 数据" })).toBeInTheDocument();
  expect(screen.queryByRole("status", { name: "正在加载币种与数据" })).not.toBeInTheDocument();

  pepeProfile.resolve(dashboardResponse("/api/symbols/1000PEPEUSDT/profile"));
  expect(await screen.findByRole("article", { name: "1000PEPEUSDT 数据证据" })).toBeInTheDocument();
});

test("shows backend stale status while retaining the oldest exact required-stream event", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.endsWith("1000PEPEUSDT/eligibility")) {
        return jsonResponse({
          symbol: "1000PEPEUSDT",
          eligible: false,
          reason_codes: ["stale_live_data"],
          evaluated_at: "2026-07-21T10:00:00Z",
        });
      }
      if (path.includes("1000PEPEUSDT/streams")) {
        return jsonResponse(["aggTrade", "bookTicker", "kline_1m", "markPrice@1s"].map((suffix, index) => ({
          symbol: "1000PEPEUSDT",
          stream_name: `1000pepeusdt@${suffix}`,
          status: "connected",
          last_event_at: `2026-07-21T09:40:0${index}Z`,
          updated_at: "2026-07-21T10:00:00Z",
        })));
      }
      return dashboardResponse(path);
    }),
  );

  render(<SymbolsPage />);

  const pepe = await screen.findByRole("article", { name: "1000PEPEUSDT 数据证据" });
  expect(within(pepe).getByText("实时数据已过期（由服务端启用条件判定）")).toBeInTheDocument();
  expect(within(pepe).getByText(/最旧必需事件/)).toHaveTextContent("09:40:00");
});

test("labels bounded catalog and gap counts as visible and truncated instead of exact totals", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.includes("BTCUSDT/partitions")) {
        return jsonResponse(Array.from({ length: 100 }, (_, index) => ({
          partition_id: `00000000-0000-0000-0000-${String(600 + index).padStart(12, "0")}`,
          symbol: "BTCUSDT",
          data_type: "kline_1m",
          start: "2026-07-01T00:00:00Z",
          end: "2026-07-02T00:00:00Z",
          parquet_path: `catalog/BTCUSDT/${index}.parquet`,
          checksum: "a".repeat(64),
          row_count: 10,
          version: 1,
          status: "approved",
          created_at: "2026-07-21T00:00:00Z",
          approved_at: "2026-07-21T01:00:00Z",
        })));
      }
      if (path.includes("BTCUSDT/gaps")) {
        return jsonResponse(Array.from({ length: 100 }, (_, index) => ({
          gap_id: `00000000-0000-0000-0000-${String(800 + index).padStart(12, "0")}`,
          symbol: "BTCUSDT",
          data_type: "kline_1m",
          start: "2026-07-20T03:00:00Z",
          end: "2026-07-20T03:01:00Z",
          reason: `gap_${index}`,
          status: "open",
          opened_at: "2026-07-20T03:02:00Z",
          repaired_at: null,
        })));
      }
      return dashboardResponse(path);
    }),
  );

  render(<SymbolsPage />);

  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  expect(within(btc).getByText("可见已批准目录行数")).toBeInTheDocument();
  expect(within(btc).getByText("至少 1,000")).toBeInTheDocument();
  expect(within(btc).getByText("可见未修复缺口")).toBeInTheDocument();
  expect(within(btc).getByText("至少 100 个")).toBeInTheDocument();
  expect(within(btc).getByText("仅显示首批目录或缺口记录；带“至少”的数量不是完整总数。")).toBeInTheDocument();
  expect(within(btc).queryByText("已批准归档行数")).not.toBeInTheDocument();
});

test("rejects eligibility payloads that violate the eligible-to-reasons invariant", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.endsWith("BTCUSDT/eligibility")) {
        return jsonResponse({
          symbol: "BTCUSDT",
          eligible: true,
          reason_codes: ["stale_live_data"],
          evaluated_at: "2026-07-21T10:00:00Z",
        });
      }
      if (path.endsWith("1000PEPEUSDT/eligibility")) {
        return jsonResponse({
          symbol: "1000PEPEUSDT",
          eligible: false,
          reason_codes: [],
          evaluated_at: "2026-07-21T10:00:00Z",
        });
      }
      return dashboardResponse(path);
    }),
  );

  render(<SymbolsPage />);

  expect(await screen.findByRole("article", { name: "BTCUSDT 数据加载失败" })).toBeInTheDocument();
  expect(screen.getByRole("article", { name: "1000PEPEUSDT 数据加载失败" })).toBeInTheDocument();
  expect(screen.queryByText("符合数据启用条件")).not.toBeInTheDocument();
});

test("isolates a PEPE evidence failure and retries only that symbol", async () => {
  const user = userEvent.setup();
  let pepeProfileAttempts = 0;
  let btcProfileAttempts = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.endsWith("BTCUSDT/profile")) {
        btcProfileAttempts += 1;
      }
      if (path.endsWith("1000PEPEUSDT/profile")) {
        pepeProfileAttempts += 1;
        if (pepeProfileAttempts === 1) {
          return jsonResponse({ detail: "profile unavailable" }, 503);
        }
      }
      return dashboardResponse(path);
    }),
  );

  render(<SymbolsPage />);

  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  const pepeError = await screen.findByRole("article", { name: "1000PEPEUSDT 数据加载失败" });
  expect(within(btc).getByText("120,000")).toBeInTheDocument();
  expect(within(pepeError).getByText("暂时无法加载 1000PEPEUSDT 的数据证据。")).toBeInTheDocument();

  await user.click(within(pepeError).getByRole("button", { name: "重试 1000PEPEUSDT" }));
  expect(await screen.findByRole("article", { name: "1000PEPEUSDT 数据证据" })).toBeInTheDocument();
  expect(screen.getByRole("article", { name: "BTCUSDT 数据证据" })).toBeInTheDocument();
  expect(btcProfileAttempts).toBe(1);
  expect(pepeProfileAttempts).toBe(2);
});

test("keeps symbol evidence visible when operations health fails and retries health separately", async () => {
  const user = userEvent.setup();
  let healthAttempts = 0;
  let btcProfileAttempts = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.endsWith("BTCUSDT/profile")) {
        btcProfileAttempts += 1;
      }
      if (path === "/api/operations/market-data") {
        healthAttempts += 1;
        if (healthAttempts === 1) {
          return jsonResponse({ detail: "operations unavailable" }, 503);
        }
      }
      return dashboardResponse(path);
    }),
  );

  render(<SymbolsPage />);

  expect(await screen.findByRole("article", { name: "BTCUSDT 数据证据" })).toBeInTheDocument();
  const healthError = screen.getByRole("alert", { name: "数据来源状态加载失败" });
  expect(healthError).toHaveTextContent("币种证据仍可查看；数据来源与健康状态暂时无法加载。");
  await user.click(within(healthError).getByRole("button", { name: "重试健康状态" }));

  expect(await screen.findByRole("region", { name: "数据来源与健康" })).toBeInTheDocument();
  expect(healthAttempts).toBe(2);
  expect(btcProfileAttempts).toBe(1);
});

test("uses a returned canonical venue symbol for a complete-month backfill", async () => {
  const user = userEvent.setup();
  const requests: Array<{ path: string; body: unknown }> = [];
  let listRequests = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (init?.method === "POST") {
        const body = JSON.parse(String(init.body)) as unknown;
        requests.push({ path, body });
        if (path === "/api/symbols") {
          return jsonResponse({
            ...baseSymbol,
            symbol: "1000PEPEUSDT",
            data_status: "requested",
            metadata_status: "metadata_unverified",
            include_agg_trades: true,
          });
        }
        return jsonResponse(["kline_1m", "mark_price", "funding", "agg_trade"].map((dataType, index) => ({
          job_id: `00000000-0000-0000-0000-00000000040${index}`,
          symbol: "1000PEPEUSDT",
          data_type: dataType,
          status: index === 0 ? "running" : "queued",
          requested_start: "2026-06-01T00:00:00Z",
          requested_end: "2026-07-01T00:00:00Z",
          created_at: "2026-07-21T10:00:00Z",
          updated_at: "2026-07-21T10:00:00Z",
        })));
      }
      if (path.startsWith("/api/backfills/")) {
        const jobIndex = Number(path.at(-1));
        const dataType = ["kline_1m", "mark_price", "funding", "agg_trade"][jobIndex];
        return jsonResponse({
          job_id: path.split("/").at(-1),
          symbol: "1000PEPEUSDT",
          data_type: dataType,
          status: "succeeded",
          requested_start: "2026-06-01T00:00:00Z",
          requested_end: "2026-07-01T00:00:00Z",
          created_at: "2026-07-21T10:00:00Z",
          updated_at: "2026-07-21T10:05:00Z",
        });
      }
      if (path.startsWith("/api/symbols?")) {
        listRequests += 1;
        return jsonResponse([]);
      }
      return jsonResponse({
        source_mode: "direct",
        archive_healthy: true,
        rest_healthy: true,
        worker_heartbeat_at: "2026-07-21T10:00:00Z",
        streams: [],
        checked_at: "2026-07-21T10:00:01Z",
      });
    }),
  );

  render(<SymbolsPage />);
  await screen.findByText("尚未监控任何币种");
  await user.click(screen.getByRole("button", { name: "添加币种" }));

  const form = screen.getByRole("form", { name: "添加监控币种" });
  expect(within(form).getByText("历史回填必须选择一个或多个完整 UTC 自然月：开始日为月初，结束日为月末；提交为 [月初 00:00Z, 下月月初 00:00Z) 半开区间。")).toBeInTheDocument();
  expect(within(form).getByText("输入 PEPE 或 PEPEUSDT 时会保存并显示 Binance USDⓈ-M 实际合约 1000PEPEUSDT。")).toBeInTheDocument();
  expect(within(form).getByText(/aggTrades 历史体量很大/)).toBeInTheDocument();
  expect(within(form).getByText("此操作只配置数据采集，不会启用交易、通知或 AI 下单。")).toBeInTheDocument();

  await user.type(within(form).getByLabelText("币种代码"), "PEPE");
  fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2026-06-01" } });
  fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2026-06-30" } });
  const symbolOptIn = within(form).getByRole("checkbox", { name: "为此币种启用 aggTrades 历史" });
  const backfillOptIn = within(form).getByRole("checkbox", { name: "我确认本次回填也包含 aggTrades" });
  expect(backfillOptIn).toBeDisabled();
  await user.click(symbolOptIn);
  expect(backfillOptIn).toBeEnabled();
  await user.click(backfillOptIn);
  await user.click(within(form).getByRole("button", { name: "添加并开始回填" }));

  const progress = await screen.findByRole("status", { name: "回填任务已创建" });
  expect(progress).toHaveTextContent("已创建 4 个回填任务");
  expect(progress).toHaveTextContent("运行中");
  expect(progress).toHaveTextContent("排队中");
  await waitFor(() => expect(listRequests).toBe(3));
  await user.click(within(progress).getByRole("button", { name: "刷新回填进度" }));
  expect(await within(progress).findAllByText("已完成")).toHaveLength(4);
  expect(requests).toEqual([
    {
      path: "/api/symbols",
      body: {
        symbol: "PEPE",
        history_start: "2026-06-01T00:00:00.000Z",
        history_end: "2026-07-01T00:00:00.000Z",
        include_agg_trades: true,
      },
    },
    {
      path: "/api/symbols/1000PEPEUSDT/backfills",
      body: {
        symbol: "1000PEPEUSDT",
        data_types: ["kline_1m", "mark_price", "funding", "agg_trade"],
        start: "2026-06-01T00:00:00.000Z",
        end: "2026-07-01T00:00:00.000Z",
        include_agg_trades: true,
      },
    },
  ]);
});

test("rejects an inclusive history selection longer than 366 UTC days", async () => {
  const user = userEvent.setup();
  const post = vi.fn();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method === "POST") {
        post();
      }
      const path = String(input);
      if (path.startsWith("/api/symbols?")) {
        return jsonResponse([]);
      }
      return jsonResponse({
        source_mode: "direct",
        archive_healthy: true,
        rest_healthy: true,
        worker_heartbeat_at: "2026-07-21T10:00:00Z",
        streams: [],
        checked_at: "2026-07-21T10:00:01Z",
      });
    }),
  );

  render(<SymbolsPage />);
  await screen.findByText("尚未监控任何币种");
  await user.click(screen.getByRole("button", { name: "添加币种" }));
  const form = screen.getByRole("form", { name: "添加监控币种" });
  await user.type(within(form).getByLabelText("币种代码"), "SOLUSDT");
  fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2024-01-01" } });
  fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2025-01-01" } });
  await user.click(within(form).getByRole("button", { name: "添加并开始回填" }));

  expect(await within(form).findByRole("alert")).toHaveTextContent("一次最多选择 366 个完整 UTC 自然日。");
  expect(post).not.toHaveBeenCalled();
});

test("keeps a configured symbol visible and retries only backfill after partial success", async () => {
  const user = userEvent.setup();
  const firstBackfill = deferred<Response>();
  let configured = false;
  let symbolPosts = 0;
  let backfillPosts = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (init?.method === "POST" && path === "/api/symbols") {
        symbolPosts += 1;
        configured = true;
        return jsonResponse({
          ...baseSymbol,
          symbol: "SOLUSDT",
          history_start: "2026-07-01T00:00:00Z",
          history_end: "2026-07-21T00:00:00Z",
          data_status: "requested",
          metadata_status: "metadata_unverified",
        });
      }
      if (init?.method === "POST" && path === "/api/symbols/SOLUSDT/backfills") {
        backfillPosts += 1;
        if (backfillPosts === 1) {
          return firstBackfill.promise;
        }
        return jsonResponse(["funding", "kline_1m", "mark_price"].map((dataType, index) => ({
          job_id: `00000000-0000-0000-0000-00000000051${index}`,
          symbol: "SOLUSDT",
          data_type: dataType,
          status: "queued",
          requested_start: "2026-07-01T00:00:00Z",
          requested_end: "2026-07-21T00:00:00Z",
          created_at: "2026-07-21T10:00:00Z",
          updated_at: "2026-07-21T10:00:00Z",
        })));
      }
      if (path.startsWith("/api/symbols?")) {
        return jsonResponse(configured ? [{
          ...baseSymbol,
          symbol: "SOLUSDT",
          history_start: "2026-07-01T00:00:00Z",
          history_end: "2026-07-21T00:00:00Z",
          data_status: backfillPosts >= 2 ? "backfilling" : "requested",
          metadata_status: "metadata_unverified",
        }] : []);
      }
      if (path === "/api/operations/market-data") {
        return jsonResponse({
          source_mode: "direct",
          archive_healthy: true,
          rest_healthy: false,
          worker_heartbeat_at: "2026-07-21T10:00:00Z",
          streams: [],
          checked_at: "2026-07-21T10:00:01Z",
        });
      }
      if (path.endsWith("/profile")) {
        return jsonResponse({ detail: "not ready" }, 404);
      }
      if (path.endsWith("/eligibility")) {
        return jsonResponse({
          symbol: "SOLUSDT",
          eligible: false,
          reason_codes: ["data_not_ready"],
          evaluated_at: "2026-07-21T10:00:00Z",
        });
      }
      if (path.includes("/partitions") || path.includes("/gaps") || path.includes("/streams")) {
        return jsonResponse([]);
      }
      throw new Error(`Unexpected mock request: ${path}`);
    }),
  );

  render(<SymbolsPage />);
  await screen.findByText("尚未监控任何币种");
  await user.click(screen.getByRole("button", { name: "添加币种" }));
  const form = screen.getByRole("form", { name: "添加监控币种" });
  await user.type(within(form).getByLabelText("币种代码"), "SOLUSDT");
  fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2026-06-01" } });
  fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2026-06-30" } });
  await user.click(within(form).getByRole("button", { name: "添加并开始回填" }));

  await waitFor(() => expect(backfillPosts).toBe(1));
  const collapse = screen.getByRole("button", { name: "收起添加表单" });
  expect(collapse).toBeDisabled();
  expect(screen.getByRole("status", { name: "添加表单已锁定" })).toHaveTextContent(
    "正在提交币种或等待回填恢复；为避免丢失仅重试入口，完成前无法收起此表单。",
  );
  firstBackfill.resolve(jsonResponse({ detail: "planner temporarily unavailable" }, 503));

  const partial = await within(form).findByRole("status", { name: "币种已添加，回填尚未启动" });
  expect(partial).toHaveTextContent("SOLUSDT 已加入监控；回填任务创建失败。可从此处安全重试，已有币种配置不会重复创建。");
  expect(await screen.findByRole("article", { name: "SOLUSDT 数据证据" })).toBeInTheDocument();
  expect(collapse).toBeDisabled();
  expect(symbolPosts).toBe(1);
  expect(backfillPosts).toBe(1);

  await user.click(within(partial).getByRole("button", { name: "仅重试创建回填任务" }));
  expect(await screen.findByRole("status", { name: "回填任务已创建" })).toHaveTextContent("已创建 3 个回填任务");
  const refreshedCard = await screen.findByRole("article", { name: "SOLUSDT 数据证据" });
  await waitFor(() => expect(within(refreshedCard).getByText(/采集状态/)).toHaveTextContent("回填中"));
  expect(collapse).toBeEnabled();
  expect(screen.queryByRole("status", { name: "添加表单已锁定" })).not.toBeInTheDocument();
  await user.click(collapse);
  expect(screen.queryByRole("form", { name: "添加监控币种" })).not.toBeInTheDocument();
  expect(symbolPosts).toBe(1);
  expect(backfillPosts).toBe(2);
});

test("aborts a protected add-and-backfill workflow if the page unmounts externally", async () => {
  const user = userEvent.setup();
  const pendingBackfill = deferred<Response>();
  let backfillSignal: AbortSignal | undefined;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (init?.method === "POST" && path === "/api/symbols") {
      return jsonResponse({
        ...baseSymbol,
        symbol: "SOLUSDT",
        data_status: "requested",
        metadata_status: "metadata_unverified",
      });
    }
    if (init?.method === "POST" && path === "/api/symbols/SOLUSDT/backfills") {
      backfillSignal = init.signal ?? undefined;
      return pendingBackfill.promise;
    }
    if (path.startsWith("/api/symbols?")) {
      return jsonResponse([]);
    }
    if (path === "/api/operations/market-data") {
      return jsonResponse({
        source_mode: "direct",
        archive_healthy: true,
        rest_healthy: true,
        worker_heartbeat_at: "2026-07-21T10:00:00Z",
        streams: [],
        checked_at: "2026-07-21T10:00:01Z",
      });
    }
    throw new Error(`Unexpected mock request: ${path}`);
  }));

  const view = render(<SymbolsPage />);
  await screen.findByText("尚未监控任何币种");
  await user.click(screen.getByRole("button", { name: "添加币种" }));
  const form = screen.getByRole("form", { name: "添加监控币种" });
  await user.type(within(form).getByLabelText("币种代码"), "SOLUSDT");
  fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2026-06-01" } });
  fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2026-06-30" } });
  await user.click(within(form).getByRole("button", { name: "添加并开始回填" }));

  await waitFor(() => expect(backfillSignal).toBeDefined());
  view.unmount();
  expect(backfillSignal?.aborted).toBe(true);
  pendingBackfill.resolve(jsonResponse([]));
});

test("requires a focused confirmation before disabling collection and preserves history", async () => {
  const user = userEvent.setup();
  const disabled: string[] = [];
  const disableRequest = deferred<Response>();
  const freshProfile = deferred<Response>();
  let collectionDisabled = false;
  let profileRefreshRequested = false;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (init?.method === "DELETE") {
        disabled.push(path);
        return disableRequest.promise;
      }
      if (collectionDisabled && path.endsWith("BTCUSDT/profile")) {
        profileRefreshRequested = true;
        return freshProfile.promise;
      }
      if (collectionDisabled && path.endsWith("BTCUSDT/eligibility")) {
        return jsonResponse({
          symbol: "BTCUSDT",
          eligible: false,
          reason_codes: ["data_not_ready"],
          evaluated_at: "2026-07-21T10:01:00Z",
        });
      }
      if (collectionDisabled && (
        path.includes("BTCUSDT/partitions") ||
        path.includes("BTCUSDT/gaps") ||
        path.includes("BTCUSDT/streams")
      )) {
        return jsonResponse([]);
      }
      return dashboardResponse(path);
    }),
  );

  render(<SymbolsPage />);
  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  const opener = within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" });
  await user.click(opener);

  let dialog = screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" });
  expect(within(dialog).getByText("只会停止新的采集与任务领取；已有历史、缺口记录和证据都会保留。")).toBeInTheDocument();
  let confirm = within(dialog).getByRole("button", { name: "确认停用" });
  expect(confirm).toHaveFocus();
  await user.tab();
  expect(within(dialog).getByRole("button", { name: "取消" })).toHaveFocus();
  await user.tab({ shift: true });
  expect(confirm).toHaveFocus();
  await user.keyboard("{Escape}");
  expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument();
  expect(opener).toHaveFocus();

  await user.click(opener);
  dialog = screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" });
  await user.click(within(dialog).getByRole("button", { name: "取消" }));
  expect(opener).toHaveFocus();

  await user.click(opener);
  dialog = screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" });
  confirm = within(dialog).getByRole("button", { name: "确认停用" });
  await user.click(confirm);

  expect(confirm).toHaveFocus();
  expect(confirm).not.toBeDisabled();
  expect(confirm).toHaveAttribute("aria-disabled", "true");
  await user.click(confirm);
  expect(disabled).toEqual(["/api/symbols/BTCUSDT"]);
  await user.tab();
  expect(within(dialog).getByRole("button", { name: "取消" })).toHaveFocus();
  await user.keyboard("{Escape}");
  expect(screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" })).toBeInTheDocument();
  await user.tab({ shift: true });
  expect(confirm).toHaveFocus();

  collectionDisabled = true;
  disableRequest.resolve(jsonResponse({
    ...baseSymbol,
    symbol: "BTCUSDT",
    enabled: false,
    data_status: "disabled",
  }));

  await waitFor(() => expect(profileRefreshRequested).toBe(true));
  expect(screen.queryByRole("alertdialog", { name: "确认停用 BTCUSDT" })).not.toBeInTheDocument();
  const updating = screen.getByRole("article", { name: "正在确认 BTCUSDT 最新数据" });
  expect(updating).toHaveFocus();
  freshProfile.resolve(jsonResponse({ detail: "not ready" }, 404));

  expect(await screen.findByRole("status", { name: "币种已停用" })).toHaveTextContent("BTCUSDT 已停用；历史数据已保留。");
  expect(disabled).toEqual(["/api/symbols/BTCUSDT"]);
  const disabledCard = screen.getByRole("article", { name: "BTCUSDT 数据证据" });
  expect(within(disabledCard).getByText("画像构建中")).toBeInTheDocument();
  expect(within(disabledCard).getByText("数据尚未就绪")).toBeInTheDocument();
  expect(within(disabledCard).queryByText("120,000")).not.toBeInTheDocument();
  expect(within(disabledCard).getByRole("button", { name: "重新启用 BTCUSDT 数据采集" })).toHaveFocus();
});

test("shows a symbol-scoped error and no success notice when post-disable evidence refresh fails", async () => {
  const user = userEvent.setup();
  let disabled = false;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (init?.method === "DELETE") {
      disabled = true;
      return jsonResponse({
        ...baseSymbol,
        symbol: "BTCUSDT",
        enabled: false,
        data_status: "disabled",
      });
    }
    if (disabled && path.endsWith("BTCUSDT/profile")) {
      return jsonResponse({ detail: "profile unavailable" }, 503);
    }
    return dashboardResponse(path);
  }));

  render(<SymbolsPage />);
  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  await user.click(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" }));
  await user.click(within(screen.getByRole("alertdialog")).getByRole("button", { name: "确认停用" }));

  const refreshError = await screen.findByRole("article", { name: "BTCUSDT 最新数据确认失败" });
  expect(refreshError).toHaveTextContent("BTCUSDT 的采集状态已经改变，但最新数据证据暂时无法确认。请重试；不会显示操作前的旧证据。");
  expect(within(refreshError).getByRole("button", { name: "重试确认 BTCUSDT 最新数据" })).toHaveFocus();
  expect(screen.queryByRole("status", { name: "币种已停用" })).not.toBeInTheDocument();
});

test("times out a never-settling post-disable refresh and recovers without repeating DELETE", async () => {
  const user = userEvent.setup();
  const neverSettles = deferred<Response>();
  let disabled = false;
  let deletes = 0;
  let refreshedProfiles = 0;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (init?.method === "DELETE") {
      deletes += 1;
      disabled = true;
      return jsonResponse({
        ...baseSymbol,
        symbol: "BTCUSDT",
        enabled: false,
        data_status: "disabled",
      });
    }
    if (disabled && path.endsWith("BTCUSDT/profile")) {
      refreshedProfiles += 1;
      return refreshedProfiles === 1
        ? neverSettles.promise
        : dashboardResponse(path);
    }
    return dashboardResponse(path);
  }));

  render(<SymbolsPage />);
  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  await user.click(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" }));
  const confirm = within(screen.getByRole("alertdialog")).getByRole("button", { name: "确认停用" });

  vi.useFakeTimers();
  await act(async () => {
    fireEvent.click(confirm);
    for (let index = 0; index < 10; index += 1) {
      await Promise.resolve();
    }
  });
  const updating = screen.getByRole("article", { name: "正在确认 BTCUSDT 最新数据" });
  expect(updating).toHaveFocus();

  await act(async () => {
    await vi.advanceTimersByTimeAsync(60_000);
  });
  const refreshError = screen.getByRole("article", { name: "BTCUSDT 最新数据确认失败" });
  const retry = within(refreshError).getByRole("button", { name: "重试确认 BTCUSDT 最新数据" });
  expect(retry).toHaveFocus();
  expect(deletes).toBe(1);

  await act(async () => {
    fireEvent.click(retry);
    for (let index = 0; index < 10; index += 1) {
      await Promise.resolve();
    }
  });
  const recovered = screen.getByRole("article", { name: "BTCUSDT 数据证据" });
  expect(within(recovered).getByRole("button", { name: "重新启用 BTCUSDT 数据采集" })).toHaveFocus();
  expect(deletes).toBe(1);
});

test.each(["delete-first", "list-first"] as const)(
  "keeps committed disable truth and focus through an add/backfill reload race: %s",
  async (raceOrder) => {
    const user = userEvent.setup();
    const deleteRequest = deferred<Response>();
    const staleReload = deferred<Response>();
    const backfillRequest = deferred<Response>();
    const disabledProfile = deferred<Response>();
    let configured = false;
    let collectionDisabled = false;
    let listRequests = 0;
    let symbolPosts = 0;
    let backfillPosts = 0;
    let deletes = 0;

    const staleSymbols = () => [{ ...baseSymbol, symbol: "BTCUSDT" }, ...(configured ? [{
      ...baseSymbol,
      symbol: "SOLUSDT",
      data_status: "requested",
      metadata_status: "metadata_unverified",
    }] : [])];

    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (init?.method === "POST" && path === "/api/symbols") {
        symbolPosts += 1;
        configured = true;
        return jsonResponse({
          ...baseSymbol,
          symbol: "SOLUSDT",
          data_status: "requested",
          metadata_status: "metadata_unverified",
        });
      }
      if (init?.method === "POST" && path === "/api/symbols/SOLUSDT/backfills") {
        backfillPosts += 1;
        return backfillRequest.promise;
      }
      if (init?.method === "DELETE" && path === "/api/symbols/BTCUSDT") {
        deletes += 1;
        return deleteRequest.promise;
      }
      if (path.startsWith("/api/symbols?")) {
        listRequests += 1;
        return listRequests === 2 ? staleReload.promise : jsonResponse(staleSymbols());
      }
      if (path === "/api/operations/market-data") {
        return dashboardResponse(path);
      }
      if (path.includes("SOLUSDT/profile")) {
        return jsonResponse({ detail: "not ready" }, 404);
      }
      if (path.includes("SOLUSDT/eligibility")) {
        return jsonResponse({
          symbol: "SOLUSDT",
          eligible: false,
          reason_codes: ["data_not_ready"],
          evaluated_at: "2026-07-21T10:00:00Z",
        });
      }
      if (path.includes("SOLUSDT/partitions") || path.includes("SOLUSDT/gaps") || path.includes("SOLUSDT/streams")) {
        return jsonResponse([]);
      }
      if (collectionDisabled && path.includes("BTCUSDT/profile")) {
        return disabledProfile.promise;
      }
      if (collectionDisabled && path.includes("BTCUSDT/eligibility")) {
        return jsonResponse({
          symbol: "BTCUSDT",
          eligible: false,
          reason_codes: ["data_not_ready"],
          evaluated_at: "2026-07-21T10:01:00Z",
        });
      }
      if (collectionDisabled && (
        path.includes("BTCUSDT/partitions") ||
        path.includes("BTCUSDT/gaps") ||
        path.includes("BTCUSDT/streams")
      )) {
        return jsonResponse([]);
      }
      return dashboardResponse(path);
    }));

    render(<SymbolsPage />);
    await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
    await user.click(screen.getByRole("button", { name: "添加币种" }));
    const form = screen.getByRole("form", { name: "添加监控币种" });
    await user.type(within(form).getByLabelText("币种代码"), "SOLUSDT");
    fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2026-06-01" } });
    fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2026-06-30" } });
    await user.click(within(form).getByRole("button", { name: "添加并开始回填" }));
    await waitFor(() => {
      expect(listRequests).toBe(2);
      expect(backfillPosts).toBe(1);
    });

    const btc = screen.getByRole("article", { name: "BTCUSDT 数据证据" });
    await user.click(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" }));
    await user.click(within(screen.getByRole("alertdialog")).getByRole("button", { name: "确认停用" }));

    if (raceOrder === "delete-first") {
      collectionDisabled = true;
      deleteRequest.resolve(jsonResponse({
        ...baseSymbol,
        symbol: "BTCUSDT",
        enabled: false,
        data_status: "disabled",
        updated_at: "2026-07-21T10:01:00Z",
      }));
      const updatingBeforeList = await screen.findByRole("article", { name: "正在确认 BTCUSDT 最新数据" });
      expect(updatingBeforeList).toHaveFocus();
      staleReload.resolve(jsonResponse(staleSymbols()));
    } else {
      staleReload.resolve(jsonResponse(staleSymbols()));
      await screen.findByRole("article", { name: "SOLUSDT 数据证据" });
      collectionDisabled = true;
      deleteRequest.resolve(jsonResponse({
        ...baseSymbol,
        symbol: "BTCUSDT",
        enabled: false,
        data_status: "disabled",
        updated_at: "2026-07-21T10:01:00Z",
      }));
    }

    const updating = await screen.findByRole("article", { name: "正在确认 BTCUSDT 最新数据" });
    expect(updating).toHaveFocus();
    const filter = screen.getByRole("searchbox", { name: "筛选已加载币种" });
    if (raceOrder === "list-first") {
      act(() => filter.focus());
      expect(filter).toHaveFocus();
    }
    backfillRequest.resolve(jsonResponse([{
      job_id: "00000000-0000-0000-0000-000000000520",
      symbol: "SOLUSDT",
      data_type: "kline_1m",
      status: "queued",
      requested_start: "2026-07-01T00:00:00Z",
      requested_end: "2026-07-21T00:00:00Z",
      created_at: "2026-07-21T10:00:00Z",
      updated_at: "2026-07-21T10:00:00Z",
    }]));
    await waitFor(() => expect(listRequests).toBe(3));
    const updatingAfterReload = screen.getByRole("article", { name: "正在确认 BTCUSDT 最新数据" });
    expect(updatingAfterReload).toBe(updating);
    if (raceOrder === "list-first") {
      expect(filter).toHaveFocus();
    } else {
      expect(updatingAfterReload).toHaveFocus();
    }

    disabledProfile.resolve(jsonResponse({ detail: "not ready" }, 404));
    const disabledCard = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
    expect(within(disabledCard).queryByText("符合数据启用条件")).not.toBeInTheDocument();
    const replacementAction = within(disabledCard).getByRole("button", { name: "重新启用 BTCUSDT 数据采集" });
    if (raceOrder === "list-first") {
      expect(filter).toHaveFocus();
      expect(replacementAction).not.toHaveFocus();
    } else {
      expect(replacementAction).toHaveFocus();
    }
    expect(await screen.findAllByRole("status", { name: "币种已停用" })).toHaveLength(1);
    expect(symbolPosts).toBe(1);
    expect(backfillPosts).toBe(1);
    expect(deletes).toBe(1);
  },
);

test("keeps focus with the most recent mutation when an earlier symbol finishes later", async () => {
  const user = userEvent.setup();
  const btcDelete = deferred<Response>();
  const pepeDelete = deferred<Response>();
  const btcProfile = deferred<Response>();
  const pepeProfile = deferred<Response>();
  const committed = new Set<string>();
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (init?.method === "DELETE" && path === "/api/symbols/BTCUSDT") {
      return btcDelete.promise;
    }
    if (init?.method === "DELETE" && path === "/api/symbols/1000PEPEUSDT") {
      return pepeDelete.promise;
    }
    const symbolName = path.includes("1000PEPEUSDT") ? "1000PEPEUSDT" : "BTCUSDT";
    if (committed.has(symbolName) && path.endsWith("/profile")) {
      return symbolName === "BTCUSDT" ? btcProfile.promise : pepeProfile.promise;
    }
    if (committed.has(symbolName) && path.endsWith("/eligibility")) {
      return jsonResponse({
        symbol: symbolName,
        eligible: false,
        reason_codes: ["data_not_ready"],
        evaluated_at: "2026-07-21T10:01:00Z",
      });
    }
    if (committed.has(symbolName) && (
      path.includes("/partitions") || path.includes("/gaps") || path.includes("/streams")
    )) {
      return jsonResponse([]);
    }
    return dashboardResponse(path);
  }));

  render(<SymbolsPage />);
  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  const pepe = screen.getByRole("article", { name: "1000PEPEUSDT 数据证据" });
  await user.click(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" }));
  await user.click(within(screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" })).getByRole("button", { name: "确认停用" }));
  await user.click(within(pepe).getByRole("button", { name: "停用 1000PEPEUSDT 数据采集" }));
  await user.click(within(screen.getByRole("alertdialog", { name: "确认停用 1000PEPEUSDT" })).getByRole("button", { name: "确认停用" }));

  committed.add("1000PEPEUSDT");
  pepeDelete.resolve(jsonResponse({
    ...baseSymbol,
    symbol: "1000PEPEUSDT",
    enabled: false,
    data_status: "disabled",
    updated_at: "2026-07-21T10:01:00Z",
  }));
  const pepeOwner = await screen.findByRole("article", { name: "正在确认 1000PEPEUSDT 最新数据" });
  expect(pepeOwner).toHaveFocus();

  committed.add("BTCUSDT");
  btcDelete.resolve(jsonResponse({
    ...baseSymbol,
    symbol: "BTCUSDT",
    enabled: false,
    data_status: "disabled",
    updated_at: "2026-07-21T10:01:00Z",
  }));
  await screen.findByRole("article", { name: "正在确认 BTCUSDT 最新数据" });
  expect(pepeOwner).toHaveFocus();

  btcProfile.resolve(jsonResponse({ detail: "not ready" }, 404));
  const disabledBtc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  expect(within(disabledBtc).getByRole("button", { name: "重新启用 BTCUSDT 数据采集" })).not.toHaveFocus();
  expect(pepeOwner).toHaveFocus();

  pepeProfile.resolve(jsonResponse({ detail: "not ready" }, 404));
  const disabledPepe = await screen.findByRole("article", { name: "1000PEPEUSDT 数据证据" });
  expect(within(disabledPepe).getByRole("button", { name: "重新启用 1000PEPEUSDT 数据采集" })).toHaveFocus();
});

test("keeps the newer successful notice when its evidence completes before an older symbol", async () => {
  const user = userEvent.setup();
  const btcDelete = deferred<Response>();
  const pepeDelete = deferred<Response>();
  const btcProfile = deferred<Response>();
  const pepeProfile = deferred<Response>();
  const committed = new Set<string>();
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (init?.method === "DELETE" && path === "/api/symbols/BTCUSDT") {
      return btcDelete.promise;
    }
    if (init?.method === "DELETE" && path === "/api/symbols/1000PEPEUSDT") {
      return pepeDelete.promise;
    }
    const symbolName = path.includes("1000PEPEUSDT") ? "1000PEPEUSDT" : "BTCUSDT";
    if (committed.has(symbolName) && path.endsWith("/profile")) {
      return symbolName === "BTCUSDT" ? btcProfile.promise : pepeProfile.promise;
    }
    if (committed.has(symbolName) && path.endsWith("/eligibility")) {
      return jsonResponse({
        symbol: symbolName,
        eligible: false,
        reason_codes: ["data_not_ready"],
        evaluated_at: "2026-07-21T10:01:00Z",
      });
    }
    if (committed.has(symbolName) && (
      path.includes("/partitions") || path.includes("/gaps") || path.includes("/streams")
    )) {
      return jsonResponse([]);
    }
    return dashboardResponse(path);
  }));

  render(<SymbolsPage />);
  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  const pepe = screen.getByRole("article", { name: "1000PEPEUSDT 数据证据" });
  await user.click(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" }));
  await user.click(within(screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" })).getByRole("button", { name: "确认停用" }));
  await user.click(within(pepe).getByRole("button", { name: "停用 1000PEPEUSDT 数据采集" }));
  await user.click(within(screen.getByRole("alertdialog", { name: "确认停用 1000PEPEUSDT" })).getByRole("button", { name: "确认停用" }));

  committed.add("1000PEPEUSDT");
  pepeDelete.resolve(jsonResponse({
    ...baseSymbol,
    symbol: "1000PEPEUSDT",
    enabled: false,
    data_status: "disabled",
    updated_at: "2026-07-21T10:02:00Z",
  }));
  pepeProfile.resolve(jsonResponse({ detail: "not ready" }, 404));

  expect(await screen.findByRole("status", { name: "币种已停用" })).toHaveTextContent(
    "1000PEPEUSDT 已停用；历史数据已保留。",
  );

  committed.add("BTCUSDT");
  btcDelete.resolve(jsonResponse({
    ...baseSymbol,
    symbol: "BTCUSDT",
    enabled: false,
    data_status: "disabled",
    updated_at: "2026-07-21T10:01:00Z",
  }));
  btcProfile.resolve(jsonResponse({ detail: "not ready" }, 404));

  await waitFor(() => expect(
    within(screen.getByRole("article", { name: "BTCUSDT 数据证据" }))
      .getByRole("button", { name: "重新启用 BTCUSDT 数据采集" }),
  ).toBeInTheDocument());
  await act(async () => {
    for (let index = 0; index < 10; index += 1) {
      await Promise.resolve();
    }
  });
  const notices = screen.getAllByRole("status", { name: "币种已停用" });
  expect(notices).toHaveLength(1);
  expect(notices[0]).toHaveTextContent("1000PEPEUSDT 已停用；历史数据已保留。");
  expect(notices[0]).not.toHaveTextContent("BTCUSDT");
});

test("does not let an older success overwrite a newer mutation failure", async () => {
  const user = userEvent.setup();
  const btcDelete = deferred<Response>();
  const pepeDelete = deferred<Response>();
  const btcProfile = deferred<Response>();
  let btcCommitted = false;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (init?.method === "DELETE" && path === "/api/symbols/BTCUSDT") {
      return btcDelete.promise;
    }
    if (init?.method === "DELETE" && path === "/api/symbols/1000PEPEUSDT") {
      return pepeDelete.promise;
    }
    if (btcCommitted && path.endsWith("BTCUSDT/profile")) {
      return btcProfile.promise;
    }
    if (btcCommitted && path.endsWith("BTCUSDT/eligibility")) {
      return jsonResponse({
        symbol: "BTCUSDT",
        eligible: false,
        reason_codes: ["data_not_ready"],
        evaluated_at: "2026-07-21T10:01:00Z",
      });
    }
    if (btcCommitted && (
      path.includes("BTCUSDT/partitions") || path.includes("BTCUSDT/gaps") || path.includes("BTCUSDT/streams")
    )) {
      return jsonResponse([]);
    }
    return dashboardResponse(path);
  }));

  render(<SymbolsPage />);
  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  const pepe = screen.getByRole("article", { name: "1000PEPEUSDT 数据证据" });
  await user.click(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" }));
  await user.click(within(screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" })).getByRole("button", { name: "确认停用" }));
  await user.click(within(pepe).getByRole("button", { name: "停用 1000PEPEUSDT 数据采集" }));
  await user.click(within(screen.getByRole("alertdialog", { name: "确认停用 1000PEPEUSDT" })).getByRole("button", { name: "确认停用" }));

  pepeDelete.resolve(jsonResponse({ detail: "temporarily unavailable" }, 503));
  const newerFailure = await screen.findByRole("alert");
  expect(newerFailure).toHaveTextContent("停用失败。采集状态未改变，请重试。");

  btcCommitted = true;
  btcDelete.resolve(jsonResponse({
    ...baseSymbol,
    symbol: "BTCUSDT",
    enabled: false,
    data_status: "disabled",
    updated_at: "2026-07-21T10:01:00Z",
  }));
  btcProfile.resolve(jsonResponse({ detail: "not ready" }, 404));

  await waitFor(() => expect(
    within(screen.getByRole("article", { name: "BTCUSDT 数据证据" }))
      .getByRole("button", { name: "重新启用 BTCUSDT 数据采集" }),
  ).toBeInTheDocument());
  await waitFor(() => {
    const alerts = screen.getAllByRole("alert");
    expect(alerts).toHaveLength(1);
    expect(alerts[0]).toHaveTextContent("停用失败。采集状态未改变，请重试。");
    expect(screen.queryByRole("status", { name: "币种已停用" })).not.toBeInTheDocument();
  });
});

test.each([
  ["strictly newer", "2026-07-21T10:02:00Z", false],
  ["invalid timestamp", "not-a-date", true],
] as const)("reconciles a settled success notice against a later %s opposite list row", async (_case, listedUpdatedAt, noticeRemains) => {
  const user = userEvent.setup();
  let disabled = false;
  let configured = false;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (init?.method === "DELETE" && path === "/api/symbols/BTCUSDT") {
      disabled = true;
      return jsonResponse({
        ...baseSymbol,
        symbol: "BTCUSDT",
        enabled: false,
        data_status: "disabled",
        updated_at: "2026-07-21T10:01:00Z",
      });
    }
    if (init?.method === "POST" && path === "/api/symbols") {
      configured = true;
      return jsonResponse({
        ...baseSymbol,
        symbol: "SOLUSDT",
        data_status: "requested",
        metadata_status: "metadata_unverified",
      });
    }
    if (init?.method === "POST" && path === "/api/symbols/SOLUSDT/backfills") {
      return jsonResponse([]);
    }
    if (path.startsWith("/api/symbols?")) {
      return jsonResponse([
        {
          ...baseSymbol,
          symbol: "BTCUSDT",
          enabled: configured ? true : !disabled,
          data_status: configured ? "data_ready" : disabled ? "disabled" : "data_ready",
          updated_at: configured ? listedUpdatedAt : disabled ? "2026-07-21T10:01:00Z" : baseSymbol.updated_at,
        },
        ...(configured ? [{
          ...baseSymbol,
          symbol: "SOLUSDT",
          data_status: "requested",
          metadata_status: "metadata_unverified",
        }] : []),
      ]);
    }
    if (path === "/api/operations/market-data") {
      return dashboardResponse(path);
    }
    if (path.includes("SOLUSDT/profile")) {
      return jsonResponse({ detail: "not ready" }, 404);
    }
    if (path.includes("SOLUSDT/eligibility")) {
      return jsonResponse({
        symbol: "SOLUSDT",
        eligible: false,
        reason_codes: ["data_not_ready"],
        evaluated_at: "2026-07-21T10:00:00Z",
      });
    }
    if (path.includes("SOLUSDT/partitions") || path.includes("SOLUSDT/gaps") || path.includes("SOLUSDT/streams")) {
      return jsonResponse([]);
    }
    if (disabled && !configured && path.includes("BTCUSDT/profile")) {
      return jsonResponse({ detail: "not ready" }, 404);
    }
    if (disabled && !configured && path.includes("BTCUSDT/eligibility")) {
      return jsonResponse({
        symbol: "BTCUSDT",
        eligible: false,
        reason_codes: ["data_not_ready"],
        evaluated_at: "2026-07-21T10:01:00Z",
      });
    }
    if (disabled && !configured && (
      path.includes("BTCUSDT/partitions") || path.includes("BTCUSDT/gaps") || path.includes("BTCUSDT/streams")
    )) {
      return jsonResponse([]);
    }
    return dashboardResponse(path);
  }));

  render(<SymbolsPage />);
  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  await user.click(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" }));
  await user.click(within(screen.getByRole("alertdialog")).getByRole("button", { name: "确认停用" }));
  expect(await screen.findByRole("status", { name: "币种已停用" })).toHaveTextContent(
    "BTCUSDT 已停用；历史数据已保留。",
  );

  await user.click(screen.getByRole("button", { name: "添加币种" }));
  const form = screen.getByRole("form", { name: "添加监控币种" });
  await user.type(within(form).getByLabelText("币种代码"), "SOLUSDT");
  fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2026-06-01" } });
  fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2026-06-30" } });
  await user.click(within(form).getByRole("button", { name: "添加并开始回填" }));

  const enabledBtc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  if (noticeRemains) {
    expect(screen.getByRole("status", { name: "币种已停用" })).toHaveTextContent(
      "BTCUSDT 已停用；历史数据已保留。",
    );
    expect(within(enabledBtc).getByRole("button", { name: "重新启用 BTCUSDT 数据采集" })).toBeInTheDocument();
  } else {
    await waitFor(() => expect(screen.queryByRole("status", { name: "币种已停用" })).not.toBeInTheDocument());
    expect(within(enabledBtc).getByRole("button", { name: "停用 BTCUSDT 数据采集" })).toBeInTheDocument();
  }
});

test("suppresses committed notice and focus when a newer opposite server state wins", async () => {
  const user = userEvent.setup();
  const newerList = deferred<Response>();
  const disabledProfile = deferred<Response>();
  const backfillRequest = deferred<Response>();
  let configured = false;
  let disabledCommitted = false;
  let serverSuperseded = false;
  let listRequests = 0;
  let deletes = 0;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (init?.method === "POST" && path === "/api/symbols") {
      configured = true;
      return jsonResponse({
        ...baseSymbol,
        symbol: "SOLUSDT",
        data_status: "requested",
        metadata_status: "metadata_unverified",
      });
    }
    if (init?.method === "POST" && path === "/api/symbols/SOLUSDT/backfills") {
      return backfillRequest.promise;
    }
    if (init?.method === "DELETE" && path === "/api/symbols/BTCUSDT") {
      deletes += 1;
      disabledCommitted = true;
      return jsonResponse({
        ...baseSymbol,
        symbol: "BTCUSDT",
        enabled: false,
        data_status: "disabled",
        updated_at: "2026-07-21T10:01:00Z",
      });
    }
    if (path.startsWith("/api/symbols?")) {
      listRequests += 1;
      if (listRequests === 2) {
        return newerList.promise;
      }
      return jsonResponse([{ ...baseSymbol, symbol: "BTCUSDT" }]);
    }
    if (path === "/api/operations/market-data") {
      return dashboardResponse(path);
    }
    if (path.includes("SOLUSDT/profile")) {
      return jsonResponse({ detail: "not ready" }, 404);
    }
    if (path.includes("SOLUSDT/eligibility")) {
      return jsonResponse({
        symbol: "SOLUSDT",
        eligible: false,
        reason_codes: ["data_not_ready"],
        evaluated_at: "2026-07-21T10:00:00Z",
      });
    }
    if (path.includes("SOLUSDT/partitions") || path.includes("SOLUSDT/gaps") || path.includes("SOLUSDT/streams")) {
      return jsonResponse([]);
    }
    if (disabledCommitted && !serverSuperseded && path.includes("BTCUSDT/profile")) {
      return disabledProfile.promise;
    }
    return dashboardResponse(path);
  }));

  render(<SymbolsPage />);
  await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  await user.click(screen.getByRole("button", { name: "添加币种" }));
  const form = screen.getByRole("form", { name: "添加监控币种" });
  await user.type(within(form).getByLabelText("币种代码"), "SOLUSDT");
  fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2026-06-01" } });
  fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2026-06-30" } });
  await user.click(within(form).getByRole("button", { name: "添加并开始回填" }));
  await waitFor(() => expect(listRequests).toBe(2));

  const btc = screen.getByRole("article", { name: "BTCUSDT 数据证据" });
  await user.click(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" }));
  await user.click(within(screen.getByRole("alertdialog")).getByRole("button", { name: "确认停用" }));
  const updating = await screen.findByRole("article", { name: "正在确认 BTCUSDT 最新数据" });
  expect(updating).toHaveFocus();

  serverSuperseded = true;
  newerList.resolve(jsonResponse([
    { ...baseSymbol, symbol: "BTCUSDT", updated_at: "2026-07-21T10:02:00Z" },
    {
      ...baseSymbol,
      symbol: "SOLUSDT",
      data_status: "requested",
      metadata_status: "metadata_unverified",
    },
  ]));
  const enabledBtc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  const enabledAction = within(enabledBtc).getByRole("button", { name: "停用 BTCUSDT 数据采集" });
  expect(enabledAction).not.toHaveFocus();
  expect(screen.queryByRole("status", { name: "币种已停用" })).not.toBeInTheDocument();

  disabledProfile.resolve(jsonResponse({ detail: "not ready" }, 404));
  backfillRequest.resolve(jsonResponse({ detail: "planner unavailable" }, 503));
  await waitFor(() => expect(screen.queryByRole("status", { name: "币种已停用" })).not.toBeInTheDocument());
  expect(deletes).toBe(1);
  expect(configured).toBe(true);
});

test("re-enables a disabled symbol with its immutable history identity and no automatic backfill", async () => {
  const user = userEvent.setup();
  const posts: Array<{ path: string; body: unknown }> = [];
  let enabled = false;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (init?.method === "POST") {
        posts.push({ path, body: JSON.parse(String(init.body)) as unknown });
        enabled = true;
        return jsonResponse({
          ...baseSymbol,
          symbol: "BTCUSDT",
          enabled: true,
          history_start: "2026-07-01T00:00:00Z",
          history_end: "2026-07-21T00:00:00Z",
        });
      }
      if (path.startsWith("/api/symbols?")) {
        return jsonResponse([{
          ...baseSymbol,
          symbol: "BTCUSDT",
          enabled: false,
          data_status: "disabled",
          history_start: "2026-07-01T00:00:00Z",
          history_end: "2026-07-21T00:00:00Z",
        }]);
      }
      if (!enabled && path.endsWith("BTCUSDT/profile")) {
        return jsonResponse({ detail: "not ready" }, 404);
      }
      if (!enabled && path.endsWith("BTCUSDT/eligibility")) {
        return jsonResponse({
          symbol: "BTCUSDT",
          eligible: false,
          reason_codes: ["data_not_ready"],
          evaluated_at: "2026-07-21T10:00:00Z",
        });
      }
      if (!enabled && (
        path.includes("BTCUSDT/partitions") ||
        path.includes("BTCUSDT/gaps") ||
        path.includes("BTCUSDT/streams")
      )) {
        return jsonResponse([]);
      }
      return dashboardResponse(path);
    }),
  );

  render(<SymbolsPage />);

  const btc = await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  expect(within(btc).getByText("重新启用只会恢复采集，并复用原历史区间；不会自动创建重复回填任务。")).toBeInTheDocument();
  await user.click(within(btc).getByRole("button", { name: "重新启用 BTCUSDT 数据采集" }));

  expect(await screen.findByRole("status", { name: "币种已重新启用" })).toHaveTextContent("BTCUSDT 已重新启用；未自动创建回填任务。");
  expect(posts).toEqual([{
    path: "/api/symbols",
    body: {
      symbol: "BTCUSDT",
      history_start: "2026-07-01T00:00:00Z",
      history_end: "2026-07-21T00:00:00Z",
      include_agg_trades: false,
    },
  }]);
  expect(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" })).toBeInTheDocument();
  expect(within(btc).getByText("120,000")).toBeInTheDocument();
  expect(within(btc).getByText("符合数据启用条件")).toBeInTheDocument();
  expect(within(btc).queryByText("数据尚未就绪")).not.toBeInTheDocument();
});

test("filters the full symbol grid from the title and action row", async () => {
  const user = userEvent.setup();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => dashboardResponse(String(input))),
  );

  render(<SymbolsPage />);
  await screen.findByRole("article", { name: "BTCUSDT 数据证据" });

  const filter = screen.getByRole("searchbox", { name: "筛选已加载币种" });
  await user.type(filter, "pepe");

  expect(screen.queryByRole("article", { name: "BTCUSDT 数据证据" })).not.toBeInTheDocument();
  expect(screen.getByRole("article", { name: "1000PEPEUSDT 数据证据" })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "添加币种" })).toBeInTheDocument();
});

test("states that a truncated symbol filter searches only the loaded first 100 items", async () => {
  const user = userEvent.setup();
  const symbols = Array.from({ length: 100 }, (_, index) => ({
    ...baseSymbol,
    symbol: `S${String(index).padStart(3, "0")}USDT`,
  }));
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
    const path = String(input);
    if (path.startsWith("/api/symbols?")) {
      return Promise.resolve(jsonResponse(symbols));
    }
    if (path === "/api/operations/market-data") {
      return Promise.resolve(dashboardResponse(path));
    }
    return new Promise<Response>(() => undefined);
  }));

  render(<SymbolsPage />);

  expect(await screen.findByText("当前仅加载并显示前 100 个币种；筛选只搜索这 100 个已加载项，不会搜索其余配置。")).toBeInTheDocument();
  await user.type(screen.getByRole("searchbox", { name: "筛选已加载币种" }), "BTC");
  expect(screen.getByRole("status", { name: "没有匹配的币种" })).toHaveTextContent(
    "当前筛选在已加载的前 100 个币种中没有匹配；其余配置尚未检查。",
  );
});

test("shows a filtered-empty state and clears the filter without implying the catalog is empty", async () => {
  const user = userEvent.setup();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => dashboardResponse(String(input))),
  );

  render(<SymbolsPage />);
  await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  await user.type(screen.getByRole("searchbox", { name: "筛选已加载币种" }), "doge");

  const empty = screen.getByRole("status", { name: "没有匹配的币种" });
  expect(empty).toHaveTextContent("当前筛选没有匹配项；已监控币种并未被删除。");
  await user.click(within(empty).getByRole("button", { name: "清除筛选" }));
  expect(screen.getByRole("article", { name: "BTCUSDT 数据证据" })).toBeInTheDocument();
  expect(screen.getByRole("article", { name: "1000PEPEUSDT 数据证据" })).toBeInTheDocument();
});

test("keeps the filter and primary action reachable in keyboard order", async () => {
  const user = userEvent.setup();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => dashboardResponse(String(input))),
  );

  render(<SymbolsPage />);
  await screen.findByRole("article", { name: "BTCUSDT 数据证据" });

  await user.tab();
  expect(screen.getByRole("searchbox", { name: "筛选已加载币种" })).toHaveFocus();
  await user.tab();
  expect(screen.getByRole("button", { name: "添加币种" })).toHaveFocus();
});
