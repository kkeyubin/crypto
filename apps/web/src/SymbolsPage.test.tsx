import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, test, vi } from "vitest";
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
        symbol: "PEPEUSDT",
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
      symbol: "PEPEUSDT",
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
      symbol: isBtc ? "BTCUSDT" : "PEPEUSDT",
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
      symbol: isBtc ? "BTCUSDT" : "PEPEUSDT",
      eligible: isBtc,
      reason_codes: isBtc ? [] : ["insufficient_coverage", "unrepaired_gap"],
      evaluated_at: "2026-07-21T10:00:00Z",
    });
  }
  if (path.includes("/streams")) {
    const symbol = isBtc ? "BTCUSDT" : "PEPEUSDT";
    const suffixes = ["aggtrade", "bookticker", "kline_1m", "markprice@1s"];
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

test("allows exactly 366 inclusive UTC days and converts the inclusive end to the next midnight", () => {
  expect(toArchiveUtcRange("2024-01-01", "2024-12-31", new Date("2026-07-21T12:00:00Z"))).toEqual({
    ok: true,
    start: "2024-01-01T00:00:00.000Z",
    end: "2025-01-01T00:00:00.000Z",
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
  const pepe = await screen.findByRole("article", { name: "PEPEUSDT 数据证据" });

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

test("shows backend stale status while retaining the oldest exact required-stream event", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.endsWith("PEPEUSDT/eligibility")) {
        return jsonResponse({
          symbol: "PEPEUSDT",
          eligible: false,
          reason_codes: ["stale_live_data"],
          evaluated_at: "2026-07-21T10:00:00Z",
        });
      }
      if (path.includes("PEPEUSDT/streams")) {
        return jsonResponse(["aggtrade", "bookticker", "kline_1m", "markprice@1s"].map((suffix, index) => ({
          symbol: "PEPEUSDT",
          stream_name: `pepeusdt@${suffix}`,
          status: "connected",
          last_event_at: `2026-07-21T09:40:0${index}Z`,
          updated_at: "2026-07-21T10:00:00Z",
        })));
      }
      return dashboardResponse(path);
    }),
  );

  render(<SymbolsPage />);

  const pepe = await screen.findByRole("article", { name: "PEPEUSDT 数据证据" });
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
      if (path.endsWith("PEPEUSDT/eligibility")) {
        return jsonResponse({
          symbol: "PEPEUSDT",
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
  expect(screen.getByRole("article", { name: "PEPEUSDT 数据加载失败" })).toBeInTheDocument();
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
      if (path.endsWith("PEPEUSDT/profile")) {
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
  const pepeError = await screen.findByRole("article", { name: "PEPEUSDT 数据加载失败" });
  expect(within(btc).getByText("120,000")).toBeInTheDocument();
  expect(within(pepeError).getByText("暂时无法加载 PEPEUSDT 的数据证据。")).toBeInTheDocument();

  await user.click(within(pepeError).getByRole("button", { name: "重试 PEPEUSDT" }));
  expect(await screen.findByRole("article", { name: "PEPEUSDT 数据证据" })).toBeInTheDocument();
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

test("adds a symbol with inclusive UTC days converted to a planner-compatible half-open range", async () => {
  const user = userEvent.setup();
  const requests: Array<{ path: string; body: unknown }> = [];
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
            symbol: "SOLUSDT",
            data_status: "requested",
            metadata_status: "metadata_unverified",
            include_agg_trades: true,
          });
        }
        return jsonResponse(["kline_1m", "mark_price", "funding", "agg_trade"].map((dataType, index) => ({
          job_id: `00000000-0000-0000-0000-00000000040${index}`,
          symbol: "SOLUSDT",
          data_type: dataType,
          status: index === 0 ? "running" : "queued",
          requested_start: "2026-07-01T00:00:00Z",
          requested_end: "2026-07-21T00:00:00Z",
          created_at: "2026-07-21T10:00:00Z",
          updated_at: "2026-07-21T10:00:00Z",
        })));
      }
      if (path.startsWith("/api/backfills/")) {
        const jobIndex = Number(path.at(-1));
        const dataType = ["kline_1m", "mark_price", "funding", "agg_trade"][jobIndex];
        return jsonResponse({
          job_id: path.split("/").at(-1),
          symbol: "SOLUSDT",
          data_type: dataType,
          status: "succeeded",
          requested_start: "2026-07-01T00:00:00Z",
          requested_end: "2026-07-21T00:00:00Z",
          created_at: "2026-07-21T10:00:00Z",
          updated_at: "2026-07-21T10:05:00Z",
        });
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
  await screen.findByText("尚未监控任何币种");
  await user.click(screen.getByRole("button", { name: "添加币种" }));

  const form = screen.getByRole("form", { name: "添加监控币种" });
  expect(within(form).getByText("按 UTC 自然日选择，开始日从 00:00Z 起，结束日包含整天；提交给服务的是 [开始日 00:00Z, 结束日次日 00:00Z) 半开区间。")).toBeInTheDocument();
  expect(within(form).getByText(/aggTrades 历史体量很大/)).toBeInTheDocument();
  expect(within(form).getByText("此操作只配置数据采集，不会启用交易、通知或 AI 下单。")).toBeInTheDocument();

  await user.type(within(form).getByLabelText("币种代码"), "SOLUSDT");
  fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2026-07-01" } });
  fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2026-07-20" } });
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
  await user.click(within(progress).getByRole("button", { name: "刷新回填进度" }));
  expect(await within(progress).findAllByText("已完成")).toHaveLength(4);
  expect(requests).toEqual([
    {
      path: "/api/symbols",
      body: {
        symbol: "SOLUSDT",
        history_start: "2026-07-01T00:00:00.000Z",
        history_end: "2026-07-21T00:00:00.000Z",
        include_agg_trades: true,
      },
    },
    {
      path: "/api/symbols/SOLUSDT/backfills",
      body: {
        symbol: "SOLUSDT",
        data_types: ["kline_1m", "mark_price", "funding", "agg_trade"],
        start: "2026-07-01T00:00:00.000Z",
        end: "2026-07-21T00:00:00.000Z",
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
          return jsonResponse({ detail: "planner temporarily unavailable" }, 503);
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
          data_status: "requested",
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
  fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2026-07-01" } });
  fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2026-07-20" } });
  await user.click(within(form).getByRole("button", { name: "添加并开始回填" }));

  const partial = await within(form).findByRole("status", { name: "币种已添加，回填尚未启动" });
  expect(partial).toHaveTextContent("SOLUSDT 已加入监控；回填任务创建失败。可从此处安全重试，已有币种配置不会重复创建。");
  expect(await screen.findByRole("article", { name: "SOLUSDT 数据证据" })).toBeInTheDocument();
  expect(symbolPosts).toBe(1);
  expect(backfillPosts).toBe(1);

  await user.click(within(partial).getByRole("button", { name: "仅重试创建回填任务" }));
  expect(await screen.findByRole("status", { name: "回填任务已创建" })).toHaveTextContent("已创建 3 个回填任务");
  expect(symbolPosts).toBe(1);
  expect(backfillPosts).toBe(2);
});

test("requires a focused confirmation before disabling collection and preserves history", async () => {
  const user = userEvent.setup();
  const disabled: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (init?.method === "DELETE") {
        disabled.push(path);
        return jsonResponse({
          ...baseSymbol,
          symbol: "BTCUSDT",
          enabled: false,
          data_status: "disabled",
        });
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
  await waitFor(() => expect(confirm).toHaveFocus());
  await user.tab();
  expect(within(dialog).getByRole("button", { name: "取消" })).toHaveFocus();
  await user.tab({ shift: true });
  expect(confirm).toHaveFocus();
  await user.keyboard("{Escape}");
  expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument();
  await waitFor(() => expect(opener).toHaveFocus());

  await user.click(opener);
  dialog = screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" });
  await user.click(within(dialog).getByRole("button", { name: "取消" }));
  await waitFor(() => expect(opener).toHaveFocus());

  await user.click(opener);
  dialog = screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" });
  confirm = within(dialog).getByRole("button", { name: "确认停用" });
  await user.click(confirm);

  expect(await screen.findByRole("status", { name: "币种已停用" })).toHaveTextContent("BTCUSDT 已停用；历史数据已保留。");
  expect(disabled).toEqual(["/api/symbols/BTCUSDT"]);
  await waitFor(() => expect(screen.getByRole("button", { name: "重新启用 BTCUSDT 数据采集" })).toHaveFocus());
});

test("re-enables a disabled symbol with its immutable history identity and no automatic backfill", async () => {
  const user = userEvent.setup();
  const posts: Array<{ path: string; body: unknown }> = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (init?.method === "POST") {
        posts.push({ path, body: JSON.parse(String(init.body)) as unknown });
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
});

test("filters the full symbol grid from the title and action row", async () => {
  const user = userEvent.setup();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => dashboardResponse(String(input))),
  );

  render(<SymbolsPage />);
  await screen.findByRole("article", { name: "BTCUSDT 数据证据" });

  const filter = screen.getByRole("searchbox", { name: "筛选币种" });
  await user.type(filter, "pepe");

  expect(screen.queryByRole("article", { name: "BTCUSDT 数据证据" })).not.toBeInTheDocument();
  expect(screen.getByRole("article", { name: "PEPEUSDT 数据证据" })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "添加币种" })).toBeInTheDocument();
});

test("shows a filtered-empty state and clears the filter without implying the catalog is empty", async () => {
  const user = userEvent.setup();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => dashboardResponse(String(input))),
  );

  render(<SymbolsPage />);
  await screen.findByRole("article", { name: "BTCUSDT 数据证据" });
  await user.type(screen.getByRole("searchbox", { name: "筛选币种" }), "doge");

  const empty = screen.getByRole("status", { name: "没有匹配的币种" });
  expect(empty).toHaveTextContent("当前筛选没有匹配项；已监控币种并未被删除。");
  await user.click(within(empty).getByRole("button", { name: "清除筛选" }));
  expect(screen.getByRole("article", { name: "BTCUSDT 数据证据" })).toBeInTheDocument();
  expect(screen.getByRole("article", { name: "PEPEUSDT 数据证据" })).toBeInTheDocument();
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
  expect(screen.getByRole("searchbox", { name: "筛选币种" })).toHaveFocus();
  await user.tab();
  expect(screen.getByRole("button", { name: "添加币种" })).toHaveFocus();
});
