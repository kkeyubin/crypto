import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, test, vi } from "vitest";
import { SymbolsPage } from "./SymbolsPage";
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
    return jsonResponse([{
      symbol: isBtc ? "BTCUSDT" : "PEPEUSDT",
      stream_name: isBtc ? "btcusdt@kline_1m" : "pepeusdt@kline_1m",
      status: "connected",
      last_event_at: isBtc ? "2026-07-21T09:59:59Z" : "2026-07-21T09:59:55Z",
      updated_at: "2026-07-21T10:00:00Z",
    }]);
  }
  throw new Error(`Unexpected mock request: ${path}`);
}

beforeEach(async () => {
  await i18n.changeLanguage("zh-CN");
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

  expect(within(btc).getByText(/最后事件/)).toHaveTextContent("2026");
  expect(within(pepe).getByText(/最后事件/)).toHaveTextContent("2026");
});

test("adds a symbol with an explicit UTC closed range and double aggTrades opt-in", async () => {
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
          requested_end: "2026-07-20T23:59:00Z",
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
          requested_end: "2026-07-20T23:59:00Z",
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
  expect(within(form).getByText("UTC 闭区间 [开始, 结束]；结束时刻包含在回填范围内。")).toBeInTheDocument();
  expect(within(form).getByText(/aggTrades 历史体量很大/)).toBeInTheDocument();
  expect(within(form).getByText("此操作只配置数据采集，不会启用交易、通知或 AI 下单。")).toBeInTheDocument();

  await user.type(within(form).getByLabelText("币种代码"), "SOLUSDT");
  fireEvent.change(within(form).getByLabelText("历史开始（UTC）"), { target: { value: "2026-07-01T00:00" } });
  fireEvent.change(within(form).getByLabelText("历史结束（UTC，包含）"), { target: { value: "2026-07-20T23:59" } });
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
        history_end: "2026-07-20T23:59:00.000Z",
        include_agg_trades: true,
      },
    },
    {
      path: "/api/symbols/SOLUSDT/backfills",
      body: {
        symbol: "SOLUSDT",
        data_types: ["kline_1m", "mark_price", "funding", "agg_trade"],
        start: "2026-07-01T00:00:00.000Z",
        end: "2026-07-20T23:59:00.000Z",
        include_agg_trades: true,
      },
    },
  ]);
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
  await user.click(within(btc).getByRole("button", { name: "停用 BTCUSDT 数据采集" }));

  const dialog = screen.getByRole("alertdialog", { name: "确认停用 BTCUSDT" });
  expect(within(dialog).getByText("只会停止新的采集与任务领取；已有历史、缺口记录和证据都会保留。")).toBeInTheDocument();
  const confirm = within(dialog).getByRole("button", { name: "确认停用" });
  await waitFor(() => expect(confirm).toHaveFocus());
  await user.click(confirm);

  expect(await screen.findByRole("status", { name: "币种已停用" })).toHaveTextContent("BTCUSDT 已停用；历史数据已保留。");
  expect(disabled).toEqual(["/api/symbols/BTCUSDT"]);
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
