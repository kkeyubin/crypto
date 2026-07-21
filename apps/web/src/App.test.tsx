import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, test, vi } from "vitest";
import { App } from "./App";
import i18n, { resolveInitialLocale } from "./i18n";
import styles from "./styles.css?raw";

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as Response;
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

beforeEach(async () => {
  localStorage.clear();
  window.history.replaceState(null, "", "/");
  document.documentElement.lang = "zh-CN";
  await i18n.changeLanguage("zh-CN");
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ status: "ok", service: "api", version: "0.1.0" }),
    }),
  );
});

test("defaults to Chinese and exposes API health without color alone", async () => {
  render(<App />);

  expect(screen.getByRole("heading", { name: "指挥台" })).toBeInTheDocument();
  expect(screen.getByText("Phase 0 界面骨架")).toBeInTheDocument();
  await waitFor(() => expect(screen.getByText("API 正常")).toBeInTheDocument());
  expect(screen.getByLabelText("API 状态：正常")).toBeInTheDocument();
});

test("persists an explicit English selection and updates the document language", async () => {
  const user = userEvent.setup();
  render(<App />);

  await user.click(screen.getByRole("button", { name: "切换为英文" }));

  expect(screen.getByRole("heading", { name: "Command Center" })).toBeInTheDocument();
  expect(localStorage.getItem("crypto-locale")).toBe("en");
  expect(document.documentElement.lang).toBe("en");
});

test("keeps navigation accessible and marks the active shell view", () => {
  render(<App />);

  expect(screen.getByRole("navigation", { name: "主导航" })).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "总览", current: "page" })).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "币种" })).toHaveAttribute("href", "#symbols");
  expect(screen.getByRole("button", { name: "策略（尚未开放）" })).toBeDisabled();
});

test("enables the symbols navigation and marks the data console as current", async () => {
  const user = userEvent.setup();
  render(<App />);

  const symbolsLink = screen.getByRole("link", { name: "币种" });
  await user.click(symbolsLink);

  expect(screen.getByRole("heading", { name: "币种与数据", level: 1 })).toBeInTheDocument();
  expect(symbolsLink).toHaveAttribute("aria-current", "page");
  expect(screen.getByRole("button", { name: "策略（尚未开放）" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "回测（尚未开放）" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "模拟盘（尚未开放）" })).toBeDisabled();
});

test("keeps a protected backfill recovery mounted across normal App navigation", async () => {
  const user = userEvent.setup();
  const firstBackfill = deferred<Response>();
  let configured = false;
  let listRequests = 0;
  let symbolPosts = 0;
  let backfillPosts = 0;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path === "/api/health/live") {
      return jsonResponse({ status: "ok", service: "api", version: "0.1.0" });
    }
    if (init?.method === "POST" && path === "/api/symbols") {
      symbolPosts += 1;
      configured = true;
      return jsonResponse({
        symbol: "SOLUSDT",
        enabled: true,
        history_start: "2026-07-01T00:00:00Z",
        history_end: "2026-07-21T00:00:00Z",
        include_agg_trades: false,
        data_status: "requested",
        metadata_status: "metadata_unverified",
        created_at: "2026-07-21T10:00:00Z",
        updated_at: "2026-07-21T10:00:00Z",
      });
    }
    if (init?.method === "POST" && path === "/api/symbols/SOLUSDT/backfills") {
      backfillPosts += 1;
      if (backfillPosts === 1) {
        return firstBackfill.promise;
      }
      return jsonResponse([{
        job_id: "00000000-0000-0000-0000-000000000510",
        symbol: "SOLUSDT",
        data_type: "kline_1m",
        status: "queued",
        requested_start: "2026-07-01T00:00:00Z",
        requested_end: "2026-07-21T00:00:00Z",
        created_at: "2026-07-21T10:00:00Z",
        updated_at: "2026-07-21T10:00:00Z",
      }]);
    }
    if (path.startsWith("/api/symbols?")) {
      listRequests += 1;
      return jsonResponse(configured ? [{
        symbol: "SOLUSDT",
        enabled: true,
        history_start: "2026-07-01T00:00:00Z",
        history_end: "2026-07-21T00:00:00Z",
        include_agg_trades: false,
        data_status: "requested",
        metadata_status: "metadata_unverified",
        created_at: "2026-07-21T10:00:00Z",
        updated_at: "2026-07-21T10:00:00Z",
      }] : []);
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
  }));

  render(<App />);
  await screen.findByText("API 正常");
  expect(listRequests).toBe(0);
  expect(document.querySelectorAll("#overview")).toHaveLength(1);
  expect(document.querySelectorAll("#symbols")).toHaveLength(0);

  await user.click(screen.getByRole("link", { name: "币种" }));
  await screen.findByText("尚未监控任何币种");
  await user.click(screen.getByRole("button", { name: "添加币种" }));
  const form = screen.getByRole("form", { name: "添加监控币种" });
  await user.type(within(form).getByLabelText("币种代码"), "SOLUSDT");
  fireEvent.change(within(form).getByLabelText("历史开始日（UTC）"), { target: { value: "2026-07-01" } });
  fireEvent.change(within(form).getByLabelText("历史结束日（UTC，包含整天）"), { target: { value: "2026-07-20" } });
  await user.click(within(form).getByRole("button", { name: "添加并开始回填" }));
  await waitFor(() => expect(backfillPosts).toBe(1));

  await user.click(screen.getByRole("link", { name: "总览" }));
  expect(screen.getByRole("heading", { name: "指挥台" })).toBeInTheDocument();
  expect(screen.queryByRole("form", { name: "添加监控币种" })).not.toBeInTheDocument();
  expect(document.querySelectorAll("#symbols")).toHaveLength(1);
  expect(document.getElementById("symbols")).toHaveAttribute("hidden");
  firstBackfill.resolve(jsonResponse({ detail: "planner temporarily unavailable" }, 503));

  await user.click(screen.getByRole("link", { name: "币种" }));
  const recoveredForm = await screen.findByRole("form", { name: "添加监控币种" });
  const partial = within(recoveredForm).getByRole("status", { name: "币种已添加，回填尚未启动" });
  expect(within(recoveredForm).getByLabelText("币种代码")).toHaveValue("SOLUSDT");
  expect(within(partial).getByRole("button", { name: "仅重试创建回填任务" })).toBeInTheDocument();
  expect(symbolPosts).toBe(1);

  await user.click(within(partial).getByRole("button", { name: "仅重试创建回填任务" }));
  expect(await within(recoveredForm).findByRole("status", { name: "回填任务已创建" })).toBeInTheDocument();
  expect(symbolPosts).toBe(1);
  expect(backfillPosts).toBe(2);
});

test("translates the symbols title, filter, and action after a persisted English switch", async () => {
  const user = userEvent.setup();
  render(<App />);

  await user.click(screen.getByRole("link", { name: "币种" }));
  await user.click(screen.getByRole("button", { name: "切换为英文" }));

  expect(screen.getByRole("heading", { name: "Symbols & Data" })).toBeInTheDocument();
  expect(screen.getByRole("searchbox", { name: "Filter loaded symbols" })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Add symbol" })).toBeInTheDocument();
  expect(localStorage.getItem("crypto-locale")).toBe("en");
});

test("localizes header and locale controls", () => {
  render(<App />);

  expect(screen.getByRole("link", { name: "加密研究首页" })).toBeInTheDocument();
  expect(screen.getByRole("group", { name: "语言" })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "切换为英文" })).toBeInTheDocument();
});

test("resolves only an explicit English preference to English", () => {
  expect(resolveInitialLocale("en")).toBe("en");
  expect(resolveInitialLocale(null)).toBe("zh-CN");
  expect(resolveInitialLocale("fr")).toBe("zh-CN");
});

test("stacks header, navigation, and content into explicit mobile grid rows", () => {
  const mobileStart = styles.indexOf("@media (max-width: 820px)");
  const mobileEnd = styles.indexOf("@media (max-width: 480px)", mobileStart);
  const mobileLayout = styles.slice(mobileStart, mobileEnd);

  expect(mobileLayout).toContain("grid-template-rows: 64px auto minmax(0, 1fr);");
  expect(mobileLayout).toContain("scrollbar-width: none;");
  expect(styles).toContain(".sidebar::-webkit-scrollbar");
});

test("scopes shell CSS selectors away from future pages", () => {
  expect(styles).toContain(".sidebar nav {");
  expect(styles).toContain(".main-content h1,");
  expect(styles).toContain(".status-grid > article {");
  expect(styles).not.toMatch(/^nav\s*\{/m);
  expect(styles).not.toMatch(/^article\s*\{/m);
  expect(styles).not.toMatch(/^h1,\s*$/m);
});

test("uses the approved full-width Evidence Lab grid at desktop and stacks it on narrow viewports", () => {
  expect(styles).toContain("--page: #0b1220;");
  expect(styles).toContain("--green: #60d6a7;");
  expect(styles).toContain("--amber: #f3bd68;");
  expect(styles).toContain("--red: #ef7186;");
  expect(styles).toMatch(/\.content-frame\s*\{[^}]*max-width:\s*none;/s);
  expect(styles).toMatch(/\.symbol-grid\s*\{[^}]*grid-template-columns:\s*repeat\(2, minmax\(0, 1fr\)\);/s);
  expect(styles).toMatch(/\.data-health\s*\{[^}]*grid-template-columns:\s*repeat\(4, minmax\(0, 1fr\)\);/s);
  expect(styles).toMatch(/\.evidence-metrics[^}]*font-variant-numeric:\s*tabular-nums;/s);
  expect(styles).toContain(".app-shell input:focus-visible");

  const narrowStart = styles.indexOf("@media (max-width: 640px)");
  const reducedStart = styles.indexOf("@media (prefers-reduced-motion: reduce)");
  const narrowLayout = styles.slice(narrowStart, reducedStart);
  expect(narrowStart).toBeGreaterThan(-1);
  expect(narrowLayout).toMatch(/\.symbol-grid,\s*\.data-health\s*\{[^}]*grid-template-columns:\s*minmax\(0, 1fr\);/s);
  expect(narrowLayout).toMatch(/\.filter-control\s*\{[^}]*flex:\s*0 1 auto;/s);
  expect(styles.slice(reducedStart)).toContain("animation: none !important;");
});
