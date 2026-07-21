import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, test, vi } from "vitest";
import { App } from "./App";
import i18n, { resolveInitialLocale } from "./i18n";
import styles from "./styles.css?raw";

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

test("translates the symbols title, filter, and action after a persisted English switch", async () => {
  const user = userEvent.setup();
  render(<App />);

  await user.click(screen.getByRole("link", { name: "币种" }));
  await user.click(screen.getByRole("button", { name: "切换为英文" }));

  expect(screen.getByRole("heading", { name: "Symbols & Data" })).toBeInTheDocument();
  expect(screen.getByRole("searchbox", { name: "Filter symbols" })).toBeInTheDocument();
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
