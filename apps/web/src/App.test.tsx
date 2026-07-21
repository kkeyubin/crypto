import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, test, vi } from "vitest";
import { App } from "./App";
import i18n from "./i18n";

const styles = readFileSync(resolve(process.cwd(), "src/styles.css"), "utf8");

beforeEach(async () => {
  localStorage.clear();
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

  await user.click(screen.getByRole("button", { name: "EN" }));

  expect(screen.getByRole("heading", { name: "Command Center" })).toBeInTheDocument();
  expect(localStorage.getItem("crypto-locale")).toBe("en");
  expect(document.documentElement.lang).toBe("en");
});

test("keeps navigation accessible and marks the active shell view", () => {
  render(<App />);

  expect(screen.getByRole("navigation", { name: "主导航" })).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "总览", current: "page" })).toBeInTheDocument();
});

test("stacks header, navigation, and content into explicit mobile grid rows", () => {
  const mobileStart = styles.indexOf("@media (max-width: 820px)");
  const mobileEnd = styles.indexOf("@media (max-width: 480px)", mobileStart);
  const mobileLayout = styles.slice(mobileStart, mobileEnd);

  expect(mobileLayout).toContain("grid-template-rows: 64px auto minmax(0, 1fr);");
  expect(mobileLayout).toContain("scrollbar-width: none;");
  expect(styles).toContain(".sidebar::-webkit-scrollbar");
});
