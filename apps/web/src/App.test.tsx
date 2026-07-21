import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, test, vi } from "vitest";
import { App } from "./App";
import i18n from "./i18n";

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
