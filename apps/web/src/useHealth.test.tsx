import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";
import { useHealth } from "./useHealth";

afterEach(() => {
  vi.unstubAllGlobals();
});

test("reports healthy only for the expected liveness payload", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ status: "ok", service: "api", version: "0.1.0" }),
    }),
  );

  const { result } = renderHook(() => useHealth());

  await waitFor(() => expect(result.current).toBe("healthy"));
});

test("reports unavailable for non-success, rejected, malformed, and invalid JSON responses", async () => {
  const responses = [
    { ok: false, json: async () => ({ status: "ok", service: "api", version: "0.1.0" }) },
    new Error("network unavailable"),
    { ok: true, json: async () => ({ status: "ok", service: "worker", version: "0.1.0" }) },
    { ok: true, json: async () => ({ status: "ok", service: "api", version: "not-a-version" }) },
    { ok: true, json: async () => Promise.reject(new SyntaxError("invalid JSON")) },
  ];

  for (const response of responses) {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation(() =>
        response instanceof Error ? Promise.reject(response) : Promise.resolve(response),
      ),
    );

    const { result, unmount } = renderHook(() => useHealth());
    await waitFor(() => expect(result.current).toBe("unavailable"));
    unmount();
  }
});

test("aborts the health request when unmounted", () => {
  let requestSignal: AbortSignal | undefined;
  vi.stubGlobal(
    "fetch",
    vi.fn().mockImplementation((_input: RequestInfo | URL, init?: RequestInit) => {
      requestSignal = init?.signal ?? undefined;
      return new Promise<Response>(() => undefined);
    }),
  );

  const { unmount } = renderHook(() => useHealth());
  unmount();

  expect(requestSignal?.aborted).toBe(true);
});
