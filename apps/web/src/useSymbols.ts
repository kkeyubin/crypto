import { useCallback, useEffect, useState } from "react";
import {
  loadMarketDataHealth,
  loadSymbolEvidence,
  loadSymbolsDashboard,
  type SymbolsDashboard,
} from "./apiClient";
import type { SymbolView } from "./contracts";

type SymbolsLoadState =
  | { status: "loading" }
  | { status: "ready"; dashboard: SymbolsDashboard }
  | { status: "error" };

export type SymbolsState = SymbolsLoadState & {
  reload: () => void;
  retrySymbol: (symbol: string) => Promise<void>;
  retryHealth: () => Promise<void>;
  replaceSymbol: (symbol: SymbolView) => void;
};

export function useSymbols(): SymbolsState {
  const [requestId, setRequestId] = useState(0);
  const [state, setState] = useState<SymbolsLoadState>({ status: "loading" });
  const retry = useCallback(() => setRequestId((current) => current + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    setState({ status: "loading" });

    void loadSymbolsDashboard(controller.signal).then(
      (dashboard) => {
        if (active) {
          setState({ status: "ready", dashboard });
        }
      },
      (error: unknown) => {
        if (active && !(error instanceof DOMException && error.name === "AbortError")) {
          setState({ status: "error" });
        }
      },
    );

    return () => {
      active = false;
      controller.abort();
    };
  }, [requestId, retry]);

  const retrySymbol = useCallback(async (symbolName: string) => {
    if (state.status !== "ready") {
      return;
    }
    const current = state.dashboard.items.find((item) => item.symbol.symbol === symbolName);
    if (current === undefined) {
      return;
    }
    setState((value) => value.status === "ready" ? {
      status: "ready",
      dashboard: {
        ...value.dashboard,
        items: value.dashboard.items.map((item) => item.symbol.symbol === symbolName
          ? { status: "loading" as const, symbol: item.symbol }
          : item),
      },
    } : value);
    try {
      const evidence = await loadSymbolEvidence(current.symbol);
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: {
          ...value.dashboard,
          items: value.dashboard.items.map((item) => item.symbol.symbol === symbolName
            ? { status: "ready" as const, symbol: current.symbol, evidence }
            : item),
        },
      } : value);
    } catch {
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: {
          ...value.dashboard,
          items: value.dashboard.items.map((item) => item.symbol.symbol === symbolName
            ? { status: "error" as const, symbol: current.symbol }
            : item),
        },
      } : value);
    }
  }, [state]);

  const retryHealth = useCallback(async () => {
    setState((value) => value.status === "ready" ? {
      status: "ready",
      dashboard: { ...value.dashboard, health: { status: "loading" } },
    } : value);
    try {
      const health = await loadMarketDataHealth();
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: { ...value.dashboard, health: { status: "ready", health } },
      } : value);
    } catch {
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: { ...value.dashboard, health: { status: "error" } },
      } : value);
    }
  }, []);

  const replaceSymbol = useCallback((updated: SymbolView) => {
    setState((value) => value.status === "ready" ? {
      status: "ready",
      dashboard: {
        ...value.dashboard,
        items: value.dashboard.items.map((item) => {
          if (item.symbol.symbol !== updated.symbol) {
            return item;
          }
          return item.status === "ready"
            ? { status: "ready" as const, symbol: updated, evidence: { ...item.evidence, symbol: updated } }
            : { ...item, symbol: updated };
        }),
      },
    } : value);
  }, []);

  return { ...state, reload: retry, retrySymbol, retryHealth, replaceSymbol };
}
