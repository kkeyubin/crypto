import { useCallback, useEffect, useRef, useState } from "react";
import {
  listSymbols,
  loadMarketDataHealth,
  loadSymbolEvidence,
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
  refreshSymbol: (symbol: SymbolView) => Promise<boolean>;
};

interface Operation {
  readonly token: number;
  readonly controller: AbortController;
}

function replaceItem(
  dashboard: SymbolsDashboard,
  symbolName: string,
  replacement: SymbolsDashboard["items"][number],
): SymbolsDashboard {
  return {
    ...dashboard,
    items: dashboard.items.map((item) => item.symbol.symbol === symbolName ? replacement : item),
  };
}

export function useSymbols(): SymbolsState {
  const [state, setState] = useState<SymbolsLoadState>({ status: "loading" });
  const stateRef = useRef(state);
  const mountedRef = useRef(false);
  const generationRef = useRef(0);
  const nextTokenRef = useRef(0);
  const operationsRef = useRef(new Map<string, Operation>());
  stateRef.current = state;

  const abortAll = useCallback(() => {
    for (const operation of operationsRef.current.values()) {
      operation.controller.abort();
    }
    operationsRef.current.clear();
  }, []);

  const beginOperation = useCallback((key: string): Operation => {
    operationsRef.current.get(key)?.controller.abort();
    const operation = {
      token: nextTokenRef.current + 1,
      controller: new AbortController(),
    };
    nextTokenRef.current = operation.token;
    operationsRef.current.set(key, operation);
    return operation;
  }, []);

  const isCurrent = useCallback((generation: number, key: string, token: number) => (
    mountedRef.current &&
    generationRef.current === generation &&
    operationsRef.current.get(key)?.token === token
  ), []);

  const finishOperation = useCallback((key: string, token: number) => {
    if (operationsRef.current.get(key)?.token === token) {
      operationsRef.current.delete(key);
    }
  }, []);

  const loadEvidence = useCallback(async (
    symbol: SymbolView,
    generation: number,
    publishLoading: boolean,
  ): Promise<boolean> => {
    if (!mountedRef.current || generationRef.current !== generation) {
      return false;
    }
    const key = `symbol:${symbol.symbol}`;
    const operation = beginOperation(key);
    if (publishLoading && isCurrent(generation, key, operation.token)) {
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: replaceItem(value.dashboard, symbol.symbol, { status: "loading", symbol }),
      } : value);
    }
    try {
      const evidence = await loadSymbolEvidence(symbol, operation.controller.signal);
      if (!isCurrent(generation, key, operation.token)) {
        return false;
      }
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: replaceItem(value.dashboard, symbol.symbol, { status: "ready", symbol, evidence }),
      } : value);
      return true;
    } catch {
      if (!isCurrent(generation, key, operation.token)) {
        return false;
      }
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: replaceItem(value.dashboard, symbol.symbol, { status: "error", symbol }),
      } : value);
      return false;
    } finally {
      finishOperation(key, operation.token);
    }
  }, [beginOperation, finishOperation, isCurrent]);

  const loadHealth = useCallback(async (generation: number, publishLoading: boolean): Promise<void> => {
    if (!mountedRef.current || generationRef.current !== generation) {
      return;
    }
    const key = "health";
    const operation = beginOperation(key);
    if (publishLoading && isCurrent(generation, key, operation.token)) {
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: { ...value.dashboard, health: { status: "loading" } },
      } : value);
    }
    try {
      const health = await loadMarketDataHealth(operation.controller.signal);
      if (!isCurrent(generation, key, operation.token)) {
        return;
      }
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: { ...value.dashboard, health: { status: "ready", health } },
      } : value);
    } catch {
      if (!isCurrent(generation, key, operation.token)) {
        return;
      }
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: { ...value.dashboard, health: { status: "error" } },
      } : value);
    } finally {
      finishOperation(key, operation.token);
    }
  }, [beginOperation, finishOperation, isCurrent]);

  const reload = useCallback(() => {
    if (!mountedRef.current) {
      return;
    }
    const generation = generationRef.current + 1;
    generationRef.current = generation;
    abortAll();
    setState({ status: "loading" });

    const key = "symbols";
    const operation = beginOperation(key);
    void listSymbols(operation.controller.signal).then(
      (symbols) => {
        if (!isCurrent(generation, key, operation.token)) {
          return;
        }
        finishOperation(key, operation.token);
        setState({
          status: "ready",
          dashboard: {
            health: { status: "loading" },
            items: symbols.map((symbol) => ({ status: "loading" as const, symbol })),
            symbolsTruncated: symbols.length === 100,
          },
        });
        void loadHealth(generation, false);
        for (const symbol of symbols) {
          void loadEvidence(symbol, generation, false);
        }
      },
      () => {
        if (!isCurrent(generation, key, operation.token)) {
          return;
        }
        finishOperation(key, operation.token);
        setState({ status: "error" });
      },
    );
  }, [abortAll, beginOperation, finishOperation, isCurrent, loadEvidence, loadHealth]);

  useEffect(() => {
    mountedRef.current = true;
    reload();
    return () => {
      mountedRef.current = false;
      generationRef.current += 1;
      abortAll();
    };
  }, [abortAll, reload]);

  const retrySymbol = useCallback(async (symbolName: string) => {
    const currentState = stateRef.current;
    if (currentState.status !== "ready") {
      return;
    }
    const current = currentState.dashboard.items.find((item) => item.symbol.symbol === symbolName);
    if (current === undefined) {
      return;
    }
    await loadEvidence(current.symbol, generationRef.current, true);
  }, [loadEvidence]);

  const retryHealth = useCallback(async () => {
    if (stateRef.current.status !== "ready") {
      return;
    }
    await loadHealth(generationRef.current, true);
  }, [loadHealth]);

  const refreshSymbol = useCallback(async (updated: SymbolView) => (
    loadEvidence(updated, generationRef.current, false)
  ), [loadEvidence]);

  return { ...state, reload, retrySymbol, retryHealth, refreshSymbol };
}
