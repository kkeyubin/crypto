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

interface CommittedMutation {
  readonly token: number;
  readonly symbol: SymbolView;
  evidenceConfirmed: boolean;
  listAcknowledged: boolean;
}

const SYMBOL_REFRESH_TIMEOUT_MS = 10_000;

type EvidenceLoadMode = "initial" | "retry" | "recoverable";

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  onTimeout: () => void,
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => {
      onTimeout();
      reject(new Error("Symbol evidence refresh timed out"));
    }, timeoutMs);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
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

function acknowledgesMutation(listed: SymbolView, committed: SymbolView): boolean {
  return (
    listed.enabled === committed.enabled &&
    Date.parse(listed.updated_at) >= Date.parse(committed.updated_at)
  );
}

export function useSymbols(): SymbolsState {
  const [state, setState] = useState<SymbolsLoadState>({ status: "loading" });
  const stateRef = useRef(state);
  const mountedRef = useRef(false);
  const generationRef = useRef(0);
  const nextTokenRef = useRef(0);
  const nextMutationTokenRef = useRef(0);
  const operationsRef = useRef(new Map<string, Operation>());
  const committedMutationsRef = useRef(new Map<string, CommittedMutation>());
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
    mode: EvidenceLoadMode,
    committedToken?: number,
  ): Promise<boolean> => {
    if (!mountedRef.current || generationRef.current !== generation) {
      return false;
    }
    const key = `symbol:${symbol.symbol}`;
    const operation = beginOperation(key);
    if (mode !== "initial" && isCurrent(generation, key, operation.token)) {
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: replaceItem(
          value.dashboard,
          symbol.symbol,
          mode === "recoverable" ? { status: "refreshing", symbol } : { status: "loading", symbol },
        ),
      } : value);
    }
    try {
      const evidencePromise = loadSymbolEvidence(symbol, operation.controller.signal);
      const evidence = mode === "recoverable"
        ? await withTimeout(evidencePromise, SYMBOL_REFRESH_TIMEOUT_MS, () => operation.controller.abort())
        : await evidencePromise;
      if (!isCurrent(generation, key, operation.token)) {
        return false;
      }
      if (committedToken !== undefined) {
        const committed = committedMutationsRef.current.get(symbol.symbol);
        if (committed?.token === committedToken) {
          committed.evidenceConfirmed = true;
          if (committed.listAcknowledged) {
            committedMutationsRef.current.delete(symbol.symbol);
          }
        }
      }
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: replaceItem(value.dashboard, symbol.symbol, {
          status: "ready",
          symbol,
          evidence,
          focusAction: mode === "recoverable",
        }),
      } : value);
      return true;
    } catch {
      operation.controller.abort();
      if (!isCurrent(generation, key, operation.token)) {
        return false;
      }
      setState((value) => value.status === "ready" ? {
        status: "ready",
        dashboard: replaceItem(
          value.dashboard,
          symbol.symbol,
          mode === "recoverable" ? { status: "refresh_error", symbol } : { status: "error", symbol },
        ),
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
    const listGeneration = generationRef.current;
    if (stateRef.current.status !== "ready") {
      setState({ status: "loading" });
    }

    const key = "symbols";
    const operation = beginOperation(key);
    void listSymbols(operation.controller.signal).then(
      (symbols) => {
        if (!isCurrent(listGeneration, key, operation.token)) {
          return;
        }
        finishOperation(key, operation.token);
        const generation = generationRef.current + 1;
        generationRef.current = generation;
        abortAll();

        for (const listed of symbols) {
          const committed = committedMutationsRef.current.get(listed.symbol);
          if (committed !== undefined && acknowledgesMutation(listed, committed.symbol)) {
            committed.listAcknowledged = true;
            if (committed.evidenceConfirmed) {
              committedMutationsRef.current.delete(listed.symbol);
            }
          }
        }
        const listedBySymbol = new Map(symbols.map((listed) => [listed.symbol, listed]));
        const mergedSymbols = symbols.map((listed) => (
          committedMutationsRef.current.get(listed.symbol)?.symbol ?? listed
        ));
        for (const committed of committedMutationsRef.current.values()) {
          if (!listedBySymbol.has(committed.symbol.symbol)) {
            mergedSymbols.push(committed.symbol);
          }
        }

        setState((value) => {
          const currentDashboard = value.status === "ready" ? value.dashboard : undefined;
          return {
            status: "ready",
            dashboard: {
              health: currentDashboard?.health ?? { status: "loading" },
              items: mergedSymbols.map((mergedSymbol) => {
                const committed = committedMutationsRef.current.get(mergedSymbol.symbol);
                if (committed !== undefined) {
                  return { status: "refreshing" as const, symbol: committed.symbol };
                }
                return currentDashboard?.items.find((item) => item.symbol.symbol === mergedSymbol.symbol)
                  ?? { status: "loading" as const, symbol: mergedSymbol };
              }),
              symbolsTruncated: symbols.length === 100,
            },
          };
        });
        void loadHealth(generation, false);
        for (const mergedSymbol of mergedSymbols) {
          const committed = committedMutationsRef.current.get(mergedSymbol.symbol);
          void loadEvidence(
            committed?.symbol ?? mergedSymbol,
            generation,
            committed === undefined ? "initial" : "recoverable",
            committed?.token,
          );
        }
      },
      () => {
        if (!isCurrent(listGeneration, key, operation.token)) {
          return;
        }
        finishOperation(key, operation.token);
        if (stateRef.current.status !== "ready") {
          setState({ status: "error" });
        }
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
    await loadEvidence(
      current.symbol,
      generationRef.current,
      current.status === "refresh_error" ? "recoverable" : "retry",
      committedMutationsRef.current.get(symbolName)?.token,
    );
  }, [loadEvidence]);

  const retryHealth = useCallback(async () => {
    if (stateRef.current.status !== "ready") {
      return;
    }
    await loadHealth(generationRef.current, true);
  }, [loadHealth]);

  const refreshSymbol = useCallback(async (updated: SymbolView) => {
    if (!mountedRef.current) {
      return false;
    }
    const committed = {
      token: nextMutationTokenRef.current + 1,
      symbol: updated,
      evidenceConfirmed: false,
      listAcknowledged: false,
    };
    nextMutationTokenRef.current = committed.token;
    committedMutationsRef.current.set(updated.symbol, committed);
    return loadEvidence(updated, generationRef.current, "recoverable", committed.token);
  }, [loadEvidence]);

  return { ...state, reload, retrySymbol, retryHealth, refreshSymbol };
}
