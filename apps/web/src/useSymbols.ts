import { useCallback, useEffect, useState } from "react";
import { loadSymbolsDashboard, type SymbolsDashboard } from "./apiClient";

type SymbolsLoadState =
  | { status: "loading" }
  | { status: "ready"; dashboard: SymbolsDashboard }
  | { status: "error" };

export type SymbolsState = SymbolsLoadState & { reload: () => void };

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

  return { ...state, reload: retry };
}
