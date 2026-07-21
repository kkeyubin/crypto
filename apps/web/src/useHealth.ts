import { useEffect, useState } from "react";

export type HealthState = "loading" | "healthy" | "unavailable";

export function useHealth(): HealthState {
  const [state, setState] = useState<HealthState>("loading");

  useEffect(() => {
    const controller = new AbortController();
    let active = true;

    fetch("/api/health/live", { signal: controller.signal })
      .then((response) => {
        if (!response.ok) {
          throw new Error("Health request failed");
        }

        if (active) {
          setState("healthy");
        }
      })
      .catch((error: unknown) => {
        if (active && !(error instanceof DOMException && error.name === "AbortError")) {
          setState("unavailable");
        }
      });

    return () => {
      active = false;
      controller.abort();
    };
  }, []);

  return state;
}
