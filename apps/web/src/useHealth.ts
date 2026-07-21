import { useEffect, useState } from "react";

export type HealthState = "loading" | "healthy" | "unavailable";

const semanticVersion = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;

function isLiveHealthPayload(value: unknown): value is { status: "ok"; service: "api"; version: string } {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const payload = value as Record<string, unknown>;
  return (
    payload.status === "ok" &&
    payload.service === "api" &&
    typeof payload.version === "string" &&
    semanticVersion.test(payload.version)
  );
}

export function useHealth(): HealthState {
  const [state, setState] = useState<HealthState>("loading");

  useEffect(() => {
    const controller = new AbortController();
    let active = true;

    fetch("/api/health/live", { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) {
          throw new Error("Health request failed");
        }

        const payload: unknown = await response.json();
        if (!isLiveHealthPayload(payload)) {
          throw new Error("Invalid health response");
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
