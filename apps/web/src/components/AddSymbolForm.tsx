import { useEffect, useRef, useState, type FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { addSymbol, createBackfills, getBackfill, type AddSymbolInput } from "../apiClient";
import type { IngestionJobView } from "../contracts";

interface AddSymbolFormProps {
  onAdded: () => void;
  onBackfillsCreated: () => void;
  onProtectionChange: (protectedState: boolean) => void;
}

const UTC_DAY_MS = 24 * 60 * 60 * 1000;
const MAX_HISTORY_DAYS = 366;

type ArchiveRangeResult =
  | { readonly ok: true; readonly start: string; readonly end: string }
  | { readonly ok: false; readonly reason: "invalid" | "too_large" };

function utcDayMillis(value: string): number | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (match === null) {
    return null;
  }
  const millis = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  return new Date(millis).toISOString().slice(0, 10) === value ? millis : null;
}

export function toArchiveUtcRange(
  startDay: string,
  inclusiveEndDay: string,
  now = new Date(),
): ArchiveRangeResult {
  const startMillis = utcDayMillis(startDay);
  const inclusiveEndMillis = utcDayMillis(inclusiveEndDay);
  if (startMillis === null || inclusiveEndMillis === null || inclusiveEndMillis < startMillis) {
    return { ok: false, reason: "invalid" };
  }
  const endMillis = inclusiveEndMillis + UTC_DAY_MS;
  if (endMillis - startMillis > MAX_HISTORY_DAYS * UTC_DAY_MS) {
    return { ok: false, reason: "too_large" };
  }
  const currentUtcMidnight = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  if (endMillis > currentUtcMidnight) {
    return { ok: false, reason: "invalid" };
  }
  return {
    ok: true,
    start: new Date(startMillis).toISOString(),
    end: new Date(endMillis).toISOString(),
  };
}

export function AddSymbolForm({ onAdded, onBackfillsCreated, onProtectionChange }: AddSymbolFormProps) {
  const { t } = useTranslation();
  const [symbol, setSymbol] = useState("");
  const [historyStart, setHistoryStart] = useState("");
  const [historyEnd, setHistoryEnd] = useState("");
  const [symbolAggTrades, setSymbolAggTrades] = useState(false);
  const [backfillAggTrades, setBackfillAggTrades] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jobs, setJobs] = useState<readonly IngestionJobView[]>([]);
  const [refreshingJobs, setRefreshingJobs] = useState(false);
  const [configuredInput, setConfiguredInput] = useState<AddSymbolInput | null>(null);
  const [backfillPending, setBackfillPending] = useState(false);
  const mounted = useRef(true);
  const activeControllers = useRef(new Set<AbortController>());

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      for (const controller of activeControllers.current) {
        controller.abort();
      }
      activeControllers.current.clear();
    };
  }, []);

  const beginRequest = () => {
    const controller = new AbortController();
    activeControllers.current.add(controller);
    return controller;
  };

  const finishRequest = (controller: AbortController) => {
    activeControllers.current.delete(controller);
  };

  const startBackfills = async (input: AddSymbolInput, signal: AbortSignal): Promise<boolean> => {
    try {
      const createdJobs = await createBackfills(input, signal);
      if (!mounted.current) {
        return false;
      }
      setJobs(createdJobs);
      setBackfillPending(false);
      onBackfillsCreated();
      return true;
    } catch {
      if (mounted.current) {
        setBackfillPending(true);
        onProtectionChange(true);
      }
      return false;
    }
  };

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    const range = toArchiveUtcRange(historyStart, historyEnd);
    if (!/^[A-Z0-9]{3,32}$/.test(symbol) || !range.ok) {
      setError(t(!range.ok && range.reason === "too_large" ? "utcRangeTooLarge" : "invalidUtcRange"));
      return;
    }
    if (symbolAggTrades && !backfillAggTrades) {
      setError(t("aggTradesSecondOptInRequired"));
      return;
    }
    onProtectionChange(true);
    setBusy(true);
    const controller = beginRequest();
    const input = {
      symbol,
      historyStart: range.start,
      historyEnd: range.end,
      includeAggTrades: symbolAggTrades && backfillAggTrades,
    };
    let remainProtected = true;
    try {
      await addSymbol(input, controller.signal);
      if (!mounted.current) {
        return;
      }
      setConfiguredInput(input);
      onAdded();
      remainProtected = !(await startBackfills(input, controller.signal));
    } catch {
      if (mounted.current) {
        setError(t("addSymbolError"));
        remainProtected = false;
      }
    } finally {
      finishRequest(controller);
      if (mounted.current) {
        setBusy(false);
        onProtectionChange(remainProtected);
      }
    }
  };

  const retryBackfills = async () => {
    if (configuredInput === null) {
      return;
    }
    setError(null);
    setBusy(true);
    onProtectionChange(true);
    const controller = beginRequest();
    let backfillsCreated = false;
    try {
      backfillsCreated = await startBackfills(configuredInput, controller.signal);
    } finally {
      finishRequest(controller);
      if (mounted.current) {
        setBusy(false);
        if (backfillsCreated) {
          onProtectionChange(false);
        }
      }
    }
  };

  const refreshJobs = async () => {
    setError(null);
    setRefreshingJobs(true);
    const controller = beginRequest();
    try {
      const refreshed = await Promise.all(jobs.map((job) => getBackfill(job.job_id, controller.signal)));
      if (mounted.current) {
        setJobs(refreshed);
      }
    } catch {
      if (mounted.current) {
        setError(t("backfillRefreshError"));
      }
    } finally {
      finishRequest(controller);
      if (mounted.current) {
        setRefreshingJobs(false);
      }
    }
  };

  return (
    <form className="add-symbol-form" aria-label={t("addSymbolFormLabel")} onSubmit={(event) => void submit(event)}>
      <div className="form-heading">
        <div>
          <h2>{t("addSymbolFormLabel")}</h2>
          <p className="muted compact-copy">{t("closedUtcRange")}</p>
        </div>
        <p className="scope-note">{t("dataOnlyScope")}</p>
      </div>

      <div className="form-grid">
        <label>
          <span>{t("symbolCode")}</span>
          <input
            name="symbol"
            autoComplete="off"
            pattern="[A-Z0-9]{3,32}"
            required
            value={symbol}
            onChange={(event) => setSymbol(event.target.value.toUpperCase())}
          />
        </label>
        <label>
          <span>{t("historyStartUtc")}</span>
          <input name="history-start" type="date" required value={historyStart} onChange={(event) => setHistoryStart(event.target.value)} />
        </label>
        <label>
          <span>{t("historyEndUtc")}</span>
          <input name="history-end" type="date" required value={historyEnd} onChange={(event) => setHistoryEnd(event.target.value)} />
        </label>
      </div>

      <div className="agg-warning">
        <strong>{t("aggTradesWarningTitle")}</strong>
        <p>{t("aggTradesWarning")}</p>
        <label className="check-row">
          <input
            type="checkbox"
            checked={symbolAggTrades}
            onChange={(event) => {
              setSymbolAggTrades(event.target.checked);
              if (!event.target.checked) {
                setBackfillAggTrades(false);
              }
            }}
          />
          <span>{t("symbolAggTradesOptIn")}</span>
        </label>
        <label className="check-row">
          <input
            type="checkbox"
            checked={backfillAggTrades}
            disabled={!symbolAggTrades}
            onChange={(event) => setBackfillAggTrades(event.target.checked)}
          />
          <span>{t("backfillAggTradesOptIn")}</span>
        </label>
      </div>

      {error === null ? null : <p className="inline-error" role="alert">{error}</p>}
      {configuredInput === null ? (
        <button className="primary-action" type="submit" disabled={busy}>
          {busy ? t("addingSymbol") : t("addAndBackfill")}
        </button>
      ) : null}

      {backfillPending && configuredInput !== null ? (
        <section className="partial-success" role="status" aria-label={t("partialBackfillLabel")}>
          <p>{t("partialBackfillNotice", { symbol: configuredInput.symbol })}</p>
          <button type="button" disabled={busy} onClick={() => void retryBackfills()}>
            {busy ? t("retryingBackfill") : t("retryBackfillOnly")}
          </button>
        </section>
      ) : null}

      {jobs.length === 0 ? null : (
        <section className="backfill-progress" role="status" aria-label={t("backfillCreatedLabel")}>
          <h3>{t("backfillCreated", { count: jobs.length })}</h3>
          <ul>
            {jobs.map((job) => (
              <li key={job.job_id}>
                <span>{t(`dataType.${job.data_type}`)}</span>
                <strong>{t(`jobStatus.${job.status}`)}</strong>
              </li>
            ))}
          </ul>
          <button type="button" disabled={refreshingJobs} onClick={() => void refreshJobs()}>
            {refreshingJobs ? t("refreshingBackfills") : t("refreshBackfills")}
          </button>
        </section>
      )}
    </form>
  );
}
