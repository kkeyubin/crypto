import { useState, type FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { addSymbolWithBackfill, getBackfill } from "../apiClient";
import type { IngestionJobView } from "../contracts";

interface AddSymbolFormProps {
  onAdded: () => void;
}

function toUtcIso(value: string): string {
  const normalized = value.length === 16 ? `${value}:00Z` : `${value}Z`;
  return new Date(normalized).toISOString();
}

export function AddSymbolForm({ onAdded }: AddSymbolFormProps) {
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

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    let start: string;
    let end: string;
    try {
      start = toUtcIso(historyStart);
      end = toUtcIso(historyEnd);
    } catch {
      setError(t("invalidUtcRange"));
      return;
    }
    if (!/^[A-Z0-9]{3,32}$/.test(symbol) || Date.parse(end) <= Date.parse(start)) {
      setError(t("invalidUtcRange"));
      return;
    }
    if (symbolAggTrades && !backfillAggTrades) {
      setError(t("aggTradesSecondOptInRequired"));
      return;
    }
    setBusy(true);
    try {
      const createdJobs = await addSymbolWithBackfill({
        symbol,
        historyStart: start,
        historyEnd: end,
        includeAggTrades: symbolAggTrades && backfillAggTrades,
      });
      setJobs(createdJobs);
      onAdded();
    } catch {
      setError(t("addSymbolError"));
    } finally {
      setBusy(false);
    }
  };

  const refreshJobs = async () => {
    setError(null);
    setRefreshingJobs(true);
    try {
      setJobs(await Promise.all(jobs.map((job) => getBackfill(job.job_id))));
    } catch {
      setError(t("backfillRefreshError"));
    } finally {
      setRefreshingJobs(false);
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
          <input name="history-start" type="datetime-local" required value={historyStart} onChange={(event) => setHistoryStart(event.target.value)} />
        </label>
        <label>
          <span>{t("historyEndUtc")}</span>
          <input name="history-end" type="datetime-local" required value={historyEnd} onChange={(event) => setHistoryEnd(event.target.value)} />
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
      <button className="primary-action" type="submit" disabled={busy}>
        {busy ? t("addingSymbol") : t("addAndBackfill")}
      </button>

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
