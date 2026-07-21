import { useLayoutEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type { SymbolEvidence } from "../apiClient";

interface SymbolCardProps {
  evidence: SymbolEvidence;
  focusToken?: number;
  onDisable: (symbol: string) => Promise<SymbolMutationResult | null>;
  onEnable: (symbol: SymbolEvidence["symbol"]) => Promise<SymbolMutationResult | null>;
  onMutationCommitted: (mutation: SymbolMutationResult, kind: "disabled" | "enabled") => void;
  onFocusConsumed: (token: number) => void;
}

export interface SymbolMutationResult {
  readonly updated: SymbolEvidence["symbol"];
  readonly actionToken: number;
}

const REQUIRED_STREAM_SUFFIXES = ["aggtrade", "bookticker", "kline_1m", "markprice@1s"] as const;

function summarizeFreshness(evidence: SymbolEvidence) {
  const requiredNames = REQUIRED_STREAM_SUFFIXES.map((suffix) => `${evidence.symbol.symbol.toLowerCase()}@${suffix}`);
  const byName = new Map(evidence.streams
    .filter((stream) => requiredNames.includes(stream.stream_name as (typeof requiredNames)[number]))
    .map((stream) => [stream.stream_name, stream]));
  const required = requiredNames.flatMap((name) => {
    const stream = byName.get(name);
    return stream === undefined ? [] : [stream];
  });
  const complete = required.length === requiredNames.length && required.every((stream) => stream.last_event_at !== null);
  const connected = complete && required.every((stream) => stream.status === "connected");
  const oldestEventAt = complete
    ? required.reduce<string>((oldest, stream) => Date.parse(stream.last_event_at as string) < Date.parse(oldest)
      ? stream.last_event_at as string
      : oldest, required[0].last_event_at as string)
    : null;
  return {
    observed: required.length,
    complete,
    connected,
    oldestEventAt,
    stale: evidence.eligibility.reason_codes.includes("stale_live_data"),
  };
}

export function SymbolCard({ evidence, focusToken, onDisable, onEnable, onMutationCommitted, onFocusConsumed }: SymbolCardProps) {
  const { i18n, t } = useTranslation();
  const { symbol, profile, eligibility } = evidence;
  const [confirmingDisable, setConfirmingDisable] = useState(false);
  const [disabling, setDisabling] = useState(false);
  const [enabling, setEnabling] = useState(false);
  const confirmButton = useRef<HTMLButtonElement>(null);
  const cancelButton = useRef<HTMLButtonElement>(null);
  const actionButton = useRef<HTMLButtonElement>(null);
  const dialogWasOpen = useRef(false);
  const openGaps = evidence.gaps.filter((gap) => gap.status === "open");
  const approvedPartitions = evidence.partitions.filter((partition) => partition.status === "approved");
  const approvedRows = approvedPartitions.reduce((total, partition) => total + partition.row_count, 0);
  const freshness = summarizeFreshness(evidence);
  const numberFormat = new Intl.NumberFormat(i18n.language);
  const percentFormat = new Intl.NumberFormat(i18n.language, { style: "percent", maximumFractionDigits: 0 });
  const dateTimeFormat = new Intl.DateTimeFormat(i18n.language, {
    dateStyle: "medium",
    timeStyle: "medium",
    timeZone: "UTC",
  });
  const formatDate = (value: string) => {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? t("unknownValue") : `${dateTimeFormat.format(parsed)} UTC`;
  };

  useLayoutEffect(() => {
    if (confirmingDisable) {
      dialogWasOpen.current = true;
      confirmButton.current?.focus();
    } else if (dialogWasOpen.current) {
      dialogWasOpen.current = false;
      actionButton.current?.focus();
    }
  }, [confirmingDisable]);

  useLayoutEffect(() => {
    if (focusToken !== undefined) {
      actionButton.current?.focus();
      if (document.activeElement === actionButton.current) {
        onFocusConsumed(focusToken);
      }
    }
  }, [focusToken, onFocusConsumed, symbol.enabled]);

  const closeConfirmation = () => {
    if (!disabling) {
      setConfirmingDisable(false);
    }
  };

  const confirmDisable = async () => {
    if (disabling) {
      return;
    }
    setDisabling(true);
    const mutation = await onDisable(symbol.symbol);
    setDisabling(false);
    if (mutation !== null) {
      setConfirmingDisable(false);
      onMutationCommitted(mutation, "disabled");
    }
  };

  const reenable = async () => {
    setEnabling(true);
    const mutation = await onEnable(symbol);
    setEnabling(false);
    if (mutation !== null) {
      onMutationCommitted(mutation, "enabled");
    }
  };

  return (
    <article className="symbol-card" aria-label={t("symbolEvidenceLabel", { symbol: symbol.symbol })} data-symbol-surface={symbol.symbol}>
      <header className="symbol-card__header">
        <div>
          <p className="symbol-kicker">{t("perpetualContract")}</p>
          <h2>{symbol.symbol}</h2>
        </div>
        <span className={`evidence-badge evidence-badge--${eligibility.eligible ? "healthy" : "caution"}`}>
          <span aria-hidden="true">{eligibility.eligible ? "✓" : "!"}</span>
          {t(eligibility.eligible ? "eligible" : "ineligible")}
        </span>
      </header>

      <dl className="evidence-metrics">
        <div>
          <dt>{t("profileSamples")}</dt>
          <dd>{profile === null ? t("profileBuilding") : numberFormat.format(profile.sample_count)}</dd>
        </div>
        <div>
          <dt>{t("profileCoverage")}</dt>
          <dd>{profile === null ? t("unknownValue") : percentFormat.format(profile.coverage_fraction)}</dd>
        </div>
        <div>
          <dt>{t("approvedCatalogRows")}</dt>
          <dd>{evidence.partitionsTruncated ? t("atLeastCount", { count: numberFormat.format(approvedRows) }) : numberFormat.format(approvedRows)}</dd>
        </div>
        <div>
          <dt>{t("visibleOpenGaps")}</dt>
          <dd>{evidence.gapsTruncated
            ? t("atLeastGapCount", { count: numberFormat.format(openGaps.length) })
            : openGaps.length === 0 ? t("noOpenGaps") : t("openGapCount", { count: openGaps.length })}</dd>
        </div>
      </dl>

      {evidence.partitionsTruncated || evidence.gapsTruncated ? (
        <p className="bounded-page-note">{t("boundedEvidenceNotice")}</p>
      ) : null}

      <div className="evidence-section">
        <h3>{t("freshnessTitle")}</h3>
        <p className={`freshness-state freshness-state--${freshness.complete && freshness.connected && !freshness.stale ? "healthy" : "caution"}`}>
          {freshness.observed < REQUIRED_STREAM_SUFFIXES.length
            ? t("missingRequiredStreams", { missing: REQUIRED_STREAM_SUFFIXES.length - freshness.observed, observed: freshness.observed })
            : !freshness.complete
              ? t("requiredStreamsMissingEvents")
              : !freshness.connected
                ? t("requiredStreamsLimited")
                : freshness.stale
                  ? t("requiredStreamsStale")
                  : t("requiredStreamsHealthy")}
        </p>
        <p className="timestamp-value">
          {freshness.oldestEventAt === null
            ? t("freshnessUnknown")
            : `${t("oldestRequiredEvent")}: ${formatDate(freshness.oldestEventAt)}`}
        </p>
        <p className="muted compact-copy">
          {t("collectionState")}: {t(`dataStatus.${symbol.data_status}`)} · {t("metadataState")}: {t(`metadataStatus.${symbol.metadata_status}`)}
        </p>
      </div>

      <div className="evidence-section">
        <h3>{t("eligibilityReasonsTitle")}</h3>
        {eligibility.eligible ? (
          <p className="compact-copy">{t("noBlockingReasons")}</p>
        ) : (
          <ul className="reason-list">
            {eligibility.reason_codes.map((reason) => (
              <li key={reason}>{t(`eligibilityReason.${reason}`)}</li>
            ))}
          </ul>
        )}
      </div>

      {symbol.enabled ? (
        <div className="card-actions">
          <button ref={actionButton} className="danger-action" type="button" onClick={() => setConfirmingDisable(true)}>
            {t("disableCollection", { symbol: symbol.symbol })}
          </button>
        </div>
      ) : (
        <div className="card-actions card-actions--stacked">
          <p className="disabled-note">{t("collectionDisabled")}</p>
          <p className="muted compact-copy">{t("reenablePreservesIdentity")}</p>
          <button ref={actionButton} type="button" disabled={enabling} onClick={() => void reenable()}>
            {enabling ? t("reenabling") : t("reenableCollection", { symbol: symbol.symbol })}
          </button>
        </div>
      )}

      {confirmingDisable ? (
        <div
          className="confirmation-panel"
          role="alertdialog"
          aria-modal="true"
          aria-label={t("confirmDisableTitle", { symbol: symbol.symbol })}
          onKeyDown={(event) => {
            if (event.key === "Escape") {
              event.preventDefault();
              if (!disabling) {
                closeConfirmation();
              }
            } else if (event.key === "Tab") {
              if (event.shiftKey && document.activeElement === cancelButton.current) {
                event.preventDefault();
                confirmButton.current?.focus();
              } else if (!event.shiftKey && document.activeElement === confirmButton.current) {
                event.preventDefault();
                cancelButton.current?.focus();
              }
            }
          }}
        >
          <h3>{t("confirmDisableTitle", { symbol: symbol.symbol })}</h3>
          <p>{t("disablePreservesHistory")}</p>
          <div className="confirmation-actions">
            <button ref={cancelButton} type="button" aria-disabled={disabling} onClick={closeConfirmation}>{t("cancel")}</button>
            <button ref={confirmButton} className="danger-action" type="button" aria-disabled={disabling} onClick={() => void confirmDisable()}>
              {disabling ? t("disabling") : t("confirmDisable")}
            </button>
          </div>
        </div>
      ) : null}
    </article>
  );
}
