import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type { SymbolEvidence } from "../apiClient";

interface SymbolCardProps {
  evidence: SymbolEvidence;
  onDisable: (symbol: string) => Promise<void>;
}

function latestEvent(evidence: SymbolEvidence): string | null {
  return evidence.streams.reduce<string | null>((latest, stream) => {
    if (stream.last_event_at === null) {
      return latest;
    }
    if (latest === null || Date.parse(stream.last_event_at) > Date.parse(latest)) {
      return stream.last_event_at;
    }
    return latest;
  }, null);
}

export function SymbolCard({ evidence, onDisable }: SymbolCardProps) {
  const { i18n, t } = useTranslation();
  const { symbol, profile, eligibility } = evidence;
  const [confirmingDisable, setConfirmingDisable] = useState(false);
  const [disabling, setDisabling] = useState(false);
  const confirmButton = useRef<HTMLButtonElement>(null);
  const openGaps = evidence.gaps.filter((gap) => gap.status === "open");
  const approvedPartitions = evidence.partitions.filter((partition) => partition.status === "approved");
  const approvedRows = approvedPartitions.reduce((total, partition) => total + partition.row_count, 0);
  const lastEventAt = latestEvent(evidence);
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

  useEffect(() => {
    if (confirmingDisable) {
      confirmButton.current?.focus();
    }
  }, [confirmingDisable]);

  const confirmDisable = async () => {
    setDisabling(true);
    try {
      await onDisable(symbol.symbol);
      setConfirmingDisable(false);
    } finally {
      setDisabling(false);
    }
  };

  return (
    <article className="symbol-card" aria-label={t("symbolEvidenceLabel", { symbol: symbol.symbol })}>
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
          <dt>{t("approvedArchiveRows")}</dt>
          <dd>{numberFormat.format(approvedRows)}</dd>
        </div>
        <div>
          <dt>{t("openGaps")}</dt>
          <dd>{openGaps.length === 0 ? t("noOpenGaps") : t("openGapCount", { count: openGaps.length })}</dd>
        </div>
      </dl>

      <div className="evidence-section">
        <h3>{t("freshnessTitle")}</h3>
        <p className="timestamp-value">
          {t("lastEvent")}: {lastEventAt === null ? t("noLiveEvent") : formatDate(lastEventAt)}
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
          <button className="danger-action" type="button" onClick={() => setConfirmingDisable(true)}>
            {t("disableCollection", { symbol: symbol.symbol })}
          </button>
        </div>
      ) : <p className="disabled-note">{t("collectionDisabled")}</p>}

      {confirmingDisable ? (
        <div
          className="confirmation-panel"
          role="alertdialog"
          aria-modal="true"
          aria-label={t("confirmDisableTitle", { symbol: symbol.symbol })}
          onKeyDown={(event) => {
            if (event.key === "Escape") {
              setConfirmingDisable(false);
            }
          }}
        >
          <h3>{t("confirmDisableTitle", { symbol: symbol.symbol })}</h3>
          <p>{t("disablePreservesHistory")}</p>
          <div className="confirmation-actions">
            <button type="button" onClick={() => setConfirmingDisable(false)}>{t("cancel")}</button>
            <button ref={confirmButton} className="danger-action" type="button" disabled={disabling} onClick={() => void confirmDisable()}>
              {disabling ? t("disabling") : t("confirmDisable")}
            </button>
          </div>
        </div>
      ) : null}
    </article>
  );
}
