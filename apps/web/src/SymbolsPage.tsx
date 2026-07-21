import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { AddSymbolForm } from "./components/AddSymbolForm";
import { disableSymbol, enableSymbol, type SymbolEvidenceState } from "./apiClient";
import type { SymbolView } from "./contracts";
import { DataStatus } from "./components/DataStatus";
import { SymbolCard } from "./components/SymbolCard";
import { useSymbols } from "./useSymbols";

function RecoverableSymbolState({ item, onRetry }: {
  item: Extract<SymbolEvidenceState, { status: "refreshing" | "refresh_error" }>;
  onRetry: (symbol: string) => Promise<void>;
}) {
  const { t } = useTranslation();
  const owner = useRef<HTMLElement>(null);
  const retry = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (item.status === "refreshing") {
      owner.current?.focus();
    } else {
      retry.current?.focus();
    }
  }, [item.status]);

  return (
    <article
      ref={owner}
      className="symbol-card symbol-card--state"
      aria-label={t(item.status === "refreshing" ? "symbolEvidenceRefreshingLabel" : "symbolEvidenceRefreshErrorLabel", { symbol: item.symbol.symbol })}
      tabIndex={-1}
    >
      <p role={item.status === "refreshing" ? "status" : "alert"}>
        {t(item.status === "refreshing" ? "symbolEvidenceRefreshing" : "symbolEvidenceRefreshError", { symbol: item.symbol.symbol })}
      </p>
      {item.status === "refresh_error" ? (
        <button ref={retry} type="button" onClick={() => void onRetry(item.symbol.symbol)}>
          {t("retrySymbolRefresh", { symbol: item.symbol.symbol })}
        </button>
      ) : null}
    </article>
  );
}

export function SymbolsPage() {
  const { t } = useTranslation();
  const symbols = useSymbols();
  const [showAddForm, setShowAddForm] = useState(false);
  const [addFormProtected, setAddFormProtected] = useState(false);
  const [filter, setFilter] = useState("");
  const [actionNotice, setActionNotice] = useState<{
    kind: "disabled" | "enabled" | "disableError" | "enableError";
    symbol?: string;
  } | null>(null);

  const handleDisable = async (symbol: string): Promise<SymbolView | null> => {
    setActionNotice(null);
    try {
      return await disableSymbol(symbol);
    } catch {
      setActionNotice({ kind: "disableError" });
      return null;
    }
  };

  const handleEnable = async (symbol: SymbolView): Promise<SymbolView | null> => {
    setActionNotice(null);
    try {
      return await enableSymbol(symbol);
    } catch {
      setActionNotice({ kind: "enableError" });
      return null;
    }
  };

  const handleMutationCommitted = (updated: SymbolView, kind: "disabled" | "enabled") => {
    void symbols.refreshSymbol(updated).then((refreshed) => {
      if (refreshed) {
        setActionNotice({ kind, symbol: updated.symbol });
      }
    });
  };
  const normalizedFilter = filter.trim().toUpperCase();
  const visibleItems = symbols.status === "ready"
    ? symbols.dashboard.items.filter((item) => item.symbol.symbol.includes(normalizedFilter))
    : [];

  return (
    <div className="content-frame symbols-page">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">{t("evidenceLab")}</p>
          <h1>{t("symbolsTitle")}</h1>
        </div>
        <div className="page-actions">
          <label className="filter-control">
            <span>{t("filterSymbols")}</span>
            <input type="search" value={filter} onChange={(event) => setFilter(event.target.value)} />
          </label>
          <button
            className="primary-action"
            type="button"
            aria-expanded={showAddForm}
            disabled={showAddForm && addFormProtected}
            onClick={() => setShowAddForm((visible) => !visible)}
          >
            {showAddForm ? t("closeAddSymbol") : t("addSymbol")}
          </button>
        </div>
      </div>
      {showAddForm && addFormProtected ? (
        <p className="form-protection-note" role="status" aria-label={t("addFormProtectedLabel")}>
          {t("addFormProtectedNotice")}
        </p>
      ) : null}
      {showAddForm ? (
        <AddSymbolForm
          onAdded={symbols.reload}
          onBackfillsCreated={symbols.reload}
          onProtectionChange={setAddFormProtected}
        />
      ) : null}
      {actionNotice === null ? null : actionNotice.kind === "disabled" || actionNotice.kind === "enabled" ? (
        <p className="action-notice" role="status" aria-label={t(actionNotice.kind === "disabled" ? "symbolDisabledLabel" : "symbolEnabledLabel")}>
          {t(actionNotice.kind === "disabled" ? "symbolDisabledNotice" : "symbolEnabledNotice", { symbol: actionNotice.symbol })}
        </p>
      ) : <p className="inline-error" role="alert">{t(actionNotice.kind === "disableError" ? "disableSymbolError" : "enableSymbolError")}</p>}
      {symbols.status === "loading" ? (
        <p className="page-state" role="status" aria-label={t("symbolsLoading")}>
          {t("symbolsLoading")}
        </p>
      ) : symbols.status === "ready" && symbols.dashboard.items.length === 0 ? (
        <section className="page-state empty-state" aria-labelledby="empty-symbols-title">
          <h2 id="empty-symbols-title">{t("symbolsEmptyTitle")}</h2>
          <p>{t("symbolsEmptyDescription")}</p>
        </section>
      ) : symbols.status === "error" ? (
        <div className="page-state error-state" role="alert">
          <p>{t("symbolsLoadError")}</p>
          <button type="button" onClick={symbols.reload}>{t("retrySymbols")}</button>
        </div>
      ) : symbols.status === "ready" ? (
        <>
          {symbols.dashboard.health.status === "ready" ? (
            <DataStatus health={symbols.dashboard.health.health} />
          ) : symbols.dashboard.health.status === "loading" ? (
            <p className="health-load-state" role="status">{t("healthLoading")}</p>
          ) : (
            <div className="health-error" role="alert" aria-label={t("healthLoadErrorLabel")}>
              <p>{t("healthLoadError")}</p>
              <button type="button" onClick={() => void symbols.retryHealth()}>{t("retryHealth")}</button>
            </div>
          )}
          {symbols.dashboard.symbolsTruncated ? (
            <p className="bounded-page-note" role="status">{t("boundedSymbolsNotice")}</p>
          ) : null}
          {normalizedFilter.length > 0 && visibleItems.length === 0 ? (
            <section className="page-state filtered-empty" role="status" aria-label={t("filteredEmptyTitle")}>
              <h2>{t("filteredEmptyTitle")}</h2>
              <p>{t(symbols.dashboard.symbolsTruncated ? "filteredEmptyTruncatedDescription" : "filteredEmptyDescription")}</p>
              <button type="button" onClick={() => setFilter("")}>{t("clearFilter")}</button>
            </section>
          ) : null}
          <section className="symbol-grid" aria-label={t("monitoredSymbols")}>
            {visibleItems.map((item) => item.status === "ready" ? (
              <SymbolCard
                key={item.symbol.symbol}
                evidence={item.evidence}
                focusAction={item.focusAction}
                onDisable={handleDisable}
                onEnable={handleEnable}
                onMutationCommitted={handleMutationCommitted}
              />
            ) : item.status === "loading" ? (
              <article className="symbol-card symbol-card--state" aria-label={t("symbolEvidenceLoadingLabel", { symbol: item.symbol.symbol })} key={item.symbol.symbol}>
                <p role="status">{t("symbolEvidenceLoading", { symbol: item.symbol.symbol })}</p>
              </article>
            ) : item.status === "refreshing" || item.status === "refresh_error" ? (
              <RecoverableSymbolState key={item.symbol.symbol} item={item} onRetry={symbols.retrySymbol} />
            ) : (
              <article className="symbol-card symbol-card--state" aria-label={t("symbolEvidenceErrorLabel", { symbol: item.symbol.symbol })} key={item.symbol.symbol}>
                <p>{t("symbolEvidenceError", { symbol: item.symbol.symbol })}</p>
                <button type="button" onClick={() => void symbols.retrySymbol(item.symbol.symbol)}>{t("retrySymbolEvidence", { symbol: item.symbol.symbol })}</button>
              </article>
            ))}
          </section>
        </>
      ) : null}
    </div>
  );
}
