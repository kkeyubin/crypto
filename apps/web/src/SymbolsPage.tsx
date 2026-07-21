import { useState } from "react";
import { useTranslation } from "react-i18next";
import { AddSymbolForm } from "./components/AddSymbolForm";
import { disableSymbol } from "./apiClient";
import { DataStatus } from "./components/DataStatus";
import { SymbolCard } from "./components/SymbolCard";
import { useSymbols } from "./useSymbols";

export function SymbolsPage() {
  const { t } = useTranslation();
  const symbols = useSymbols();
  const [showAddForm, setShowAddForm] = useState(false);
  const [filter, setFilter] = useState("");
  const [actionNotice, setActionNotice] = useState<{ kind: "success" | "error"; symbol?: string } | null>(null);

  const handleDisable = async (symbol: string) => {
    setActionNotice(null);
    try {
      await disableSymbol(symbol);
      setActionNotice({ kind: "success", symbol });
      symbols.reload();
    } catch {
      setActionNotice({ kind: "error" });
    }
  };

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
          <button className="primary-action" type="button" aria-expanded={showAddForm} onClick={() => setShowAddForm((visible) => !visible)}>
            {showAddForm ? t("closeAddSymbol") : t("addSymbol")}
          </button>
        </div>
      </div>
      {showAddForm ? <AddSymbolForm onAdded={symbols.reload} /> : null}
      {actionNotice === null ? null : actionNotice.kind === "success" ? (
        <p className="action-notice" role="status" aria-label={t("symbolDisabledLabel")}>
          {t("symbolDisabledNotice", { symbol: actionNotice.symbol })}
        </p>
      ) : (
        <p className="inline-error" role="alert">{t("disableSymbolError")}</p>
      )}
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
          <DataStatus dashboard={symbols.dashboard} />
          <section className="symbol-grid" aria-label={t("monitoredSymbols")}>
            {symbols.dashboard.items.filter((evidence) => evidence.symbol.symbol.includes(filter.trim().toUpperCase())).map((evidence) => (
              <SymbolCard key={evidence.symbol.symbol} evidence={evidence} onDisable={handleDisable} />
            ))}
          </section>
        </>
      ) : null}
    </div>
  );
}
