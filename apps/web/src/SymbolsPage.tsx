import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { AddSymbolForm } from "./components/AddSymbolForm";
import { disableSymbol, enableSymbol, type SymbolEvidenceState } from "./apiClient";
import type { SymbolView } from "./contracts";
import { DataStatus } from "./components/DataStatus";
import { SymbolCard, type SymbolMutationResult } from "./components/SymbolCard";
import { useSymbols } from "./useSymbols";

type MutationKind = "disabled" | "enabled";

interface FocusIntent {
  readonly symbol: string;
  readonly token: number;
}

interface UserAction extends FocusIntent {
  readonly canceled: boolean;
}

interface MutationExpectation {
  readonly expectedEnabled: boolean;
  readonly committedUpdatedAt: string;
}

interface PendingMutation extends FocusIntent, MutationExpectation {
  readonly kind: MutationKind;
}

type ActionNotice =
  | {
    readonly kind: MutationKind;
    readonly actionToken: number;
    readonly symbol: string;
    readonly expectedEnabled: boolean;
    readonly committedUpdatedAt: string;
  }
  | {
    readonly kind: "disableError" | "enableError";
    readonly actionToken: number;
  };

function isStrictlyNewerOpposite(symbol: SymbolView, expected: MutationExpectation): boolean {
  const listedUpdatedAt = Date.parse(symbol.updated_at);
  const committedUpdatedAt = Date.parse(expected.committedUpdatedAt);
  return (
    Number.isFinite(listedUpdatedAt) &&
    Number.isFinite(committedUpdatedAt) &&
    listedUpdatedAt > committedUpdatedAt &&
    symbol.enabled !== expected.expectedEnabled
  );
}

function RecoverableSymbolState({ item, focusToken, onRetry }: {
  item: Extract<SymbolEvidenceState, { status: "refreshing" | "refresh_error" }>;
  focusToken?: number;
  onRetry: (symbol: string) => Promise<void>;
}) {
  const { t } = useTranslation();
  const owner = useRef<HTMLElement>(null);
  const retry = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (focusToken === undefined) {
      return;
    }
    if (item.status === "refreshing") {
      owner.current?.focus();
    } else {
      retry.current?.focus();
    }
  }, [focusToken, item.status]);

  return (
    <article
      ref={owner}
      className="symbol-card symbol-card--state"
      aria-label={t(item.status === "refreshing" ? "symbolEvidenceRefreshingLabel" : "symbolEvidenceRefreshErrorLabel", { symbol: item.symbol.symbol })}
      data-symbol-surface={item.symbol.symbol}
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
  const [focusIntent, setFocusIntent] = useState<FocusIntent | null>(null);
  const [pendingMutations, setPendingMutations] = useState<Record<string, PendingMutation>>({});
  const nextActionToken = useRef(0);
  const latestUserAction = useRef<UserAction | null>(null);
  const [actionNotice, setActionNotice] = useState<ActionNotice | null>(null);

  const reportActionNotice = useCallback((candidate: ActionNotice) => {
    setActionNotice((current) => {
      const latestStartedOrReported = Math.max(
        nextActionToken.current,
        current?.actionToken ?? 0,
      );
      if (candidate.actionToken < latestStartedOrReported || current?.actionToken === candidate.actionToken) {
        return current;
      }
      return candidate;
    });
  }, []);

  const beginUserAction = useCallback((symbol: string): number => {
    const token = nextActionToken.current + 1;
    nextActionToken.current = token;
    latestUserAction.current = { symbol, token, canceled: false };
    setFocusIntent(null);
    return token;
  }, []);

  const abandonUserAction = useCallback((token: number) => {
    if (latestUserAction.current?.token === token) {
      latestUserAction.current = null;
    }
    setFocusIntent((current) => current?.token === token ? null : current);
  }, []);

  const consumeFocusIntent = useCallback((token: number) => {
    abandonUserAction(token);
  }, [abandonUserAction]);

  const cancelFocusOutside = useCallback((target: EventTarget | null) => {
    const latest = latestUserAction.current;
    if (latest === null || !(target instanceof Element)) {
      return;
    }
    const surface = target.closest<HTMLElement>("[data-symbol-surface]");
    if (surface?.dataset.symbolSurface === latest.symbol) {
      return;
    }
    latestUserAction.current = { ...latest, canceled: true };
    setFocusIntent((current) => current?.token === latest.token ? null : current);
  }, []);

  const handleDisable = async (symbol: string): Promise<SymbolMutationResult | null> => {
    const actionToken = beginUserAction(symbol);
    setActionNotice(null);
    try {
      return { updated: await disableSymbol(symbol), actionToken };
    } catch {
      abandonUserAction(actionToken);
      reportActionNotice({ kind: "disableError", actionToken });
      return null;
    }
  };

  const handleEnable = async (symbol: SymbolView): Promise<SymbolMutationResult | null> => {
    const actionToken = beginUserAction(symbol.symbol);
    setActionNotice(null);
    try {
      return { updated: await enableSymbol(symbol), actionToken };
    } catch {
      abandonUserAction(actionToken);
      reportActionNotice({ kind: "enableError", actionToken });
      return null;
    }
  };

  const handleMutationCommitted = (mutation: SymbolMutationResult, kind: MutationKind) => {
    const { updated, actionToken } = mutation;
    setPendingMutations((current) => ({
      ...current,
      [updated.symbol]: {
        symbol: updated.symbol,
        token: actionToken,
        kind,
        expectedEnabled: updated.enabled,
        committedUpdatedAt: updated.updated_at,
      },
    }));
    const latest = latestUserAction.current;
    if (latest?.token === actionToken && !latest.canceled) {
      setFocusIntent({ symbol: updated.symbol, token: actionToken });
    }
    void symbols.refreshSymbol(updated);
  };

  useEffect(() => {
    if (symbols.status !== "ready") {
      return;
    }
    const completed: PendingMutation[] = [];
    const superseded: PendingMutation[] = [];
    for (const pending of Object.values(pendingMutations)) {
      const item = symbols.dashboard.items.find((candidate) => candidate.symbol.symbol === pending.symbol);
      if (item === undefined) {
        continue;
      }
      if (isStrictlyNewerOpposite(item.symbol, pending)) {
        superseded.push(pending);
      } else if (item.status === "ready" && item.symbol.enabled === pending.expectedEnabled) {
        completed.push(pending);
      }
    }
    const settled = [...completed, ...superseded];
    if (settled.length === 0) {
      return;
    }
    setPendingMutations((current) => {
      const next = { ...current };
      for (const mutation of settled) {
        if (next[mutation.symbol]?.token === mutation.token) {
          delete next[mutation.symbol];
        }
      }
      return next;
    });
    for (const mutation of superseded) {
      abandonUserAction(mutation.token);
    }
    const latestCompleted = completed.sort((left, right) => right.token - left.token)[0];
    if (latestCompleted !== undefined) {
      reportActionNotice({
        kind: latestCompleted.kind,
        actionToken: latestCompleted.token,
        symbol: latestCompleted.symbol,
        expectedEnabled: latestCompleted.expectedEnabled,
        committedUpdatedAt: latestCompleted.committedUpdatedAt,
      });
    }
  }, [abandonUserAction, pendingMutations, reportActionNotice, symbols]);

  useEffect(() => {
    if (
      symbols.status !== "ready" ||
      actionNotice === null ||
      !("symbol" in actionNotice)
    ) {
      return;
    }
    const item = symbols.dashboard.items.find((candidate) => candidate.symbol.symbol === actionNotice.symbol);
    if (item === undefined || !isStrictlyNewerOpposite(item.symbol, actionNotice)) {
      return;
    }
    setActionNotice((current) => current?.actionToken === actionNotice.actionToken ? null : current);
  }, [actionNotice, symbols]);
  const normalizedFilter = filter.trim().toUpperCase();
  const visibleItems = symbols.status === "ready"
    ? symbols.dashboard.items.filter((item) => item.symbol.symbol.includes(normalizedFilter))
    : [];
  const focusTokenFor = (item: SymbolEvidenceState): number | undefined => {
    if (focusIntent?.symbol !== item.symbol.symbol) {
      return undefined;
    }
    const pending = pendingMutations[item.symbol.symbol];
    return pending !== undefined && isStrictlyNewerOpposite(item.symbol, pending)
      ? undefined
      : focusIntent.token;
  };

  return (
    <div
      className="content-frame symbols-page"
      onFocusCapture={(event) => cancelFocusOutside(event.target)}
      onBlurCapture={(event) => {
        if (event.relatedTarget instanceof Element && !event.currentTarget.contains(event.relatedTarget)) {
          cancelFocusOutside(event.relatedTarget);
        }
      }}
    >
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
                focusToken={focusTokenFor(item)}
                onDisable={handleDisable}
                onEnable={handleEnable}
                onMutationCommitted={handleMutationCommitted}
                onFocusConsumed={consumeFocusIntent}
              />
            ) : item.status === "loading" ? (
              <article className="symbol-card symbol-card--state" aria-label={t("symbolEvidenceLoadingLabel", { symbol: item.symbol.symbol })} data-symbol-surface={item.symbol.symbol} key={item.symbol.symbol}>
                <p role="status">{t("symbolEvidenceLoading", { symbol: item.symbol.symbol })}</p>
              </article>
            ) : item.status === "refreshing" || item.status === "refresh_error" ? (
              <RecoverableSymbolState
                key={item.symbol.symbol}
                item={item}
                focusToken={focusTokenFor(item)}
                onRetry={symbols.retrySymbol}
              />
            ) : (
              <article className="symbol-card symbol-card--state" aria-label={t("symbolEvidenceErrorLabel", { symbol: item.symbol.symbol })} data-symbol-surface={item.symbol.symbol} key={item.symbol.symbol}>
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
