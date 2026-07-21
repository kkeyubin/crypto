import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { StrategySpec } from "./contracts";
import { setLocale } from "./i18n";
import { SymbolsPage } from "./SymbolsPage";
import { useHealth } from "./useHealth";

const pendingNavigationKeys = ["strategies", "backtests", "paper", "operations"] as const;

const noStrategyLoaded: StrategySpec | undefined = undefined;

export function App() {
  const { i18n, t } = useTranslation();
  const [view, setView] = useState<"overview" | "symbols">(
    window.location.hash === "#symbols" ? "symbols" : "overview",
  );
  const health = useHealth();
  const healthText = t(
    health === "healthy" ? "apiHealthy" : health === "loading" ? "apiLoading" : "apiUnavailable",
  );
  const healthStatus = healthText.replace(/^API\s/, "");

  useEffect(() => {
    const updateView = () => {
      setView(window.location.hash === "#symbols" ? "symbols" : "overview");
    };
    window.addEventListener("hashchange", updateView);
    return () => window.removeEventListener("hashchange", updateView);
  }, []);

  return (
    <div className="app-shell">
      <header className="app-header">
        <a className="product-name" href="#overview" aria-label={t("homeLabel")}>
          CRYPTO RESEARCH
        </a>
        <div className="locale-switch" role="group" aria-label={t("languageLabel")}>
          <button
            type="button"
            aria-label={t("switchToChinese")}
            aria-pressed={i18n.language === "zh-CN"}
            onClick={() => void setLocale("zh-CN")}
          >
            中文
          </button>
          <button
            type="button"
            aria-label={t("switchToEnglish")}
            aria-pressed={i18n.language === "en"}
            onClick={() => void setLocale("en")}
          >
            EN
          </button>
        </div>
      </header>

      <aside className="sidebar">
        <nav aria-label={t("navigationLabel")}>
          <a href="#overview" aria-current={view === "overview" ? "page" : undefined}>{t("overview")}</a>
          <a href="#symbols" aria-current={view === "symbols" ? "page" : undefined}>{t("symbols")}</a>
          {pendingNavigationKeys.map((key) => (
            <button key={key} type="button" disabled aria-label={t("unavailableNavigation", { label: t(key) })}>
              {t(key)} <span className="sr-only">{t("notAvailable")}</span>
            </button>
          ))}
        </nav>
      </aside>

      <main id={view} className="main-content">
        {view === "symbols" ? <SymbolsPage /> : <div className="content-frame">
          <p className="eyebrow">{t("shellNotice")}</p>
          <h1>{t("commandCenter")}</h1>
          <p className="muted evidence-notice">{t("evidenceNotice")}</p>

          <section className="status-grid" aria-label={t("foundation")}>
            <article>
              <h2>{t("foundation")}</h2>
              <p className="muted">{t("healthDescription")}</p>
              <p className={`status status--${health}`} aria-label={t("apiStatusLabel", { status: healthStatus })}>
                <span aria-hidden="true">●</span>
                {healthText}
              </p>
            </article>
            <article>
              <h2>{t("scope")}</h2>
              <p className="muted">{t("shellDescription")}</p>
              <p className="shell-state" aria-label={t("activeView")}>
                {t("overview")}
              </p>
            </article>
          </section>
        </div>}
      </main>
    </div>
  );
}

void noStrategyLoaded;
