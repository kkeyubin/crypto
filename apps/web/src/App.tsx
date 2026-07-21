import { useTranslation } from "react-i18next";
import type { StrategySpec } from "./contracts";
import { setLocale } from "./i18n";
import { useHealth } from "./useHealth";

const navigationKeys = ["overview", "symbols", "strategies", "backtests", "paper", "operations"] as const;

type NavigationKey = (typeof navigationKeys)[number];

const navigationHref: Record<NavigationKey, string> = {
  overview: "#overview",
  symbols: "#symbols",
  strategies: "#strategies",
  backtests: "#backtests",
  paper: "#paper",
  operations: "#operations",
};

const noStrategyLoaded: StrategySpec | undefined = undefined;

export function App() {
  const { i18n, t } = useTranslation();
  const health = useHealth();
  const healthText = t(
    health === "healthy" ? "apiHealthy" : health === "loading" ? "apiLoading" : "apiUnavailable",
  );
  const healthStatus = healthText.replace(/^API\s/, "");

  return (
    <div className="app-shell">
      <header className="app-header">
        <a className="product-name" href="#overview" aria-label="Crypto Research home">
          CRYPTO RESEARCH
        </a>
        <div className="locale-switch" aria-label="Language">
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
            aria-label="EN"
            aria-pressed={i18n.language === "en"}
            onClick={() => void setLocale("en")}
          >
            EN
          </button>
        </div>
      </header>

      <aside className="sidebar">
        <nav aria-label={t("navigationLabel")}>
          {navigationKeys.map((key) => (
            <a key={key} href={navigationHref[key]} aria-current={key === "overview" ? "page" : undefined}>
              {t(key)}
            </a>
          ))}
        </nav>
      </aside>

      <main id="overview" className="main-content">
        <div className="content-frame">
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
        </div>
      </main>
    </div>
  );
}

void noStrategyLoaded;
