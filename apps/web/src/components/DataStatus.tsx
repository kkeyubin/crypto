import { useTranslation } from "react-i18next";
import type { MarketDataHealthView } from "../contracts";

interface DataStatusProps {
  health: MarketDataHealthView;
}

export function DataStatus({ health }: DataStatusProps) {
  const { t } = useTranslation();
  // Task 6 centralizes heartbeat, exact-stream, and freshness policy in source_mode.
  const liveHealthy = health.source_mode !== "degraded";
  const sourceTone = health.source_mode === "degraded" ? "caution" : "neutral";

  return (
    <section className="data-health" aria-label={t("dataHealthTitle")}>
      <div className={`health-cell health-cell--${sourceTone}`}>
        <span className="health-label">{t("sourcePath")}</span>
        <strong><span aria-hidden="true">↗</span> {t(`sourceMode.${health.source_mode}`)}</strong>
      </div>
      <div className={`health-cell health-cell--${health.archive_healthy ? "healthy" : "caution"}`}>
        <span className="health-label">{t("archiveSource")}</span>
        <strong><span aria-hidden="true">{health.archive_healthy ? "✓" : "!"}</span> {t(health.archive_healthy ? "archiveHealthy" : "archiveLimited")}</strong>
      </div>
      <div className={`health-cell health-cell--${liveHealthy ? "healthy" : "caution"}`}>
        <span className="health-label">{t("liveSource")}</span>
        <strong><span aria-hidden="true">{liveHealthy ? "✓" : "!"}</span> {t(liveHealthy ? "liveHealthy" : "liveLimited")}</strong>
      </div>
      <div className={`health-cell health-cell--${health.rest_healthy ? "healthy" : "caution"}`}>
        <span className="health-label">{t("restMetadataSource")}</span>
        <strong><span aria-hidden="true">{health.rest_healthy ? "✓" : "!"}</span> {t(health.rest_healthy ? "restHealthy" : "restLimited")}</strong>
      </div>
    </section>
  );
}
