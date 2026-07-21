import i18n from "i18next";
import { initReactI18next } from "react-i18next";

export type Locale = "zh-CN" | "en";

const resources = {
  "zh-CN": {
    translation: {
      commandCenter: "指挥台",
      overview: "总览",
      symbols: "币种",
      strategies: "策略",
      backtests: "回测",
      paper: "模拟盘",
      operations: "运行",
      foundation: "系统基础",
      apiHealthy: "API 正常",
      apiLoading: "API 检查中",
      apiUnavailable: "API 不可用",
      apiStatusLabel: "API 状态：{{status}}",
      evidenceNotice: "每个币种独立获得证据与模拟盘启用许可。",
      shellNotice: "Phase 0 界面骨架",
      scope: "界面范围",
      shellDescription: "此界面仅提供导航与服务健康检查；尚未接入行情、策略结果、模拟盘订单或 AI 状态。",
      navigationLabel: "主导航",
      healthDescription: "服务健康检查",
      activeView: "当前页面",
      switchToChinese: "切换为中文",
      switchToEnglish: "Switch to English",
    },
  },
  en: {
    translation: {
      commandCenter: "Command Center",
      overview: "Overview",
      symbols: "Symbols",
      strategies: "Strategies",
      backtests: "Backtests",
      paper: "Paper",
      operations: "Operations",
      foundation: "System Foundation",
      apiHealthy: "API Healthy",
      apiLoading: "Checking API",
      apiUnavailable: "API Unavailable",
      apiStatusLabel: "API status: {{status}}",
      evidenceNotice: "Each symbol earns independent evidence and paper approval.",
      shellNotice: "Phase 0 interface shell",
      scope: "Shell scope",
      shellDescription: "This view only provides navigation and service health. It does not show market data, strategy performance, paper orders, or AI status.",
      navigationLabel: "Primary navigation",
      healthDescription: "Service health check",
      activeView: "Current page",
      switchToChinese: "Switch to Chinese",
      switchToEnglish: "Switch to English",
    },
  },
};

const storedLocale = localStorage.getItem("crypto-locale");
const initialLocale: Locale = storedLocale === "en" ? "en" : "zh-CN";

document.documentElement.lang = initialLocale;

void i18n.use(initReactI18next).init({
  resources,
  lng: initialLocale,
  fallbackLng: "zh-CN",
  interpolation: { escapeValue: false },
});

export async function setLocale(locale: Locale): Promise<void> {
  localStorage.setItem("crypto-locale", locale);
  document.documentElement.lang = locale;
  await i18n.changeLanguage(locale);
}

export default i18n;
