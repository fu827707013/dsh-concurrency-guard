/**
 * 配置解析与常量。
 *
 * 配置来源（优先级从高到低）：
 *  1. 运行时 ctx.concurrencyGuard.configure({...}) 修改后的内存配置
 *  2. 插件 loader 传入的 config 对象（apply(ctx, config)）
 *  3. 环境变量 DSH_CG_*
 *  4. 默认值
 *
 * 纯 node 内建，零依赖；不 import 任何 @deepseek-ai 包（最大兼容性）。
 */
import { homedir } from "node:os";
import { join } from "node:path";

export const DEFAULTS = Object.freeze({
  /** 并发上限（供应商/中继允许的最大同时模型请求数） */
  maxConcurrency: 5,
  /** queue=排队节制（默认）；monitor=只监控不拦 */
  mode: "queue",
  /** 活跃并发达到该值记 warn 日志 */
  warnAt: 4,
  /** 排队最长等待 ms；超时强制放行（fail-open）；0=无限等待 */
  maxQueueWaitMs: 300_000,
  /** 最近完成历史保留条数（硬上限，环形截断） */
  history: 30,
  /** 最近完成历史时间 TTL ms；超龄记录自动清理；0=关闭（只靠条数上限） */
  historyTtlMs: 3_600_000,
  /** 流式请求"无活动"判死阈值 ms：消费端弃流后按最后一次 chunk 心跳超时记 interrupted */
  maxStreamStallMs: 600_000,
  /** 滞留清扫周期 ms（内部/测试用，正常无需改动） */
  sweepIntervalMs: 60_000,

  // ---- 会话级并发控制（v1.5.0+） ----
  /** 会话级限额总开关：false 时跳过会话门（限额保留但惰性生效） */
  sessionLimitsEnabled: true,
  /** 辅助请求（压缩/标题）豁免会话门：只过全局门，避免书签性请求被会话限流拖死 */
  sessionExemptAuxiliary: true,
  /** 会话限额条目自动过期天数：0 = 永久保留；>0 时按 lastUsedAt 清扫未再活动的条目 */
  sessionLimitTtlDays: 0,
  /** 会话"在线"判定窗口 ms：注册表在途或最近活动落在窗口内即视为在线 */
  onlineWindowMs: 600_000,
  /** 会话标题后台刷新周期 ms（惰性接入 ctx.sessionQuery，缺失自动降级） */
  sessionTitleRefreshMs: 30_000,
  /** 历史数据保留天数：0 = 关闭（全量保留，靠各块自身上限）；>0 时跨天归档/启动装载自动裁剪为最近 N 天 */
  dayRetention: 0
});

function envNumber(raw, fallback) {
  const value = Number(raw);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

/** 默认状态文件：$DSH_HOME/concurrency-guard/state.json（DSH_HOME 缺省 ~/.dsh） */
export function defaultStateFile() {
  const home = process.env.DSH_HOME?.trim() || join(homedir(), ".dsh");
  return join(home, "concurrency-guard", "state.json");
}

function normNumber(value, fallback, { min = 0 } = {}) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, n);
}

function normBool(value, fallback) {
  if (value === void 0 || value === null || value === "") return fallback;
  return value === true || value === "true" || value === "1" || value === 1;
}

/**
 * 解析/合并一份配置。
 * @param {object|null|undefined} config - 插件 loader 传入的配置（可选）。
 * @returns {Readonly<{mode:"queue"|"monitor",maxConcurrency:number,warnAt:number,maxQueueWaitMs:number,history:number,stateFile:string}>}
 */
export function resolveConfig(config) {
  const env = process.env;
  const rawMode = config?.mode ?? env.DSH_CG_MODE ?? DEFAULTS.mode;
  return Object.freeze({
    mode: rawMode === "monitor" ? "monitor" : "queue",
    maxConcurrency: envNumber(config?.maxConcurrency ?? env.DSH_CG_MAX_CONCURRENCY, DEFAULTS.maxConcurrency),
    warnAt: envNumber(config?.warnAt ?? env.DSH_CG_WARN_AT, DEFAULTS.warnAt),
    maxQueueWaitMs:
      Number.isFinite(Number(env.DSH_CG_MAX_QUEUE_WAIT_MS))
        ? Math.max(0, Number(env.DSH_CG_MAX_QUEUE_WAIT_MS))
        : Number.isFinite(Number(config?.maxQueueWaitMs))
          ? Math.max(0, Number(config.maxQueueWaitMs))
          : DEFAULTS.maxQueueWaitMs,
    history: Math.max(0, Math.floor(normNumber(config?.history ?? env.DSH_CG_HISTORY, DEFAULTS.history))),
    historyTtlMs: envNumber(config?.historyTtlMs ?? env.DSH_CG_HISTORY_TTL_MS, DEFAULTS.historyTtlMs),
    maxStreamStallMs: envNumber(config?.maxStreamStallMs ?? env.DSH_CG_MAX_STREAM_STALL_MS, DEFAULTS.maxStreamStallMs),
    sweepIntervalMs: Math.max(1000, Math.floor(envNumber(config?.sweepIntervalMs ?? env.DSH_CG_SWEEP_INTERVAL_MS, DEFAULTS.sweepIntervalMs))),
    sessionLimitsEnabled: normBool(config?.sessionLimitsEnabled ?? env.DSH_CG_SESSION_LIMITS_ENABLED, DEFAULTS.sessionLimitsEnabled),
    sessionExemptAuxiliary: normBool(config?.sessionExemptAuxiliary ?? env.DSH_CG_SESSION_EXEMPT_AUXILIARY, DEFAULTS.sessionExemptAuxiliary),
    sessionLimitTtlDays: Math.max(0, Math.floor(normNumber(config?.sessionLimitTtlDays ?? env.DSH_CG_SESSION_LIMIT_TTL_DAYS, DEFAULTS.sessionLimitTtlDays))),
    onlineWindowMs: envNumber(config?.onlineWindowMs ?? env.DSH_CG_ONLINE_WINDOW_MS, DEFAULTS.onlineWindowMs),
    sessionTitleRefreshMs: Math.max(5000, Math.floor(envNumber(config?.sessionTitleRefreshMs ?? env.DSH_CG_SESSION_TITLE_REFRESH_MS, DEFAULTS.sessionTitleRefreshMs))),
    dayRetention: Math.max(0, Math.floor(normNumber(config?.dayRetention ?? env.DSH_CG_DAY_RETENTION, DEFAULTS.dayRetention))),
    stateFile: (config?.stateFile ?? (env.DSH_CG_STATE_FILE || defaultStateFile())).trim()
  });
}

/**
 * 运行时局部修改配置（configure API 使用）：逐字段覆盖 + 归一化。
 * @param {object} current - 当前配置。
 * @param {object} partial - 需要修改的字段。
 * @returns 新的不可变配置。
 */
export function mergeConfig(current, partial) {
  const next = {
    ...current,
    ...Object.fromEntries(Object.entries(partial).filter(([, v]) => v !== void 0))
  };
  return Object.freeze({
    mode: next.mode === "monitor" ? "monitor" : "queue",
    maxConcurrency: envNumber(next.maxConcurrency, current.maxConcurrency),
    warnAt: envNumber(next.warnAt, current.warnAt),
    maxQueueWaitMs:
      Number.isFinite(Number(next.maxQueueWaitMs)) ? Math.max(0, Number(next.maxQueueWaitMs)) : current.maxQueueWaitMs,
    history: Math.max(0, Math.floor(normNumber(next.history, current.history))),
    historyTtlMs: envNumber(next.historyTtlMs, current.historyTtlMs),
    maxStreamStallMs: envNumber(next.maxStreamStallMs, current.maxStreamStallMs),
    sweepIntervalMs: Math.max(1000, Math.floor(envNumber(next.sweepIntervalMs, current.sweepIntervalMs))),
    sessionLimitsEnabled: normBool(next.sessionLimitsEnabled, current.sessionLimitsEnabled),
    sessionExemptAuxiliary: normBool(next.sessionExemptAuxiliary, current.sessionExemptAuxiliary),
    sessionLimitTtlDays: Math.max(0, Math.floor(normNumber(next.sessionLimitTtlDays, current.sessionLimitTtlDays))),
    onlineWindowMs: envNumber(next.onlineWindowMs, current.onlineWindowMs),
    sessionTitleRefreshMs: Math.max(5000, Math.floor(envNumber(next.sessionTitleRefreshMs, current.sessionTitleRefreshMs))),
    dayRetention: Math.max(0, Math.floor(normNumber(next.dayRetention, current.dayRetention))),
    stateFile: typeof next.stateFile === "string" && next.stateFile.trim() !== "" ? next.stateFile.trim() : current.stateFile
  });
}

/** 人类可读的请求标签（日志用）。 */
export function requestLabel(rec) {
  const purpose = rec.purpose ? ` (${rec.purpose})` : "";
  const sid = rec.sessionId ? ` sid=${rec.sessionId}` : "";
  const gk = rec.gateKey && rec.gateKey !== rec.sessionId ? ` root=${rec.gateKey}` : "";
  return `${rec.provider}/${rec.model}${purpose}${sid}${gk}`;
}