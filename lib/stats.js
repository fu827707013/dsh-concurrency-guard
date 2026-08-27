/**
 * 持久化统计：跨重启的按天汇总 + 异常分类聚合。
 *
 * 目标：无论进程重启多少次，"今天一共发了多少请求、失败多少次、失败分类分布"
 * 都能连续累计。数据随 state.json 落盘（250ms 防抖），启动时读回。
 *
 * 结构（全部为可 JSON 序列化的纯对象）：
 *   stats.firstSeenAt           统计起始时间（首个记录）
 *   stats.totals                ...全历史累计
 *   stats.todayKey              当日键 "YYYY-MM-DD"（本地时区）
 *   stats.today                 { started, completed, cancelled, errored, gateHeld, failOpen,
 *                                  byKind: {main:n,...}, byErrKind: {rate_limit:n,...} }
 *   stats.byDay                 { "YYYY-MM-DD": {...同 today 结构...} }  已归档的历史天
 *
 * 设计取舍：存聚合不存明细——按天汇总体积恒定（每键 ~200B），
 * 不需要像原始 history 那样限条数/TTL；原始明细仍由 history（30 条/full）
 * 提供短期窗口。
 */

/** 异常分类：把 finish(error) 的错误对象/消息归入可读类别。 */
export function classifyErrorKind(error) {
  const text = String(
    (error instanceof Error ? error.message : error?.message) ?? error ?? ""
  ).toLowerCase();
  if (!text) return "unknown";
  if (/rate.?limit|too many requests|\b429\b/.test(text)) return "rate_limit";
  if (/timeout|timed out|deadline|etimedout/i.test(text)) return "timeout";
  if (/econn|enetdown|ehost|eai_again|socket|network|fetch failed/i.test(text)) return "network";
  if (/abort|aborterror|user aborted/i.test(text)) return "aborted";
  if (/unauthorized|forbidden|\b401\b|\b403\b|auth/i.test(text)) return "auth";
  if (/provider|upstream|bad gateway|\b502\b|\b503\b|\b5\d\d\b|api error/i.test(text)) return "provider";
  return "other";
}

/** 本地时区当日键 "YYYY-MM-DD"。 */
export function dayKey(now = Date.now()) {
  const d = new Date(now);
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

const EMPTY_DAY = () => ({
  started: 0,
  completed: 0,
  cancelled: 0,
  interrupted: 0,
  errored: 0,
  gateHeld: 0,
  failOpen: 0,
  warnHits: 0,
  byKind: {},
  byErrKind: {}
});

/** 新建一份空统计。 */
export function createStats(now = Date.now()) {
  return { firstSeenAt: null, totals: EMPTY_DAY(), todayKey: dayKey(now), today: EMPTY_DAY(), byDay: {} };
}

/** 记录一次请求开始。 */
export function recordStarted(stats, now = Date.now()) {
  stats.firstSeenAt ??= now;
  stats.totals.started += 1;
  stats.today.started += 1;
}

/**
 * 记录一次请求收尾（finish 时调用）。gateHeld/failOpen/warnHits 由 gate.js
 * 就地同步（它们不是"每条请求一次"的语义），这里只处理 状态 + 来源分类 + 异常分类。
 * @param {object} stats
 * @param {string} status - ok|cancelled|error
 * @param {string} [errKind] - 异常分类（status=error 时）
 * @param {string} [kind] - 请求来源分类（main/subagent/...）
 * @param {number} [now]
 */
export function recordFinish(stats, status, errKind, kind, now = Date.now()) {
  // 跨天回滚：把 today 归档进 byDay，开新桶
  const key = dayKey(now);
  if (key !== stats.todayKey) {
    const prev = stats.byDay[stats.todayKey] ?? EMPTY_DAY();
    stats.byDay[stats.todayKey] = mergeDay(prev, stats.today);
    stats.todayKey = key;
    stats.today = EMPTY_DAY();
  }
  const t = stats.today;
  const totals = stats.totals;
  const inc = (obj, k) => { obj[k] = (obj[k] ?? 0) + 1; };

  if (status === "ok") { t.completed += 1; totals.completed += 1; }
  else if (status === "cancelled") { t.cancelled += 1; totals.cancelled += 1; }
  else if (status === "interrupted") { t.interrupted += 1; totals.interrupted += 1; }
  else { t.errored += 1; totals.errored += 1; if (errKind) { inc(t.byErrKind, errKind); inc(totals.byErrKind, errKind); } }
  if (kind) { inc(t.byKind, kind); inc(totals.byKind, kind); }
}

function mergeDay(a, b) {
  const out = { ...a, started: a.started + b.started, completed: a.completed + b.completed, cancelled: a.cancelled + b.cancelled, interrupted: (a.interrupted ?? 0) + (b.interrupted ?? 0), errored: a.errored + b.errored, gateHeld: (a.gateHeld ?? 0) + (b.gateHeld ?? 0), failOpen: (a.failOpen ?? 0) + (b.failOpen ?? 0), byKind: { ...a.byKind }, byErrKind: { ...a.byErrKind } };
  for (const [k, v] of Object.entries(b.byKind)) out.byKind[k] = (out.byKind[k] ?? 0) + v;
  for (const [k, v] of Object.entries(b.byErrKind)) out.byErrKind[k] = (out.byErrKind[k] ?? 0) + v;
  return out;
}

/**
 * 启动时把磁盘里读到的 stats 接回内存（尽力而为：结构非法则放弃该部分）。
 * @param {object|null|undefined} raw - 已解析的 state.json 的 stats 字段。
 */
export function adoptStats(raw) {
  const stats = createStats();
  if (!raw || typeof raw !== "object") return stats;
  const num = (v) => (Number.isFinite(v) && v >= 0 ? v : 0);
  const sanitizeDay = (d) => {
    const base = EMPTY_DAY();
    if (!d || typeof d !== "object") return base;
    for (const k of ["started", "completed", "cancelled", "interrupted", "errored", "gateHeld", "failOpen", "warnHits"]) base[k] = num(d[k]);
    if (d.byKind && typeof d.byKind === "object") for (const [k, v] of Object.entries(d.byKind)) if (typeof v === "number" && v >= 0) base.byKind[k] = v;
    if (d.byErrKind && typeof d.byErrKind === "object") for (const [k, v] of Object.entries(d.byErrKind)) if (typeof v === "number" && v >= 0) base.byErrKind[k] = v;
    return base;
  };
  if (typeof raw.firstSeenAt === "number") stats.firstSeenAt = raw.firstSeenAt;
  stats.totals = sanitizeDay(raw.totals);
  stats.todayKey = typeof raw.todayKey === "string" ? raw.todayKey : dayKey();
  stats.today = sanitizeDay(raw.today);
  if (raw.byDay && typeof raw.byDay === "object") {
    for (const [k, v] of Object.entries(raw.byDay)) {
      if (/^\d{4}-\d{2}-\d{2}$/.test(k)) stats.byDay[k] = sanitizeDay(v);
    }
  }
  return stats;
}

/** 面板展示用的最近 N 天（含今天，倒序）。 */
export function recentDays(stats, n = 7) {
  const out = [];
  const today = stats.todayKey;
  const seen = new Set([today]);
  out.push({ date: today, ...stats.today });
  const dates = Object.keys(stats.byDay).sort().reverse();
  for (const d of dates) {
    if (seen.has(d)) continue;
    seen.add(d);
    out.push({ date: d, ...stats.byDay[d] });
    if (out.length >= n) break;
  }
  return out;
}