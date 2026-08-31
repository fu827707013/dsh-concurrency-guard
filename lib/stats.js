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
 *   stats.errorDetails          { "<归一化错误信息>": { message, code, kind, count, firstAt, lastAt,
 *                                  status, requestId, retryAfterMs, lastSessionId, lastModel,
 *                                  lastProvider, sessions: { "<sessionId>": { count, lastAt } } } }
 *                                 全局异常聚合（跨天/跨重启累计，按错误信息聚类，上限 MAX_ERROR_DETAILS 条）
 *   stats.errorEvents           [ { at, sessionId, provider, model, kind, code, status, requestId, message } ]
 *                                 逐条错误事件（按时间升序，环形上限 ERROR_EVENT_CAP 条）——明细报表数据源
 *
 * 设计取舍：errorDetails 只做"同信息异常次数"聚合（上限 100 键，每条 ~400B 以内，体积有界）；
 * errorEvents 提供"每次错误一条 + 时间"的明细窗口（200 条 × ~350B ≈ 70KB），
 * 滚动淘汰最旧事件，配合 errorDetails 的全历史次数形成"汇总 + 明细"两层。
 */

/** errorDetails 键数上限：超限按 lastAt 淘汰最旧（保留最近高频/最近发生）。 */
const MAX_ERROR_DETAILS = 100;

/** errorEvents 逐条错误事件上限：超限按时间淘汰最旧。 */
const ERROR_EVENT_CAP = 200;

/**
 * 错误信息归一化键：把 URL / ISO 时间戳 / 长数字抹平，
 * 让"同一类错误、仅 URL 或时间戳不同"的消息聚到同一条（例如
 * "request to http://a/v1 failed" 与 "request to http://b/v1 failed"）。
 */
export function normalizeErrorKey(msg) {
  const m = String(msg ?? "").trim();
  if (!m) return "(empty)";
  return m
    .replace(/https?:\/\/[^\s"')\]]+/gi, "<url>")
    .replace(/\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?([.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b/g, "<ts>")
    .replace(/\b\d{9,}\b/g, "<num>")
    .slice(0, 240);
}

/** 异常分类：把 finish(error) 的错误对象/消息归入可读类别。 */
export function classifyErrorKind(error) {
  const text = String(
    (error instanceof Error ? error.message : error?.message) ?? error ?? ""
  ).toLowerCase();
  if (!text) return "unknown";
  if (/rate.?limit|too many requests|\b429\b/.test(text)) return "rate_limit";
  if (/timeout|timed out|deadline|etimedout/i.test(text)) return "timeout";
  // TRANSPORT / "request to <url> failed"（dsh finish chunk 的 failure.code=TRANSPORT）归为网络层
  if (/econn|enetdown|ehost|eai_again|socket|network|fetch failed|transport|request to .* failed/i.test(text)) return "network";
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
  sessionGateHeld: 0,
  sessionFailOpen: 0,
  warnHits: 0,
  byKind: {},
  byErrKind: {}
});

/** 新建一份空统计。 */
export function createStats(now = Date.now()) {
  return { firstSeenAt: null, totals: EMPTY_DAY(), todayKey: dayKey(now), today: EMPTY_DAY(), byDay: {}, errorDetails: {}, errorEvents: [] };
}

/** 记录一次请求开始。 */
export function recordStarted(stats, now = Date.now()) {
  stats.firstSeenAt ??= now;
  stats.totals.started += 1;
  stats.today.started += 1;
}

/**
 * 记录一次请求收尾（finish 时调用）。gateHeld/failOpen/warnHits 由 gate.js
 * 就地同步（它们不是"每条请求一次"的语义），这里只处理 状态 + 来源分类 + 异常分类 + 异常明细。
 * @param {object} stats
 * @param {string} status - ok|cancelled|error|interrupted
 * @param {string} [errKind] - 异常分类（status=error 时）
 * @param {string} [kind] - 请求来源分类（main/subagent/...）
 * @param {number} [now]
 * @param {{msg?: string, code?: string|number|null, sessionId?: string|null, provider?: string, model?: string}} [errorInfo] - 异常原文/错误码/上下文（status=error 时，用于 errorDetails 明细聚合）
 */
export function recordFinish(stats, status, errKind, kind, now = Date.now(), errorInfo) {
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
  else {
    t.errored += 1; totals.errored += 1;
    if (errKind) { inc(t.byErrKind, errKind); inc(totals.byErrKind, errKind); }
    if (errorInfo) {
      recordErrorDetail(stats, errorInfo, errKind, now);
      recordErrorEvent(stats, errorInfo, errKind, now);
    }
  }
  if (kind) { inc(t.byKind, kind); inc(totals.byKind, kind); }
}

/**
 * 逐条错误事件（明细报表数据源）：按时间升序追加，环形上限 ERROR_EVENT_CAP 条。
 * 与 errorDetails（聚合）互补：聚合回答"同错误总共几次/哪些会话"，
 * 事件流回答"某会话某时刻具体发生了哪次错误"。
 */
function recordErrorEvent(stats, errorInfo, kind, now) {
  const events = stats.errorEvents ?? (stats.errorEvents = []);
  events.push({
    at: now,
    sessionId: errorInfo.sessionId ?? null,
    provider: errorInfo.provider ?? null,
    model: errorInfo.model ?? null,
    sourceKind: typeof errorInfo.sourceKind === "string" ? errorInfo.sourceKind : null,
    kind: kind ?? "unknown",
    code: errorInfo.code !== void 0 && errorInfo.code !== null ? String(errorInfo.code) : null,
    status: Number.isInteger(errorInfo.status) && errorInfo.status >= 100 && errorInfo.status <= 599
      ? errorInfo.status
      : null,
    requestId: typeof errorInfo.requestId === "string" && errorInfo.requestId.length > 0
      ? errorInfo.requestId.slice(0, 128)
      : null,
    message: String(errorInfo.msg ?? "").trim().slice(0, 500) || "(empty)"
  });
  if (events.length > ERROR_EVENT_CAP) events.splice(0, events.length - ERROR_EVENT_CAP);
}

/** errorDetails 单键的会话上下文上限：只保留最近高频的 N 个会话（防体积膨胀）。 */
const SESSION_CAP = 12;

/**
 * 异常明细聚合：按归一化错误信息归并，累计相同异常出现次数（跨重启保留）。
 * 同一 key 更新：count+1、lastAt=now、message/code/kind/上下文取最近一次；
 * 首次出现：firstAt=now。会话维度按"每个会话出现次数"记录（上限 SESSION_CAP）。
 * 超过 MAX_ERROR_DETAILS 时淘汰 lastAt 最旧的键。
 */
function recordErrorDetail(stats, errorInfo, kind, now) {
  const key = normalizeErrorKey(errorInfo.msg);
  const text = String(errorInfo.msg ?? "").trim().slice(0, 500); // 保留尽量完整的原始错误信息
  const code = errorInfo.code !== void 0 && errorInfo.code !== null ? String(errorInfo.code) : null;
  const sid = errorInfo.sessionId ?? null;
  const sourceKind = typeof errorInfo.sourceKind === "string" ? errorInfo.sourceKind : null;
  // LlmFailure 协议内的补充诊断字段（HTTP 状态 / 供应商 requestId / Retry-After）
  const status = Number.isInteger(errorInfo.status) && errorInfo.status >= 100 && errorInfo.status <= 599
    ? errorInfo.status
    : null;
  const requestId = typeof errorInfo.requestId === "string" && errorInfo.requestId.length > 0
    ? errorInfo.requestId.slice(0, 128)
    : null;
  const retryAfterMs = Number.isFinite(errorInfo.retryAfterMs) && errorInfo.retryAfterMs > 0
    ? Math.round(errorInfo.retryAfterMs)
    : null;
  const details = stats.errorDetails ?? (stats.errorDetails = {});
  const ex = details[key];
  if (ex) {
    ex.count += 1;
    if (text) ex.message = text;
    if (code) ex.code = code;
    if (kind) ex.kind = kind;
    if (sourceKind) ex.lastSourceKind = sourceKind;
    if (status !== null) ex.status = status;
    if (requestId) ex.requestId = requestId;
    if (retryAfterMs !== null) ex.retryAfterMs = retryAfterMs;
    ex.lastAt = now;
  } else {
    details[key] = {
      message: text || key,
      code,
      kind: kind ?? "unknown",
      count: 1,
      firstAt: now,
      lastAt: now,
      lastSessionId: sid,
      lastModel: errorInfo.model ?? null,
      lastProvider: errorInfo.provider ?? null,
      lastSourceKind: sourceKind,
      status,
      requestId,
      retryAfterMs,
      sessions: {}
    };
  }
  // 会话维度累计（最近一次会话始终单独记录在 lastSessionId）
  if (ex) {
    ex.lastSessionId = sid;
    if (errorInfo.model !== void 0) ex.lastModel = errorInfo.model;
    if (errorInfo.provider !== void 0) ex.lastProvider = errorInfo.provider;
    const sKey = sid ?? "(none)";
    ex.sessions ??= {};
    const prev = ex.sessions[sKey];
    ex.sessions[sKey] = typeof prev === "number"
      ? { count: prev + 1, lastAt: now }
      : { count: (prev?.count ?? 0) + 1, lastAt: now };
    const sKeys = Object.keys(ex.sessions);
    if (sKeys.length > SESSION_CAP) {
      // 淘汰 count 最少、其次 lastAt 最旧的会话，保持上限
      const victim = sKeys.reduce((a, b) => {
        const A = ex.sessions[a];
        const B = ex.sessions[b];
        const ac = typeof A === "number" ? A : (A?.count ?? 0);
        const bc = typeof B === "number" ? B : (B?.count ?? 0);
        const al = typeof A === "number" ? 0 : (A?.lastAt ?? 0);
        const bl = typeof B === "number" ? 0 : (B?.lastAt ?? 0);
        return ac < bc || (ac === bc && al <= bl) ? a : b;
      });
      delete ex.sessions[victim];
    }
  } else {
    // 首次创建时也已初始化 sessions
    details[key].sessions[sid ?? "(none)"] = { count: 1, lastAt: now };
  }
  const keys = Object.keys(details);
  if (keys.length > MAX_ERROR_DETAILS) {
    // 淘汰 lastAt 最旧的键，直到回到上限
    const excess = keys.length - MAX_ERROR_DETAILS;
    const sorted = keys
      .map((k) => ({ k, lastAt: details[k].lastAt ?? 0 }))
      .sort((a, b) => a.lastAt - b.lastAt);
    for (let i = 0; i < excess; i++) delete details[sorted[i].k];
  }
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
  // 异常明细：结构非法条目丢弃（防御性读取，上限防呆）
  if (raw.errorDetails && typeof raw.errorDetails === "object") {
    const num = (v, d = 0) => (Number.isFinite(v) && v > 0 ? v : d);
    for (const [k, v] of Object.entries(raw.errorDetails)) {
      if (!k || typeof k !== "string" || !v || typeof v !== "object") continue;
      if (typeof v.message !== "string" && typeof v.message !== "number") continue;
      // 会话上下文（v1.3.5+）：旧数据缺失时补默认值；值兼容 number（旧）与 {count,lastAt}（新）
      const sessions = {};
      if (v.sessions && typeof v.sessions === "object") {
        for (const [sk, sc] of Object.entries(v.sessions)) {
          const key = sk.slice(0, 64);
          if (typeof sc === "number") {
            if (sc > 0) sessions[key] = { count: sc, lastAt: num(v.lastAt, num(v.firstAt)) };
          } else if (sc && typeof sc === "object") {
            const c = num(sc.count);
            if (c > 0) sessions[key] = { count: c, lastAt: num(sc.lastAt, num(v.lastAt, num(v.firstAt))) };
          }
        }
      }
      stats.errorDetails[k] = {
        message: String(v.message).slice(0, 500),
        code: v.code !== void 0 && v.code !== null ? String(v.code) : null,
        kind: typeof v.kind === "string" ? v.kind : "unknown",
        count: num(v.count),
        firstAt: num(v.firstAt),
        lastAt: num(v.lastAt, num(v.firstAt)),
        lastSessionId: typeof v.lastSessionId === "string" ? v.lastSessionId : null,
        lastModel: typeof v.lastModel === "string" ? v.lastModel : null,
        lastProvider: typeof v.lastProvider === "string" ? v.lastProvider : null,
        lastSourceKind: typeof v.lastSourceKind === "string" ? v.lastSourceKind : null,
        status: Number.isInteger(v.status) && v.status >= 100 && v.status <= 599 ? v.status : null,
        requestId: typeof v.requestId === "string" ? v.requestId.slice(0, 128) : null,
        retryAfterMs: Number.isFinite(v.retryAfterMs) && v.retryAfterMs > 0 ? Math.round(v.retryAfterMs) : null,
        sessions
      };
      // 防御性截断：每键会话上下文不超过 SESSION_CAP（保留 count 最多、其次 lastAt 最新）
      const sKeys = Object.keys(sessions);
      if (sKeys.length > SESSION_CAP) {
        sKeys.sort((a, b) =>
          (sessions[b].count - sessions[a].count) || (sessions[b].lastAt - sessions[a].lastAt)
        );
        for (const sk of sKeys.slice(SESSION_CAP)) delete sessions[sk];
      }
    }
    const keys = Object.keys(stats.errorDetails);
    if (keys.length > MAX_ERROR_DETAILS) {
      keys.sort((a, b) => (stats.errorDetails[a].lastAt ?? 0) - (stats.errorDetails[b].lastAt ?? 0));
      for (let i = 0; i < keys.length - MAX_ERROR_DETAILS; i++) delete stats.errorDetails[keys[i]];
    }
  }
  // 逐条错误事件（v1.3.9+）：防御性读回 + 环形截断
  if (Array.isArray(raw.errorEvents)) {
    const events = [];
    for (const e of raw.errorEvents) {
      if (!e || typeof e !== "object") continue;
      const at = num(e.at);
      if (at <= 0) continue;
      events.push({
        at,
        sessionId: typeof e.sessionId === "string" ? e.sessionId.slice(0, 128) : null,
        provider: typeof e.provider === "string" ? e.provider.slice(0, 64) : null,
        model: typeof e.model === "string" ? e.model.slice(0, 128) : null,
        sourceKind: typeof e.sourceKind === "string" ? e.sourceKind : null,
        kind: typeof e.kind === "string" ? e.kind : "unknown",
        code: e.code !== void 0 && e.code !== null ? String(e.code).slice(0, 64) : null,
        status: Number.isInteger(e.status) && e.status >= 100 && e.status <= 599 ? e.status : null,
        requestId: typeof e.requestId === "string" ? e.requestId.slice(0, 128) : null,
        message: String(e.message ?? "").slice(0, 500) || "(empty)"
      });
    }
    events.sort((a, b) => a.at - b.at);
    if (events.length > ERROR_EVENT_CAP) events.splice(0, events.length - ERROR_EVENT_CAP);
    stats.errorEvents = events;
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