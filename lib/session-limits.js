/**
 * 会话限额的增删改（唯一写入口）。
 *
 * API 服务、HTTP 端点、工具、TTL 清扫都经这里，保证：
 *   - 限额 Map 与会话门信号量的 cap 始终同步（实时生效，不重启）；
 *   - cap=0 定义为"暂停该会话"（pausedFrom 记录原值，供恢复）；
 *   - 清除限额时把该会话门队列中的等待者全部放行（转移到全局门），
 *     已持有会话位的在途请求由 finish() 幂等归还；
 *   - 每次变更触发 state.onChange() 落盘（重启保留）。
 */
import { createSessionSem, gcSessionSem, settleSessionWaiters } from "./gate.js";
import { requestLabel } from "./config.js";

/** cap 合法范围。0 特殊含义=暂停；>64 视为配置错误拒绝。 */
export const SESSION_CAP_MIN = 0;
export const SESSION_CAP_MAX = 64;

function normCap(value) {
  const n = Number(value);
  if (!Number.isInteger(n) || n < SESSION_CAP_MIN || n > SESSION_CAP_MAX) return null;
  return n;
}

/**
 * 设置（或覆盖）一个会话限额；cap=0 即暂停。
 * @param {object} state
 * @param {import("node:events").EventEmitter} logger
 * @param {string} sessionId - 限额键（面板/API 传入的 gateKey：根会话 id 或原始 id）。
 * @param {number} cap - 0..64；0=暂停。
 * @param {{source?: string, note?: string|null}} [opts]
 * @returns {{ok:true, cap:number}|{ok:false, error:string}}
 */
export function applySessionLimit(state, logger, sessionId, cap, opts = {}) {
  if (typeof sessionId !== "string" || sessionId.trim() === "") {
    return { ok: false, error: "sessionId 必须是非空字符串" };
  }
  const key = sessionId.trim();
  const capN = normCap(cap);
  if (capN === null) {
    return { ok: false, error: `cap 必须是 ${SESSION_CAP_MIN}..${SESSION_CAP_MAX} 的整数（0 = 暂停）` };
  }

  const prev = state.sessionLimits.get(key);
  const lim = {
    cap: capN,
    source: typeof opts.source === "string" && opts.source !== "" ? opts.source : (prev?.source ?? "service"),
    note: opts.note !== void 0 && opts.note !== null ? String(opts.note).slice(0, 200) : (prev?.note ?? null),
    updatedAt: Date.now(),
    pausedFrom: capN === 0
      ? (prev && prev.cap !== 0 ? prev.cap : (prev?.pausedFrom ?? null))
      : null
  };
  state.sessionLimits.set(key, lim);

  const sem = state.sessionSems.get(key) ?? createSessionSem(key, capN);
  sem.cap = capN;
  state.sessionSems.set(key, sem);

  if (capN === 0 && !(prev?.cap === 0)) {
    logger.warn(
      `[concurrency-guard] 会话 ${key} 已暂停（cap=0）：后续请求全部排队，排队超时仍 fail-open 兜底`
    );
  } else if (prev && prev.cap !== capN && capN > 0 && capN < prev.cap) {
    logger.info(
      `[concurrency-guard] 会话 ${key} 并发上限降低 ${prev.cap}→${capN}（不影响在途请求，只影响后续准入）`
    );
  } else if (!prev || prev.cap !== capN) {
    logger.info(`[concurrency-guard] 会话 ${key} 并发上限设为 ${capN}（${lim.source}）`);
  }
  state.onChange?.();
  return { ok: true, cap: capN };
}

/**
 * 清除一个会话限额（回退到只走全局门）。
 * @returns {{ok:true, removed:boolean}|{ok:false, error:string}}
 */
export function removeSessionLimit(state, logger, sessionId) {
  if (typeof sessionId !== "string" || sessionId.trim() === "") {
    return { ok: false, error: "sessionId 必须是非空字符串" };
  }
  const key = sessionId.trim();
  const had = state.sessionLimits.delete(key);
  const sem = state.sessionSems.get(key);
  if (sem) {
    sem.disabled = true;
    const released = sem.waiters.length;
    if (released > 0) {
      settleSessionWaiters(sem);
      logger.info(`[concurrency-guard] 会话 ${key} 限额已清除，${released} 个排队请求放行（进入全局门）`);
    }
    gcSessionSem(state, key);
  }
  if (had) {
    logger.info(`[concurrency-guard] 会话 ${key} 限额已清除（回退全局门）`);
    state.onChange?.();
  }
  return { ok: true, removed: had };
}

/**
 * 恢复一个被暂停的会话：pausedFrom ?? 默认 5。
 * @returns {{ok:true, cap:number}|{ok:false, error:string}}
 */
export function resumeSessionLimit(state, logger, sessionId) {
  if (typeof sessionId !== "string" || sessionId.trim() === "") {
    return { ok: false, error: "sessionId 必须是非空字符串" };
  }
  const key = sessionId.trim();
  const lim = state.sessionLimits.get(key);
  if (!lim) return { ok: false, error: `会话 ${key} 没有显式限额，无需恢复` };
  const cap = lim.pausedFrom ?? 5;
  return applySessionLimit(state, logger, key, cap, { source: lim.source, note: lim.note });
}

/**
 * 限额 TTL 过期清扫（sweep 周期内调用）：超过 sessionLimitTtlDays 未再更新、
 * 且当前无在途/排队的会话，清除其限额。
 * @returns {string[]} 被清除的 gateKey 列表
 */
export function expireSessionLimits(state, logger) {
  const days = state.cfg.sessionLimitTtlDays;
  if (!days || days <= 0) return [];
  const cutoff = Date.now() - days * 86_400_000;
  const expired = [];
  for (const [key, lim] of state.sessionLimits) {
    if (lim.updatedAt >= cutoff) continue;
    const sem = state.sessionSems.get(key);
    if (sem && (sem.active > 0 || sem.waiters.length > 0)) continue;
    const busy = [...state.registry.values()].some((r) => r.gateKey === key);
    if (busy) continue;
    expired.push(key);
  }
  for (const key of expired) removeSessionLimit(state, logger, key);
  if (expired.length > 0) {
    logger.info(`[concurrency-guard] 会话限额 TTL 过期清理：${expired.join(", ")}`);
  }
  return expired;
}

export { requestLabel };
