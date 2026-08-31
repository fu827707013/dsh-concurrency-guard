/**
 * 并发门闩：FIFO 信号量。
 *
 * 语义：
 * - 并发未满 → 立即放行（active++）。
 * - 并发已满（queue 模式）→ 请求入 FIFO 队列等待：
 *     * 有请求结束 → 位子**转移**给队首（active 不变，队首经 settle 出队）；
 *     * 等待期间 AbortSignal 触发 → 立即出队并取消（不占位）；
 *     * 排队超过 maxQueueWaitMs → **fail-open** 强制放行（active++，可短暂超限），
 *       宁可瞬时超限也不让请求永久卡死。
 * - monitor 模式 → 无视上限，只计数。
 *
 * 关键细节：位子转移必须走等待者的 settle()（清定时器/abort 监听并归还 waiting
 * 计数），否则超时定时器会在转移后二次触发（fail-open 误增 + active 虚高）。
 */
import { requestLabel } from "./config.js";

function updatePeak(state) {
  if (state.active > state.peakActive) state.peakActive = state.active;
  if (state.waiting > state.peakWaiting) state.peakWaiting = state.waiting;
}

/**
 * 尝试获取一个并发位。立即路径同步返回 outcome；排队路径返回 Promise。
 * @param {object} state - createState() 的共享状态。
 * @param {import("node:events").EventEmitter} logger - cordis logger（info/warn/debug/error）。
 * @param {object} rec - 请求记录（newRecord 产物）。
 * @param {AbortSignal|undefined} signal - 请求的取消信号（可无）。
 * @returns {{ok:true,source:"immediate"|"slot"|"timeout",waitedMs:number} | {ok:false,reason:"aborted",waitedMs:number}}
 */
export function acquire(state, logger, rec, signal) {
  const cfg = state.cfg;

  // monitor 模式：只计数，不拦
  if (cfg.mode !== "queue") {
    state.active += 1;
    updatePeak(state);
    return { ok: true, source: "immediate", waitedMs: 0 };
  }

  // 有富余位：立即放行
  if (state.active < cfg.maxConcurrency) {
    state.active += 1;
    updatePeak(state);
    return { ok: true, source: "immediate", waitedMs: 0 };
  }

  // 满员：入队（幂等日志：每 30s 至多一条）
  state.waiting += 1;
  state.gateHeld += 1;
  state.stats.today.gateHeld += 1;
  state.stats.totals.gateHeld += 1;
  updatePeak(state);
  const now = Date.now();
  if (now - state.lastQueueLogAt > 30_000) {
    state.lastQueueLogAt = now;
    logger.info(
      `[concurrency-guard] 并发已达上限 ${cfg.maxConcurrency}，请求开始排队（队列 ${state.waiting}）`
    );
  }

  const queuedAt = Date.now();
  rec.queuedAt = queuedAt;

  return new Promise((resolve) => {
    const entry = {
      rec,
      signal,
      queuedAt,
      settled: false,
      timer: null,
      resolve
    };

    /** 唯一出队通道：清定时器/监听、从队列移除、归还 waiting、resolve。 */
    const settle = (outcome) => {
      if (entry.settled) return;
      entry.settled = true;
      if (entry.timer !== null) clearTimeout(entry.timer);
      if (signal) signal.removeEventListener("abort", onAbort);
      const at = state.waiters.indexOf(entry);
      if (at !== -1) state.waiters.splice(at, 1);
      state.waiting = Math.max(0, state.waiting - 1);
      resolve(outcome);
    };
    entry.settle = settle;

    const onAbort = () => {
      if (entry.settled) return;
      if (signal?.aborted) {
        settle({ ok: false, reason: "aborted", waitedMs: Date.now() - queuedAt });
      }
    };

    // fail-open：排队超时强制放行（借位 → active++，可能短暂超限）
    entry.timer =
      cfg.maxQueueWaitMs > 0
        ? setTimeout(() => {
            if (entry.settled) return;
            state.failOpen += 1;
            state.stats.today.failOpen += 1;
            state.stats.totals.failOpen += 1;
            state.active += 1;
            updatePeak(state);
            settle({ ok: true, source: "timeout", waitedMs: Date.now() - queuedAt });
          }, cfg.maxQueueWaitMs)
        : null;

    state.waiters.push(entry);

    if (signal) {
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
}

/**
 * 释放一个并发位：优先把位子转移给队首（通过其 settle，见模块注释），
 * 队列空才真正 active--。
 * @param {object} state
 */
export function releaseSlot(state) {
  const next = state.waiters.shift();
  if (next) {
    next.settle({ ok: true, source: "slot", waitedMs: Math.max(0, Date.now() - next.queuedAt) });
  } else {
    state.active = Math.max(0, state.active - 1);
  }
}

/**
 * 供日志/调试使用：打印当前门闩水位（不改变状态）。
 * @param {object} state
 * @param {import("node:events").EventEmitter} logger
 */
export function logWatermark(state, logger, rec) {
  const cfg = state.cfg;
  const active = state.active;
  if (active >= cfg.warnAt && Date.now() - state.lastWarnAt > 5_000) {
    state.lastWarnAt = Date.now();
    state.warnHits += 1;
    state.stats.today.warnHits += 1;
    state.stats.totals.warnHits += 1;
    const suffix = cfg.mode === "monitor"
      ? "（monitor 模式：仅记录，未限制）"
      : `（上限 ${cfg.maxConcurrency}）`;
    logger.warn(`[concurrency-guard] 当前活跃并发 ${active} ≥ ${cfg.warnAt}${suffix}`);
  }
  logger.debug(`[concurrency-guard] → ${requestLabel(rec)} 开始（活跃 ${state.active}）`);
}

// ================================================================
// 会话门（v1.5.0+）：与全局门同构的 FIFO 信号量，按 gateKey 独立计数。
// 语义与 acquire()/releaseSlot() 完全一致，只是计数挂到会话信号量上：
//   cap=0 视为"暂停该会话"（全部排队，fail-open 超时仍兜底）；
//   降低 cap 不打断在途，只影响后续准入；
//   clearSessionLimit 时把在途等待者全部放行（settleSessionWaiters）。
// ================================================================

/**
 * 新建一个会话门信号量。
 * @param {string} key - gateKey（解析后的根会话 id 或原始 sessionId）。
 * @param {number} cap - 会话并发上限（0 = 暂停）。
 */
export function createSessionSem(key, cap) {
  return {
    key,
    cap: Math.max(0, Math.floor(cap)),
    active: 0,
    waiters: [],
    peakActive: 0,
    peakWaiting: 0,
    gateHeld: 0,
    failOpen: 0,
    lastQueueLogAt: 0,
    disabled: false      // clearSessionLimit 置位；排空后 GC
  };
}

/** 取会话门信号量（不存在则按限额创建并注册）。 */
export function ensureSessionSem(state, gateKey) {
  let sem = state.sessionSems.get(gateKey);
  if (!sem) {
    const cap = state.sessionLimits.get(gateKey)?.cap ?? 1;
    sem = createSessionSem(gateKey, cap);
    state.sessionSems.set(gateKey, sem);
  }
  return sem;
}

/**
 * 尝试获取该会话的一个并发位。cap=0（暂停）或满员 → 进该会话自己的 FIFO 队列。
 * @param {object} sem - createSessionSem 产物。
 * @param {object} state - 共享状态（计数/统计就地更新）。
 * @param {import("node:events").EventEmitter} logger
 * @param {object} rec - 请求记录。
 * @param {AbortSignal|undefined} signal
 * @returns {Promise<{ok:true,source:"immediate"|"slot"|"timeout",waitedMs:number}|{ok:false,reason:"aborted",waitedMs:number}>}
 */
export function acquireSessionSlot(sem, state, logger, rec, signal) {
  const cfg = state.cfg;

  if (sem.cap > 0 && sem.active < sem.cap) {
    sem.active += 1;
    if (sem.active > sem.peakActive) sem.peakActive = sem.active;
    return { ok: true, source: "immediate", waitedMs: 0 };
  }

  sem.gateHeld += 1;
  state.sessionGateHeld += 1;
  state.stats.today.sessionGateHeld += 1;
  state.stats.totals.sessionGateHeld += 1;
  if (sem.waiters.length > sem.peakWaiting) sem.peakWaiting = sem.waiters.length + 1;

  const now = Date.now();
  const capLabel = sem.cap === 0 ? "已暂停" : `上限 ${sem.cap}`;
  if (now - sem.lastQueueLogAt > 30_000) {
    sem.lastQueueLogAt = now;
    logger.info(
      `[concurrency-guard] 会话 ${sem.key} ${capLabel}，请求开始排队（会话队列 ${sem.waiters.length + 1}）: ${requestLabel(rec)}`
    );
  }

  const queuedAt = Date.now();
  rec.queuedAt = queuedAt;

  return new Promise((resolve) => {
    const entry = {
      rec,
      signal,
      queuedAt,
      settled: false,
      timer: null,
      resolve
    };

    const settle = (outcome) => {
      if (entry.settled) return;
      entry.settled = true;
      if (entry.timer !== null) clearTimeout(entry.timer);
      if (signal) signal.removeEventListener("abort", onAbort);
      const at = sem.waiters.indexOf(entry);
      if (at !== -1) sem.waiters.splice(at, 1);
      resolve(outcome);
    };
    entry.settle = settle;

    const onAbort = () => {
      if (entry.settled) return;
      if (signal?.aborted) {
        settle({ ok: false, reason: "aborted", waitedMs: Date.now() - queuedAt });
      }
    };

    // fail-open：会话门排队超时强制放行（语义同全局门）
    entry.timer =
      cfg.maxQueueWaitMs > 0
        ? setTimeout(() => {
            if (entry.settled) return;
            sem.failOpen += 1;
            state.sessionFailOpen += 1;
            state.stats.today.sessionFailOpen += 1;
            state.stats.totals.sessionFailOpen += 1;
            sem.active += 1;
            if (sem.active > sem.peakActive) sem.peakActive = sem.active;
            logger.warn(
              `[concurrency-guard] 会话 ${sem.key} 排队超时（${cfg.maxQueueWaitMs}ms）强制放行（fail-open）: ${requestLabel(rec)}`
            );
            settle({ ok: true, source: "timeout", waitedMs: Date.now() - queuedAt });
          }, cfg.maxQueueWaitMs)
        : null;

    sem.waiters.push(entry);

    if (signal) {
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
}

/**
 * 释放一个会话位：优先转移给该会话队首，队列空才 active--。
 * @param {object} sem
 */
export function releaseSessionSlot(sem) {
  const next = sem.waiters.shift();
  if (next) {
    next.settle({ ok: true, source: "slot", waitedMs: Math.max(0, Date.now() - next.queuedAt) });
  } else {
    sem.active = Math.max(0, sem.active - 1);
  }
}

/**
 * 清除限额时把所有在该会话门排队的请求放行（不占会话位，直接进入全局门）。
 * @param {object} sem
 */
export function settleSessionWaiters(sem) {
  const waiters = [...sem.waiters];
  sem.waiters.length = 0;
  for (const entry of waiters) {
    entry.settle({ ok: true, source: "slot", waitedMs: Math.max(0, Date.now() - entry.queuedAt) });
  }
}

/**
 * 惰性回收已禁用且排空的会话门（clearSessionLimit 或限额 TTL 过期后调用）。
 * @param {object} state
 * @param {string} gateKey
 */
export function gcSessionSem(state, gateKey) {
  const sem = state.sessionSems.get(gateKey);
  if (sem && sem.disabled && sem.active === 0 && sem.waiters.length === 0) {
    state.sessionSems.delete(gateKey);
  }
}