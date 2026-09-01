/**
 * 请求记录生命周期 + 快照组装。
 *
 * 一条记录的生命周期：waterfall 监听时创建（phase="waiting"）→
 * acquire 放行后（phase="streaming"）→ 流结束/取消/错误 → finish()（phase="ended"）。
 * finish() 是幂等收尾：归还并发位、更新计数与历史、触发状态落盘。
 */
import { randomUUID } from "node:crypto";
import { releaseSlot, releaseSessionSlot, gcSessionSem } from "./gate.js";
import { classifyErrorKind, recordFinish } from "./stats.js";

/** 新建一条在途记录（不进入任何结构，由调用方 registry.set）。
 *  @param {object} options - llm/stream 请求包。
 *  @param {string} kind - classifyKind() 分类结果（main/subagent/plugin/compaction/session-title）。 */
export function newRecord(options, kind) {
  return {
    id: randomUUID(),
    provider: options.provider ?? "?",
    model: options.model ?? "?",
    sessionId: options.sessionId ?? null,
    purpose: options.purpose ?? null,
    kind: kind ?? "unknown",
    createdAt: Date.now(),
    phase: "waiting",          // waiting | streaming | ended
    ownsSlot: false,           // 是否持有全局并发位（决定 finish 是否 releaseSlot）
    holdsSessionSlot: false,   // 是否持有会话门并发位
    sessionGate: null,         // 持有的会话门 gateKey
    gateStage: null,           // 当前等待/持有的门：null | "session" | "global"
    gateKey: null,             // 归并后的会话作用域键（根会话 id 或原始 id）
    rootId: null,              // 顶层会话 id（与 gateKey 相同；解析不到父链时=原始 id）
    exempt: false,             // 辅助请求豁免会话门
    finished: false,
    status: "waiting",         // waiting | ok | cancelled | error
    waitedMs: 0,
    queuedAt: 0,
    dispatchedAt: 0,
    lastActivityAt: Date.now(),  // 最后一块 chunk 时间（弃流判定用）
    endedAt: 0,
    error: void 0,
    errorCode: void 0   // finish/error chunk 的 failure.code（TRANSPORT 等），供异常明细展示
  };
}

/** 时间维度清理：按 historyTtlMs 逐出最老记录（0=关闭）。返回是否清理了记录。 */
export function pruneHistoryByTtl(state) {
  const ttl = state.cfg.historyTtlMs;
  if (!ttl || ttl <= 0 || state.history.length === 0) return false;
  const cutoff = Date.now() - ttl;
  let removed = 0;
  while (state.history.length > 0 && state.history[0].createdMs < cutoff) {
    state.history.shift();
    removed += 1;
  }
  return removed > 0;
}

function pushHistory(state, rec) {
  const cap = state.cfg.history;
  if (cap <= 0) return;
  state.history.push({
    id: rec.id,
    provider: rec.provider,
    model: rec.model,
    purpose: rec.purpose,
    sessionId: rec.sessionId,
    gateKey: rec.gateKey ?? null,
    gates: rec.holdsSessionSlot || rec.sessionGate ? "session+global" : "global",
    kind: rec.kind,
    status: rec.status,
    createdMs: rec.createdAt,
    waitedMs: rec.waitedMs ?? 0,
    durationMs: rec.endedAt - (rec.dispatchedAt ?? rec.createdAt),
    error: rec.error ?? null
  });
  // 硬上限截断 + 时间 TTL 清理
  if (state.history.length > cap) state.history.splice(0, state.history.length - cap);
  pruneHistoryByTtl(state);
}

/**
 * 幂等收尾一条记录。无论从哪条路径完成（正常/取消/错误/超时清扫/消费端弃流），
 * 都只执行一次：归还并发位 → 计数 → 历史 → 触发落盘。
 * @param {object} state
 * @param {object} rec
 * @param {"ok"|"cancelled"|"error"} status
 * @param {string} [error] - 附加说明（取消原因/错误信息）。
 */
export function finish(state, rec, status, error) {
  if (rec.finished) return;
  rec.finished = true;
  rec.endedAt = Date.now();
  rec.phase = "ended";
  rec.status = status;
  if (error !== void 0) rec.error = error;
  state.registry.delete(rec.id);

  // 先还会话门位（窄资源优先），再还全局门位；两者都做位子转移
  if (rec.holdsSessionSlot) {
    rec.holdsSessionSlot = false;
    const sem = state.sessionSems.get(rec.sessionGate);
    if (sem) {
      releaseSessionSlot(sem);
      gcSessionSem(state, rec.sessionGate);
    }
  }
  if (rec.ownsSlot) {
    rec.ownsSlot = false;
    releaseSlot(state); // 位子转移给队首或直接释放
  }

  if (status === "cancelled") state.cancelled += 1;
  else if (status === "interrupted") state.interrupted += 1;
  else state.completed += 1;
  if (status !== "cancelled") pushHistory(state, rec);

  // 持久化统计：状态/来源/异常分类 记入今日与总计（跨重启连续累计）；
  // error 原文 + 错误码 + 会话/模型/供应商上下文 同时喂入 errorDetails 异常明细聚合（按信息聚类计数）。
  recordFinish(
    state.stats,
    status,
    status === "error" ? classifyErrorKind(error) : void 0,
    rec.kind,
    Date.now(),
    status === "error"
      ? {
          msg: error ?? "",
          code: rec.errorCode ?? null,
          sessionId: rec.sessionId ?? null,
          provider: rec.provider,
          model: rec.model,
          sourceKind: rec.kind ?? null,
          status: rec.failureStatus ?? null,
          requestId: rec.failureRequestId ?? null,
          retryAfterMs: rec.failureRetryAfterMs ?? null
        }
      : void 0,
    state.cfg?.dayRetention ?? 0
  );

  // 状态变更钩子（由 index.js 挂 persist.schedule）：完成/取消/错误后触发落盘
  state.onChange?.();
}

/** 在途记录明细（按创建序），供快照/面板使用。 */
export function activeRecords(state) {
  return [...state.registry.values()]
    .sort((a, b) => a.createdAt - b.createdAt)
    .map((rec) => ({
      id: rec.id,
      provider: rec.provider,
      model: rec.model,
      sessionId: rec.sessionId,
      gateKey: rec.gateKey ?? null,
      purpose: rec.purpose,
      kind: rec.kind,
      phase: rec.phase,
      gateStage: rec.gateStage ?? null,
      createdAt: rec.createdAt,
      queuedAt: rec.queuedAt ?? null,
      dispatchedAt: rec.dispatchedAt ?? null,
      waitedMs: rec.waitedMs || 0,
      ageMs: Date.now() - rec.createdAt
    }));
}

/** 按来源种类聚合在途请求（main/subagent/plugin/compaction/session-title）。 */
export function byKind(state) {
  const map = new Map();
  for (const rec of state.registry.values()) {
    const row = map.get(rec.kind) ?? { kind: rec.kind, active: 0, waiting: 0 };
    row[rec.phase === "streaming" ? "active" : "waiting"] += 1;
    map.set(rec.kind, row);
  }
  return [...map.values()].sort((a, b) => a.kind.localeCompare(b.kind));
}

/** 按 provider/model 聚合在途请求。 */
export function byModel(state) {
  const map = new Map();
  for (const rec of state.registry.values()) {
    const key = `${rec.provider}/${rec.model}`;
    const row = map.get(key) ?? { provider: rec.provider, model: rec.model, active: 0, waiting: 0 };
    row[rec.phase === "streaming" ? "active" : "waiting"] += 1;
    map.set(key, row);
  }
  return [...map.values()].sort(
    (a, b) => a.provider.localeCompare(b.provider) || a.model.localeCompare(b.model)
  );
}

/** 按 provider 聚合在途请求（轻量补充维度）。 */
export function byProvider(state) {
  const map = new Map();
  for (const rec of state.registry.values()) {
    const row = map.get(rec.provider) ?? { provider: rec.provider, active: 0, waiting: 0 };
    row[rec.phase === "streaming" ? "active" : "waiting"] += 1;
    map.set(rec.provider, row);
  }
  return [...map.values()].sort((a, b) => a.provider.localeCompare(b.provider));
}

/** 会话活跃聚合窗口（recentCount 统计近 N ms 内完成的请求数）。 */
const SESSION_WINDOW_MS = 60_000;

/**
 * 按会话（sessionId）聚合活跃度：在途/排队/最近开始/最近结束/近 60s 完成数。
 * 用于回答"这个会话还在不在推进"——模型请求间隙（工具执行等）在途为 0，
 * 但最近活动时间会持续更新，不会误判为闲置。
 * 数据源：注册表（在途/排队）+ 历史（已完成）。无 sessionId 的请求归入 null。
 */
export function bySession(state) {
  const now = Date.now();
  const map = new Map();
  const rowOf = (sid) => {
    let r = map.get(sid);
    if (!r) {
      r = { sessionId: sid, active: 0, waiting: 0, lastStartMs: 0, lastEndMs: 0, recentCount: 0, kinds: new Map() };
      map.set(sid, r);
    }
    return r;
  };
  for (const rec of state.registry.values()) {
    const r = rowOf(rec.sessionId);
    if (rec.phase === "streaming") r.active += 1;
    else if (rec.phase === "waiting") r.waiting += 1;
    if (rec.createdAt > r.lastStartMs) r.lastStartMs = rec.createdAt;
    if (rec.kind) r.kinds.set(rec.kind, (r.kinds.get(rec.kind) ?? 0) + 1);
  }
  for (const h of state.history) {
    const r = rowOf(h.sessionId);
    const start = h.createdMs;
    const end = start + (h.durationMs ?? 0);
    if (start > r.lastStartMs) r.lastStartMs = start;
    if (end > r.lastEndMs) r.lastEndMs = end;
    if (start >= now - SESSION_WINDOW_MS) r.recentCount += 1;
    if (h.kind) r.kinds.set(h.kind, (r.kinds.get(h.kind) ?? 0) + 1);
  }
  return [...map.values()]
    .map((r) => {
      let kind = null;
      let max = 0;
      for (const [k, n] of r.kinds) if (n > max) { max = n; kind = k; }
      return { sessionId: r.sessionId, kind, active: r.active, waiting: r.waiting, lastStartMs: r.lastStartMs, lastEndMs: r.lastEndMs, recentCount: r.recentCount };
    })
    .sort(
      (a, b) => (b.active - a.active) || (b.lastStartMs - a.lastStartMs) || String(a.sessionId ?? "").localeCompare(String(b.sessionId ?? ""))
    );
}

/**
 * 会话级并发视图（v1.5.0+）：按 gateKey（根会话作用域）聚合的"在线会话"列表。
 *
 * 数据源：
 *   - 注册表（在途/排队：精确活跃度 + 各自等待的门）
 *   - 历史窗口（最近 onlineWindowMs 内的活动：请求间隙不误判闲置）
 *   - 显式限额（即使当前闲置也列出，便于清除过期条目）
 *   - 会话门信号量（sessionWaiting/gateHeld/failOpen 细分）
 *   - 标题缓存（ctx.sessionQuery 后台刷新，缺失为 null）
 *
 * 与 bySession（原始 sessionId 聚合，向后兼容）并存：本视图回答
 * "哪些对话在线、各占多少并发、限额多少、怎么调"。
 */
export function sessionsView(state) {
  const now = Date.now();
  const windowMs = state.cfg.onlineWindowMs;
  const resolver = state.resolver;
  const map = new Map();

  const rowOf = (key) => {
    let r = map.get(key);
    if (!r) {
      const meta = state.sessionMeta?.get(key);
      r = {
        gateKey: key,
        sessionId: key,            // 兼容字段：主键即 gateKey
        title: meta?.title ?? null,
        cwd: meta?.cwd ?? null,
        kind: null,
        active: 0,
        waiting: 0,
        sessionWaiting: 0,         // 等会话门（该会话自己的队列）
        globalWaiting: 0,          // 已过会话门、等全局门
        capped: state.sessionLimits.has(key),
        cap: state.sessionLimits.get(key)?.cap ?? null,
        paused: state.sessionLimits.get(key)?.cap === 0,
        online: false,
        lastActivityMs: 0,
        gateHeld: 0,
        failOpen: 0,
        recentCount: 0
      };
      map.set(key, r);
    }
    return r;
  };

  for (const rec of state.registry.values()) {
    const key = rec.gateKey ?? "(none)";
    const r = rowOf(key);
    if (rec.phase === "streaming") r.active += 1;
    else if (rec.phase === "waiting") {
      r.waiting += 1;
      if (rec.gateStage === "session") r.sessionWaiting += 1;
      else if (rec.gateStage === "global") r.globalWaiting += 1;
      else r.globalWaiting += 1;   // 无会话门时等全局门
    }
    const act = rec.lastActivityAt ?? rec.createdAt;
    if (act > r.lastActivityMs) r.lastActivityMs = act;
    r.online = true;
    if (rec.kind && rec.kind !== "compaction" && rec.kind !== "session-title") {
      if (!r.kind) r.kind = rec.kind;
    } else if (!r.kind) {
      r.kind = rec.kind;
    }
  }

  const hWindow = now - windowMs;
  for (const h of state.history) {
    const end = h.createdMs + (h.durationMs ?? 0);
    if (end < hWindow) continue;
    const key = h.gateKey ?? (h.sessionId ? (resolver ? resolver.resolve(h.sessionId) : h.sessionId) : "(none)");
    const r = rowOf(key);
    if (end > r.lastActivityMs) r.lastActivityMs = end;
    if (h.createdMs >= hWindow) r.recentCount += 1;
    if (h.kind && h.kind !== "compaction" && h.kind !== "session-title" && !r.kind) r.kind = h.kind;
  }

  for (const [key, sem] of state.sessionSems) {
    const r = rowOf(key);
    r.sessionWaiting = Math.max(r.sessionWaiting, sem.waiters.length);
    r.gateHeld = sem.gateHeld;
    r.failOpen = sem.failOpen;
  }

  // 显式限额的会话即使当前不在线也列出（cap 管理入口）
  for (const [key, lim] of state.sessionLimits) {
    const r = rowOf(key);
    r.capped = true;
    r.cap = lim.cap;
    r.paused = lim.cap === 0;
  }

  return [...map.values()]
    .map((r) => {
      const meta = state.sessionMeta?.get(r.gateKey);
      return {
        ...r,
        online: r.online || r.lastActivityMs >= hWindow || r.capped,
        title: meta?.title ?? r.title ?? null,
        cwd: meta?.cwd ?? r.cwd ?? null
      };
    })
    .sort(
      (a, b) =>
        (b.active - a.active) ||
        (b.online - a.online) ||
        (b.lastActivityMs - a.lastActivityMs) ||
        String(a.gateKey).localeCompare(String(b.gateKey))
    );
}

/**
 * 组装一份完整快照（HTTP/文件/工具共用）。
 * @param {object} state
 * @param {boolean} full - 是否附带最近完成历史。
 */
export function snapshot(state, full = false) {
  return {
    pid: state.pid,
    startedAt: state.startedAt,
    uptimeSec: Math.round((Date.now() - state.startedAt) / 1000),
    updatedAt: Date.now(),
    config: { ...state.cfg },
    gauges: {
      active: state.active,
      waiting: state.waiting,
      queueDepth: state.waiters.length,
      peakActive: state.peakActive,
      peakWaiting: state.peakWaiting
    },
    counters: {
      started: state.started,
      completed: state.completed,
      cancelled: state.cancelled,
      interrupted: state.interrupted,
      gateHeld: state.gateHeld,
      failOpen: state.failOpen,
      sessionGateHeld: state.sessionGateHeld,
      sessionFailOpen: state.sessionFailOpen,
      warnHits: state.warnHits
    },
    stats: state.stats,
    byModel: byModel(state),
    byProvider: byProvider(state),
    byKind: byKind(state),
    bySession: bySession(state),
    sessions: sessionsView(state),
    sessionLimits: [...state.sessionLimits.entries()].map(([scopeKey, v]) => ({
      scopeKey,
      cap: v.cap,
      source: v.source ?? null,
      note: v.note ?? null,
      updatedAt: v.updatedAt,
      pausedFrom: v.pausedFrom ?? null
    })),
    activeRequests: activeRecords(state),
    ...(full ? { recent: [...state.history].reverse() } : {})
  };
}