/**
 * 请求记录生命周期 + 快照组装。
 *
 * 一条记录的生命周期：waterfall 监听时创建（phase="waiting"）→
 * acquire 放行后（phase="streaming"）→ 流结束/取消/错误 → finish()（phase="ended"）。
 * finish() 是幂等收尾：归还并发位、更新计数与历史、触发状态落盘。
 */
import { randomUUID } from "node:crypto";
import { releaseSlot } from "./gate.js";
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
    ownsSlot: false,           // 是否持有并发位（决定 finish 是否 releaseSlot）
    finished: false,
    status: "waiting",         // waiting | ok | cancelled | error
    waitedMs: 0,
    queuedAt: 0,
    dispatchedAt: 0,
    endedAt: 0,
    error: void 0
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
    kind: rec.kind,
    status: rec.status,
    createdMs: rec.createdAt,
    waitedMs: rec.waitedMs ?? 0,
    durationMs: rec.endedAt - (rec.dispatchedAt ?? rec.createdAt),
    error: rec.error
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

  if (rec.ownsSlot) {
    rec.ownsSlot = false;
    releaseSlot(state); // 位子转移给队首或直接释放
  }

  if (status === "cancelled") state.cancelled += 1;
  else state.completed += 1;
  if (status !== "cancelled") pushHistory(state, rec);

  // 持久化统计：状态/来源/异常分类 记入今日与总计（跨重启连续累计）
  recordFinish(state.stats, status, status === "error" ? classifyErrorKind(error) : void 0, rec.kind);

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
      purpose: rec.purpose,
      kind: rec.kind,
      phase: rec.phase,
      createdAt: rec.createdAt,
      queuedAt: rec.queuedAt || void 0,
      dispatchedAt: rec.dispatchedAt || void 0,
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
      gateHeld: state.gateHeld,
      failOpen: state.failOpen,
      warnHits: state.warnHits
    },
    stats: state.stats,
    byModel: byModel(state),
    byProvider: byProvider(state),
    byKind: byKind(state),
    bySession: bySession(state),
    activeRequests: activeRecords(state),
    ...(full ? { recent: [...state.history].reverse() } : {})
  };
}