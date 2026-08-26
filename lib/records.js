/**
 * 请求记录生命周期 + 快照组装。
 *
 * 一条记录的生命周期：waterfall 监听时创建（phase="waiting"）→
 * acquire 放行后（phase="streaming"）→ 流结束/取消/错误 → finish()（phase="ended"）。
 * finish() 是幂等收尾：归还并发位、更新计数与历史、触发状态落盘。
 */
import { randomUUID } from "node:crypto";
import { releaseSlot } from "./gate.js";

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
    byModel: byModel(state),
    byProvider: byProvider(state),
    byKind: byKind(state),
    activeRequests: activeRecords(state),
    ...(full ? { recent: [...state.history].reverse() } : {})
  };
}