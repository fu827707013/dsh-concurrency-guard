/**
 * 会话作用域解析：把请求的 sessionId 归并到顶层会话（rootId）。
 *
 * 为什么需要：子代理/派生代理是独立的 session（裸 UUID），其请求的
 * sessionId 不是用户视角的"对话"。用户说"限对话 A"，指的是顶层会话——
 * 主循环 + 全部子代理 + 压缩/标题都要受同一把会话锁。
 *
 * 实现：经 ctx.sessions.get(id).header.parentSession 向上走父链（同步 Map
 * 查询），缓存 60s。ctx.sessions 服务缺失/查询抛错一律降级返回原始 id——
 * 与插件"零硬依赖"哲学一致，绝不让解析失败影响主链路。
 */
const CACHE_TTL_MS = 60_000;

/** knownSessions LRU 上限（标题刷新候选集；防止无限增长） */
export const KNOWN_SESSION_CAP = 300;

/**
 * 记录一个"见过"的会话 id（LRU）——标题后台刷新的候选集。
 * 会话可能处于请求间隙（registry 空），但只要有历史请求就该能刷到标题，
 * 因此单独维护已知会话集合，而不是依赖"此刻在途"。
 */
export function rememberSession(state, sessionId) {
  if (!sessionId) return;
  state.knownSessions.delete(sessionId); // 刷新 LRU 顺序
  state.knownSessions.set(sessionId, Date.now());
  if (state.knownSessions.size > KNOWN_SESSION_CAP) {
    const oldest = state.knownSessions.keys().next().value;
    if (oldest !== void 0) state.knownSessions.delete(oldest);
  }
}

export function createScopeResolver(ctx) {
  /** null=未探测；false=服务不可用；对象=服务句柄 */
  let sessionsSvc = null;
  const cache = new Map(); // sessionId -> { rootId, ts }

  const tryGetSessions = () => {
    if (sessionsSvc === null) {
      try {
        sessionsSvc = ctx.get("sessions") ?? false;
      } catch {
        sessionsSvc = false;
      }
    }
    return sessionsSvc;
  };

  /**
   * 解析一个 sessionId 的顶层根会话 id。
   * @param {string} sessionId - 原始会话 id（非空）。
   * @returns {string} 根会话 id；解析不到父链时就是它自己。
   */
  const resolve = (sessionId) => {
    if (!sessionId) return sessionId;
    const hit = cache.get(sessionId);
    if (hit && Date.now() - hit.ts < CACHE_TTL_MS) return hit.rootId;

    let root = sessionId;
    let cur = sessionId;
    const svc = tryGetSessions();
    if (svc) {
      const seen = new Set();
      try {
        while (cur) {
          if (seen.has(cur)) break;
          seen.add(cur);
          const sess = svc.get(cur);
          const parent = sess?.header?.parentSession;
          if (typeof parent === "string" && parent !== "" && !seen.has(parent)) {
            root = parent;
            cur = parent;
          } else break;
        }
      } catch {
        // 降级：保持 root = sessionId
      }
    }
    cache.set(sessionId, { rootId: root, ts: Date.now() });
    return root;
  };

  const reset = () => cache.clear();

  return { resolve, reset };
}
