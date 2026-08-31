/**
 * 对外"挂面"：ctx.concurrencyGuard 服务 + HTTP JSON 端点 + concurrency_status 工具。
 *
 * - 服务：status(full) 快照；configure(partial) 热改配置；reset() 清零计数/历史；
 *         setSessionLimit/clearSessionLimit/resumeSession 会话级并发控制（v1.5.0+）。
 * - HTTP：GET  /api/concurrency-guard/status（?full=1 带最近历史）
 *          POST /api/concurrency-guard/config（JSON body，如 {"mode":"monitor"}）
 *          POST /api/concurrency-guard/history（{action:"clear"|"prune"|"reset"}）
 *          POST /api/concurrency-guard/sessions（{action:"set"|"clear"|"pause"|"resume", sessionId, cap?}）
 * - 工具：concurrency_status（模型侧直接查看，isConcurrencySafe 不占用并发位）；
 *         concurrency_session_list / concurrency_session_set（会话级并发查看与热调）。
 *
 * webServer/tools 服务用 ctx.get 惰性获取 + internal/service 事件补挂，
 * 缺失时降级（日志告警），绝不让插件纤维失败。
 */
import { mergeConfig } from "./config.js";
import { pruneHistoryByTtl, snapshot } from "./records.js";
import { applySessionLimit, removeSessionLimit, resumeSessionLimit } from "./session-limits.js";

/** 创建可供其它插件调用的服务对象。 */
export function createService(state, logger) {
  return {
    status(full = false) {
      return snapshot(state, full === true);
    },
    configure(partial = {}) {
      const previous = state.cfg;
      state.cfg = mergeConfig(previous, partial);
      logger.info(
        `[concurrency-guard] 配置已更新: mode=${state.cfg.mode} max=${state.cfg.maxConcurrency} ` +
          `warnAt=${state.cfg.warnAt} maxQueueWaitMs=${state.cfg.maxQueueWaitMs}`
      );
      state.onChange?.(); // 配置变更也落盘
      return snapshot(state, false);
    },
    reset() {
      state.started = 0;
      state.completed = 0;
      state.cancelled = 0;
      state.gateHeld = 0;
      state.failOpen = 0;
      state.warnHits = 0;
      state.peakActive = 0;
      state.peakWaiting = 0;
      state.history = [];
      logger.info("[concurrency-guard] 计数与历史已清零");
      state.onChange?.();
      return snapshot(state, false);
    },
    /** 只清空「最近完成」历史（保留计数与在途记录）；返回清理条数。 */
    clearHistory() {
      const removed = state.history.length;
      state.history = [];
      logger.info(`[concurrency-guard] 最近完成历史已清空（${removed} 条）`);
      state.onChange?.();
      return { cleared: removed };
    },
    /** 按时间 TTL 手动触发一次历史清理（写盘前自动执行，这里供主动调用）。 */
    pruneHistory() {
      const removed = pruneHistoryByTtl(state);
      if (removed > 0) state.onChange?.();
      return { pruned: removed };
    },

    // ---- 会话级并发控制（v1.5.0+） ----
    /**
     * 设置（覆盖）会话并发上限；cap=0 即暂停该会话（后续请求排队，fail-open 兜底）。
     * @param {string} sessionId - 限额键（gateKey：根会话 id 或原始 id）。
     * @param {number} cap - 0..64。
     * @param {{source?: string, note?: string|null}} [opts]
     * @returns {object} 快照（同 status()）。
     * @throws 参数非法时抛错（HTTP 层转 400）。
     */
    setSessionLimit(sessionId, cap, opts = {}) {
      const r = applySessionLimit(state, logger, sessionId, cap, { source: opts.source ?? "service", note: opts.note });
      if (!r.ok) throw new Error(r.error);
      return snapshot(state, false);
    },
    /**
     * 清除会话限额（回退全局门；排队中的请求放行）。
     * @returns {object} 快照。
     */
    clearSessionLimit(sessionId) {
      const r = removeSessionLimit(state, logger, sessionId);
      if (!r.ok) throw new Error(r.error);
      return snapshot(state, false);
    },
    /** 恢复被暂停的会话（pausedFrom 或默认 5）。 */
    resumeSession(sessionId) {
      const r = resumeSessionLimit(state, logger, sessionId);
      if (!r.ok) throw new Error(r.error);
      return snapshot(state, false);
    },
    /** 会话级并发视图（sessions + sessionLimits），供工具/面板使用。 */
    sessionStatus() {
      const snap = snapshot(state, false);
      return { sessions: snap.sessions, sessionLimits: snap.sessionLimits, config: snap.config, updatedAt: snap.updatedAt };
    }
  };
}

/** 惰性注册 HTTP 路由（webServer 服务就绪后才挂载；重复调用幂等）。 */
export function registerHttpRoutes(ctx, state, logger, getApi) {
  let didRoute = false;

  const tryRegister = () => {
    if (didRoute) return;
    try {
      const webServer = ctx.get("webServer");
      if (!webServer || typeof webServer.register !== "function") return;

      ctx.effect(() => webServer.register({
        kind: "exact",
        path: "/api/concurrency-guard/status",
        handler: async (req, res) => {
          const full = (req.url ?? "").includes("full=1");
          sendJson(res, 200, snapshot(state, full));
        }
      }), "concurrency-guard: GET /api/concurrency-guard/status");

      ctx.effect(() => webServer.register({
        kind: "exact",
        path: "/api/concurrency-guard/config",
        handler: async (req, res) => {
          try {
            const body = await readJsonBody(req);
            sendJson(res, 200, getApi().configure(body ?? {}));
          } catch (error) {
            sendJson(res, 400, { error: String(error?.message ?? error) });
          }
        }
      }), "concurrency-guard: POST /api/concurrency-guard/config");

      ctx.effect(() => webServer.register({
        kind: "exact",
        path: "/api/concurrency-guard/history",
        handler: async (req, res) => {
          try {
            const body = await readJsonBody(req);
            const action = body?.action;
            if (action === "clear") {
              sendJson(res, 200, getApi().clearHistory());
            } else if (action === "prune") {
              sendJson(res, 200, getApi().pruneHistory());
            } else if (action === "reset") {
              sendJson(res, 200, getApi().reset());
            } else {
              sendJson(res, 400, { error: "action 必须是 clear / prune / reset" });
            }
          } catch (error) {
            sendJson(res, 400, { error: String(error?.message ?? error) });
          }
        }
      }), "concurrency-guard: POST /api/concurrency-guard/history");

      ctx.effect(() => webServer.register({
        kind: "exact",
        path: "/api/concurrency-guard/sessions",
        handler: async (req, res) => {
          try {
            const body = (await readJsonBody(req)) ?? {};
            const action = body?.action;
            const sessionId = typeof body?.sessionId === "string" ? body.sessionId : "";
            const api = getApi();
            if (action === "set") {
              sendJson(res, 200, api.setSessionLimit(sessionId, body?.cap, { source: "http", note: body?.note }));
            } else if (action === "clear") {
              sendJson(res, 200, api.clearSessionLimit(sessionId));
            } else if (action === "pause") {
              sendJson(res, 200, api.setSessionLimit(sessionId, 0, { source: "http", note: body?.note }));
            } else if (action === "resume") {
              sendJson(res, 200, api.resumeSession(sessionId));
            } else {
              sendJson(res, 400, { error: "action 必须是 set / clear / pause / resume（set 需带 cap）" });
            }
          } catch (error) {
            sendJson(res, 400, { error: String(error?.message ?? error) });
          }
        }
      }), "concurrency-guard: POST /api/concurrency-guard/sessions");

      didRoute = true;
      logger.info(
        "[concurrency-guard] HTTP 端点已注册: GET /status, POST /config, POST /history, POST /sessions"
      );
    } catch (error) {
      logger.warn(`[concurrency-guard] HTTP 端点注册失败（稍后重试）: ${String(error?.message ?? error)}`);
    }
  };

  tryRegister();
  ctx.on("internal/service", (serviceName) => {
    if (serviceName === "webServer") tryRegister();
  });
}

/** 惰性注册 concurrency_status 工具（tools 服务就绪后才挂载）。 */
export function registerTool(ctx, state, logger) {
  let didTool = false;

  const tryRegister = () => {
    if (didTool) return;
    try {
      const tools = ctx.get("tools");
      if (!tools || typeof tools.register !== "function") return;

      tools.register({
        name: "concurrency_status",
        description:
          "查看 DSH 当前全部模型请求的并发状况：活跃/排队数、并发上限与模式、峰值、分模型/分供应商统计、" +
          "在途请求明细，以及最近完成历史（full=true 时）。用于防止并发超限被供应商锁定的监控。",
        parameters: {
          type: "object",
          properties: {
            full: {
              type: "boolean",
              description: "是否附带最近完成历史（默认 false）"
            }
          }
        },
        output: {
          schema: { type: "object" },
          render: (_args, value) => value
        },
        isConcurrencySafe: true,
        timeoutMs: 10_000,
        async execute(args) {
          return snapshot(state, args?.full === true);
        }
      });

      tools.register({
        name: "concurrency_session_list",
        description:
          "列出当前在线活跃会话的并发状况：每个会话（按顶层会话聚合，含子代理）的活跃/等待数、" +
          "会话门排队数、限额（cap）、是否暂停、最近活动时间；以及全部显式限额清单。只读，不改任何状态。",
        parameters: {
          type: "object",
          properties: {}
        },
        output: {
          schema: { type: "object" },
          render: (_args, value) => value
        },
        isConcurrencySafe: true,
        timeoutMs: 10_000,
        async execute() {
          const snap = snapshot(state, false);
          return { sessions: snap.sessions, sessionLimits: snap.sessionLimits, config: snap.config, updatedAt: snap.updatedAt };
        }
      });

      tools.register({
        name: "concurrency_session_set",
        description:
          "实时调整某个在线会话的并发上限（无需重启）：action=set 设置 cap（0..64，0=暂停该会话，" +
          "后续请求排队且排队超时仍 fail-open 兜底）；pause 暂停（等价 cap=0）；resume 恢复（回到暂停前数值）；" +
          "clear 清除限额回退到只走全局门。降低上限不打断在途请求，只影响后续准入。" +
          "sessionId 用 concurrency_session_list 返回的 gateKey（顶层会话 id 或原始会话 id）。",
        parameters: {
          type: "object",
          properties: {
            action: {
              type: "string",
              enum: ["set", "pause", "resume", "clear"],
              description: "set=设置上限 / pause=暂停 / resume=恢复 / clear=清除限额"
            },
            sessionId: {
              type: "string",
              description: "目标会话 id（concurrency_session_list 的 gateKey）"
            },
            cap: {
              type: "number",
              description: "action=set 时的并发上限（0..64；0=暂停）"
            }
          },
          required: ["action", "sessionId"]
        },
        output: {
          schema: { type: "object" },
          render: (_args, value) => value
        },
        isConcurrencySafe: true,
        timeoutMs: 10_000,
        async execute(args) {
          const api = getApi();
          const action = args?.action;
          const sessionId = typeof args?.sessionId === "string" ? args.sessionId : "";
          if (action === "set") {
            return api.setSessionLimit(sessionId, args?.cap, { source: "tool" });
          }
          if (action === "pause") {
            return api.setSessionLimit(sessionId, 0, { source: "tool" });
          }
          if (action === "resume") {
            return api.resumeSession(sessionId);
          }
          if (action === "clear") {
            return api.clearSessionLimit(sessionId);
          }
          throw new Error("action 必须是 set / pause / resume / clear");
        }
      });

      didTool = true;
      logger.info("[concurrency-guard] 工具已注册: concurrency_status / concurrency_session_list / concurrency_session_set");
    } catch (error) {
      logger.warn(`[concurrency-guard] 工具注册失败（稍后重试）: ${String(error?.message ?? error)}`);
    }
  };

  tryRegister();
  ctx.on("internal/service", (serviceName) => {
    if (serviceName === "tools") tryRegister();
  });
}

/** 读取并解析请求 JSON body（异常时返回 null，由调用方按 400 处理）。 */
async function readJsonBody(req) {
  let raw = "";
  for await (const chunk of req) raw += chunk;
  if (raw.trim() === "") return null;
  return JSON.parse(raw);
}

function sendJson(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store"
  });
  res.end(body);
}