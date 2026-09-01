/**
 * dsh-concurrency-guard — 宿主入口。
 *
 * 监控与门闩的核心：DSH 里所有模型请求（主会话、进程内子代理、workflow 派生代理、
 * 会话标题、压缩、以及任何调用 ctx.llm.stream 的插件）最终都汇入
 * `ctx.waterfall(this, "llm/stream", options, next)`。本插件注册
 * `ctx.on("llm/stream", (options, next) => ...)` 瀑布监听：
 *
 *   - 每条请求入注册表（phase=waiting）；
 *   - 首次迭代时经并发门闩 acquire（queue 模式满员则 FIFO 排队 / monitor 不拦）；
 *   - 放行后包一层流（yield* 透传），流结束/取消/错误时 finish() 归还并发位；
 *   - 监听器与流包装全程 try/catch 兜底：任何异常都回退 `next()`，绝不影响模型调用。
 *
 * 挂面（见 api.js）：ctx.concurrencyGuard 服务、HTTP 端点、concurrency_status 工具、
 * 状态文件（persist.js）。
 */
import { createPersister } from "./persist.js";
import { resolveConfig, mergeConfig, requestLabel } from "./config.js";
import { createState } from "./state.js";
import { acquire, logWatermark, ensureSessionSem, acquireSessionSlot, releaseSessionSlot, gcSessionSem } from "./gate.js";
import { finish, newRecord } from "./records.js";
import { classifyKind } from "./classify.js";
import { adoptStats, recordStarted, retainStats } from "./stats.js";
import { createService, registerHttpRoutes, registerTool } from "./api.js";
import { createScopeResolver, rememberSession } from "./scope.js";
import { expireSessionLimits } from "./session-limits.js";
import { readFileSync } from "node:fs";

export const name = "concurrency-guard";

/** llm 保证事件源存在；webServer/tools 用 ctx.get 惰性取（api.js），缺失不致命。 */
export const inject = ["llm"];

/** 过期清扫阈值：从未放行的滞留记录 / 疑似被消费端弃流的超长流。 */
const SWEEP_WAITING_MS = 15 * 60_000;

export function apply(ctx, config) {
  const logger = ctx.logger;
  const state = createState(resolveConfig(config));
  const persister = createPersister(state, logger);
  // 状态变更 → 落盘：finish/configure/reset 都经此钩子，避免散落调用
  state.onChange = persister.schedule;
  // 会话作用域解析器（rootId 归并）：ctx.sessions 惰性接入，缺失降级
  state.resolver = createScopeResolver(ctx);

  // 启动装载：读回持久化统计（尽力而为；首次运行/文件损坏则从零开始）。
  // 同时把"今日"桶接续到内存计数，重启后面板/工具看到的计数不归零。
  try {
    const raw = JSON.parse(readFileSync(state.cfg.stateFile, "utf8"));
    // 配置接续：上次运行时 configure()/设置页改过的配置已随 state.json 落盘，
    // 重启后继续生效（否则面板改的上限/阈值一重启就丢回默认）。
    if (raw?.config && typeof raw.config === "object") {
      state.cfg = resolveConfig(raw.config);
      logger.info(
        `[concurrency-guard] 已接续持久化配置: mode=${state.cfg.mode} max=${state.cfg.maxConcurrency} ` +
          `warnAt=${state.cfg.warnAt} maxQueueWaitMs=${state.cfg.maxQueueWaitMs}`
      );
    }
    const adopted = adoptStats(raw?.stats);
    if (adopted.totals.started > 0 || adopted.today.started > 0) {
      state.stats = adopted;
      const t = adopted.today;
      state.started = t.started;
      state.completed = t.completed;
      state.cancelled = t.cancelled;
      state.interrupted = t.interrupted ?? 0;
      state.gateHeld = t.gateHeld ?? 0;
      state.failOpen = t.failOpen ?? 0;
      state.sessionGateHeld = t.sessionGateHeld ?? 0;
      state.sessionFailOpen = t.sessionFailOpen ?? 0;
      state.warnHits = t.warnHits ?? 0;
      logger.info(
        `[concurrency-guard] 已接续持久化统计：今日 请求=${state.started} 完成=${state.completed} ` +
          `异常=${t.errored} 中断=${state.interrupted}（历史累计 ${adopted.totals.started}）`
      );
      // 历史保留策略：启动时若配置了 dayRetention > 0，立即裁剪超龄历史（byDay/errorDetails/errorEvents/totals）
      if ((state.cfg?.dayRetention ?? 0) > 0) {
        const r = retainStats(state.stats, state.cfg.dayRetention);
        if (r.removed.days + r.removed.details + r.removed.events > 0) {
          logger.info(
            `[concurrency-guard] 按保留 ${state.cfg.dayRetention} 天裁剪历史：` +
              `每日 ${r.removed.days} 天 / 异常聚合 ${r.removed.details} 条 / 逐条事件 ${r.removed.events} 条`
          );
        }
      }
    }
    // 遗留对账：上一个进程快照里的在途记录（waiting/streaming）从未收尾，
    // 即上次退出时仍有请求悬空（进程被杀 / 插件 fiber 重建）→ 记入"中断"。
    const leftovers = Array.isArray(raw?.activeRequests)
      ? raw.activeRequests.filter((r) => r && (r.phase === "waiting" || r.phase === "streaming")).length
      : 0;
    if (leftovers > 0) {
      state.stats.today.interrupted += leftovers;
      state.stats.totals.interrupted += leftovers;
      state.interrupted += leftovers;
      logger.warn(
        `[concurrency-guard] 检测到上次进程遗留 ${leftovers} 个未完成请求（异常退出），已记为中断`
      );
    }
    // 会话限额接续：上次运行设置的会话 cap 随 state.json 落盘，重启后继续生效
    if (Array.isArray(raw?.sessionLimits)) {
      let resumed = 0;
      for (const item of raw.sessionLimits) {
        if (!item || typeof item.scopeKey !== "string" || item.scopeKey === "") continue;
        const cap = Number(item.cap);
        if (!Number.isInteger(cap) || cap < 0 || cap > 64) continue;
        state.sessionLimits.set(item.scopeKey, {
          cap,
          source: typeof item.source === "string" ? item.source : "persisted",
          note: typeof item.note === "string" ? item.note.slice(0, 200) : null,
          updatedAt: Number.isFinite(Number(item.updatedAt)) ? Number(item.updatedAt) : Date.now(),
          pausedFrom: item.pausedFrom !== void 0 && item.pausedFrom !== null ? Number(item.pausedFrom) : null
        });
        resumed += 1;
      }
      if (resumed > 0) {
        logger.info(`[concurrency-guard] 已接续会话限额 ${resumed} 条（重启保留）`);
      }
    }
  } catch {
    logger.debug("[concurrency-guard] 无历史统计可接续（首次运行）");
  }

  // 动态获取 dsh-llm 的 isAgentLoopRequest（请求来源分类）。动态 import + 失败降级，
  // 保证插件在无法解析 @deepseek-ai 包的环境中也可加载（分类退化为启发式）。
  let isLoopRequest = null;
  let isLoopResolved = false;
  const loopCheck = (request) => {
    if (!isLoopResolved) return null;
    return isLoopRequest ? isLoopRequest(request) : null;
  };
  import("@deepseek-ai/dsh-llm")
    .then((mod) => {
      isLoopRequest = typeof mod.isAgentLoopRequest === "function" ? mod.isAgentLoopRequest : null;
      isLoopResolved = true;
      logger.debug("[concurrency-guard] isAgentLoopRequest 已加载，来源分类启用 loop 标记");
    })
    .catch(() => {
      isLoopResolved = true; // 保持 null → 启发式分类
      logger.debug("[concurrency-guard] 无法解析 @deepseek-ai/dsh-llm，来源分类使用启发式");
    });

  // ---------- 核心：llm/stream 瀑布监听 ----------
  ctx.on("llm/stream", (options, next) => {
    // 监听器自身必须绝对安全：任何异常都回退原链路，绝不影响模型调用。
    try {
      const kind = classifyKind(options, loopCheck);
      const rawSid = options.sessionId ?? null;
      // 会话作用域：解析到顶层会话（rootId）作为门闩键；解析失败/无会话 → 原始 id / null
      const gateKey = rawSid ? (state.resolver.resolve(rawSid) || rawSid) : null;
      // 记录"见过"的会话（LRU）——标题后台刷新候选集，请求间隙也能刷到标题
      rememberSession(state, rawSid);
      rememberSession(state, gateKey);
      const exempt = state.cfg.sessionExemptAuxiliary && (kind === "compaction" || kind === "session-title");
      const rec = newRecord(options, kind);
      rec.gateKey = gateKey;
      rec.rootId = gateKey;
      rec.exempt = exempt;
      state.registry.set(rec.id, rec);
      state.started += 1;
      recordStarted(state.stats);
      persister.schedule();

      return (async function* () {
        let drained = false; // 流是否被消费到自然结束（generator 顶部声明：任何 return 路径的 finally 都可见，避免 TDZ）
        try {
          // 已在到达前被取消：不发请求，返回空流（消费端视作正常 stop）。
          if (options.signal?.aborted) {
            finish(state, rec, "cancelled", "aborted-before-gate");
            return;
          }

          let outcome;
          // 会话门：命中显式限额 && 未豁免 && 总开关开 && queue 模式 才启用。
          // 固定获取顺序 会话门→全局门：等全局门期间被 abort → 先还会话位（转移给该会话队首）。
          const limited =
            !exempt &&
            gateKey !== null &&
            state.cfg.sessionLimitsEnabled &&
            state.cfg.mode === "queue" &&
            state.sessionLimits.has(gateKey);
          let sessionSem = null;
          try {
            if (limited) {
              sessionSem = ensureSessionSem(state, gateKey);
              rec.gateStage = "session";
              const sOutcome = await acquireSessionSlot(sessionSem, state, logger, rec, options.signal);
              if (!sOutcome.ok) {
                finish(state, rec, "cancelled", "aborted-queued");
                return;
              }
              rec.holdsSessionSlot = true;
              rec.sessionGate = gateKey;
              rec.gateStage = "global";
              outcome = await acquire(state, logger, rec, options.signal);
              if (!outcome.ok) {
                // 等全局门期间被取消：释放会话位（转移给该会话队首），不留悬空。
                // 先清标记再手动释放，避免 finish() 二次归还。
                rec.holdsSessionSlot = false;
                releaseSessionSlot(sessionSem);
                gcSessionSem(state, gateKey);
                finish(state, rec, "cancelled", "aborted-queued");
                return;
              }
            } else {
              outcome = await acquire(state, logger, rec, options.signal);
              if (!outcome.ok) {
                finish(state, rec, "cancelled", "aborted-queued");
                return;
              }
            }
          } catch (error) {
            // 门闩自身故障：fail-open，本次不阻塞请求（若已持会话位则归还）
            if (rec.holdsSessionSlot && sessionSem) {
              releaseSessionSlot(sessionSem);
              gcSessionSem(state, gateKey);
              rec.holdsSessionSlot = false;
            }
            logger.error(`[concurrency-guard] 门闩异常（不阻塞请求）: ${String(error?.stack ?? error)}`);
            outcome = { ok: true, source: "fallback", waitedMs: 0 };
          }

          rec.ownsSlot = true;
          rec.waitedMs = outcome.waitedMs ?? 0;
          rec.dispatchedAt = Date.now();
          rec.phase = "streaming";

          if (outcome.source === "timeout") {
            logger.warn(
              `[concurrency-guard] ${requestLabel(rec)} 排队超过 ${state.cfg.maxQueueWaitMs}ms，` +
                "强制放行（fail-open，瞬时并发可能超越上限）"
            );
          }
          logWatermark(state, logger, rec);
          persister.schedule();

          const downstream = next();
          try {
            for await (const chunk of downstream) {
              rec.lastActivityAt = Date.now(); // chunk 心跳：弃流判定依据
              // DSH 失败语义：模型请求错误以 finish chunk（reason.kind=error）正常流出、不抛异常。
              // 必须识别并计入 errored/异常分类，否则会被当成正常完成（历史 bug：errored 恒为 0）。
              if (chunk && typeof chunk === "object" && (chunk.type === "finish" || chunk.type === "end") && chunk.reason?.kind === "error") {
                const f = chunk.reason?.failure ?? {};
                const msg = String(f?.message ?? chunk.reason?.error?.message ?? "request failed");
                const code = f?.code ?? chunk.reason?.code;
                rec.errorCode = code ?? null;
                rec.finishError = code ? (msg.includes(`(code=${code})`) ? msg : `${msg} (code=${code})`) : msg;
                // LlmFailure 协议内还有 status / requestId / providerRetryAfterMs——全部接住，
                // 否则诊断信息被白白丢弃（原始响应体在 DSH 的 Error.cause 里、协议不透传）。
                rec.failureStatus = f?.status ?? null;
                rec.failureRequestId = f?.requestId ?? null;
                rec.failureRetryAfterMs = f?.providerRetryAfterMs ?? null;
              }
              yield chunk;
            }
            drained = true;
            finish(state, rec, rec.finishError ? "error" : "ok", rec.finishError);
          } catch (error) {
            if (options.signal?.aborted) {
              finish(state, rec, "cancelled", "aborted-streaming");
            } else {
              finish(state, rec, "error", String(error?.message ?? error));
            }
            throw error;
          }
        } finally {
          // 兜底收尾（幂等）。关键场景：消费端提前 return()/弃流且未走正常完成 ——
          // for-await 未自然结束即视为"回合中断"，记入 interrupted（用户可见）。
          if (!drained && rec.status === "waiting") {
            finish(state, rec, "interrupted", "consumer-stopped-early");
          } else {
            finish(state, rec, rec.status === "waiting" ? "cancelled" : rec.status);
          }
        }
      })();
    } catch (error) {
      logger.error(`[concurrency-guard] 监听器异常，已回退原链路: ${String(error?.stack ?? error)}`);
      return next();
    }
  });

  // ---------- 过期清扫（防消费端弃流导致的记录/并发位泄漏 + 会话限额 TTL） ----------
  let sweepTimer = null;
  const sweep = () => {
    const now = Date.now();
    for (const rec of [...state.registry.values()]) {
      if (rec.phase === "waiting" && now - rec.createdAt > SWEEP_WAITING_MS) {
        logger.warn(
          `[concurrency-guard] 清理滞留记录（从未放行）: ${requestLabel(rec)} 已等待 ${Math.round((now - rec.createdAt) / 1000)}s`
        );
        finish(state, rec, "cancelled", "stale-waiting");
      } else if (rec.phase === "streaming" && now - (rec.lastActivityAt ?? rec.dispatchedAt) > state.cfg.maxStreamStallMs) {
        logger.warn(
          `[concurrency-guard] 清理停滞流（消费端可能已弃流）: ${requestLabel(rec)} 已 ${Math.round((now - (rec.lastActivityAt ?? rec.dispatchedAt)) / 1000)}s 无输出，记为中断并释放并发位`
        );
        finish(state, rec, "interrupted", "stalled-stream");
      }
    }
    // 会话限额 TTL 过期清扫（sessionLimitTtlDays > 0 时启用）
    expireSessionLimits(state, logger);
  };
  sweepTimer = setInterval(sweep, state.cfg.sweepIntervalMs);

  // ---------- 会话标识后台刷新（标题 + cwd；惰性接入 ctx.sessionQuery，缺失静默降级） ----------
  // 正确 API 是 readTitleSnapshots（从事件日志折叠 session/title 标题）——listSessions
  // 的记录只有 {header,live,persisted}，没有 title 字段（v1.5.0 首版取错导致面板无标题）。
  // 候选集 = knownSessions（本进程见过的会话，LRU 300）∪ 显式限额 ∪ 在途——保证
  // 闲置会话（请求间隙）也能刷到标题，而不是依赖"此刻恰好有在途请求"。
  let titleTimer = null;
  let titleRetryTimer = null;
  const refreshTitles = async () => {
    try {
      const sessionQuery = ctx.get("sessionQuery");
      if (!sessionQuery || typeof sessionQuery.readTitleSnapshots !== "function") return;
      const needed = new Set(state.knownSessions.keys());
      for (const key of state.sessionLimits.keys()) needed.add(key);
      for (const rec of state.registry.values()) if (rec.gateKey) needed.add(rec.gateKey);
      if (needed.size === 0) return;
      const snaps = await sessionQuery.readTitleSnapshots([...needed]);
      const meta = new Map();
      for (const s of snaps ?? []) {
        // projectMany 返回 { sessionId, status: "fulfilled"|"rejected", value|reason } 包装
        if (!s || s.status !== "fulfilled") continue;
        const v = s.value;
        const id = v?.session?.id;
        if (!id) continue;
        const t = v?.title;
        meta.set(id, {
          title: typeof t === "string" ? t : (t?.title ?? null),
          cwd: typeof v.session?.cwd === "string" ? v.session.cwd : null
        });
      }
      state.sessionMeta = meta;
    } catch {
      // 静默降级：标题保持缺失，不影响主链路
    }
  };
  titleTimer = setInterval(refreshTitles, state.cfg.sessionTitleRefreshMs);
  refreshTitles();
  // 启动初期 sessionQuery 可能未就绪（插件加载顺序），5s 后兜底重试一次
  titleRetryTimer = setTimeout(refreshTitles, 5000);

  // ---------- 服务 ----------
  const api = createService(state, logger);
  try {
    ctx.provide("concurrencyGuard", api, (self) => typeof self.status === "function");
  } catch (error) {
    logger.warn(`[concurrency-guard] 无法提供 ctx.concurrencyGuard 服务: ${String(error?.message ?? error)}`);
  }

  // ---------- HTTP 端点 + 工具（惰性注册） ----------
  registerHttpRoutes(ctx, state, logger, () => api);
  registerTool(ctx, state, logger, () => api);

  // ---------- Settings 页集成（GUI 免重启改配置；动态 import + 降级，保持零硬依赖） ----------
  let settingsInstalled = false;
  const installSettings = async () => {
    if (settingsInstalled) return;
    try {
      const [{ settingsNamespace, installSettingsSection }, { z }] = await Promise.all([
        import("@deepseek-ai/dsh-settings"),
        import("zod")
      ]);
      const NS = settingsNamespace("concurrency-guard");
      const Config = z.object({
        mode: z.union([z.literal("queue"), z.literal("monitor")]).optional(),
        maxConcurrency: z.number().int().min(1).max(64).optional(),
        warnAt: z.number().int().min(0).max(64).optional(),
        maxQueueWaitMs: z.number().int().min(0).optional(),
        history: z.number().int().min(0).max(1000).optional(),
        historyTtlMs: z.number().int().min(0).optional(),
        maxStreamStallMs: z.number().int().min(1000).optional(),
        sessionLimitsEnabled: z.boolean().optional(),
        sessionExemptAuxiliary: z.boolean().optional(),
        sessionLimitTtlDays: z.number().int().min(0).optional(),
        onlineWindowMs: z.number().int().min(0).optional(),
        sessionTitleRefreshMs: z.number().int().min(5000).optional()
      });
      installSettingsSection(ctx, NS, Config, { ...state.cfg }, {
        setSource: () => {},
        onChange: (snapshot) => {
          if (!snapshot || typeof snapshot !== "object") return;
          state.cfg = mergeConfig(state.cfg, snapshot);
          logger.info(
            `[concurrency-guard] 设置页配置已应用: mode=${state.cfg.mode} max=${state.cfg.maxConcurrency} ` +
              `warnAt=${state.cfg.warnAt} maxQueueWaitMs=${state.cfg.maxQueueWaitMs}`
          );
          state.onChange?.(); // 立即落盘，重启保留
        }
      });
      settingsInstalled = true;
      logger.info("[concurrency-guard] 设置页已注册（Settings → Plugins → Plugin configuration 可编辑）");
    } catch (error) {
      logger.debug(`[concurrency-guard] settings 服务不可用，跳过设置页注册: ${String(error?.message ?? error)}`);
    }
  };
  installSettings();

  logger.info(
    `[concurrency-guard] 已启动: mode=${state.cfg.mode} maxConcurrency=${state.cfg.maxConcurrency} ` +
      `warnAt=${state.cfg.warnAt} sessionLimitsEnabled=${state.cfg.sessionLimitsEnabled} ` +
      `stateFile=${state.cfg.stateFile}`
  );

  // ---------- 卸载/断纤时清理定时器并写最终快照（cordis 无 dispose 事件，用 effect 清理函数挂靠生命周期） ----------
  ctx.effect(() => {
    return () => {
      try {
        if (sweepTimer !== null) clearInterval(sweepTimer);
        if (titleTimer !== null) clearInterval(titleTimer);
        if (titleRetryTimer !== null) clearTimeout(titleRetryTimer);
        persister.flushSync();
      } catch { /* 无视 */ }
    };
  }, "concurrency-guard: teardown");
}