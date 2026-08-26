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
import { resolveConfig, requestLabel } from "./config.js";
import { createState } from "./state.js";
import { acquire, logWatermark } from "./gate.js";
import { finish, newRecord } from "./records.js";
import { classifyKind } from "./classify.js";
import { createService, registerHttpRoutes, registerTool } from "./api.js";

export const name = "concurrency-guard";

/** llm 保证事件源存在；webServer/tools 用 ctx.get 惰性取（api.js），缺失不致命。 */
export const inject = ["llm"];

/** 过期清扫阈值：从未放行的滞留记录 / 疑似被消费端弃流的超长流。 */
const SWEEP_WAITING_MS = 15 * 60_000;
const SWEEP_STREAMING_MS = 30 * 60_000;

export function apply(ctx, config) {
  const logger = ctx.logger;
  const state = createState(resolveConfig(config));
  const persister = createPersister(state, logger);
  // 状态变更 → 落盘：finish/configure/reset 都经此钩子，避免散落调用
  state.onChange = persister.schedule;

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
      const rec = newRecord(options, classifyKind(options, loopCheck));
      state.registry.set(rec.id, rec);
      state.started += 1;
      persister.schedule();

      return (async function* () {
        try {
          // 已在到达前被取消：不发请求，返回空流（消费端视作正常 stop）。
          if (options.signal?.aborted) {
            finish(state, rec, "cancelled", "aborted-before-gate");
            return;
          }

          let outcome;
          try {
            outcome = await acquire(state, logger, rec, options.signal);
          } catch (error) {
            // 门闩自身故障：fail-open，本次不阻塞请求。
            logger.error(`[concurrency-guard] 门闩异常（不阻塞请求）: ${String(error?.stack ?? error)}`);
            outcome = { ok: true, source: "fallback", waitedMs: 0 };
          }

          // 排队期间被取消：立即出队，空流返回。
          if (!outcome.ok) {
            finish(state, rec, "cancelled", "aborted-queued");
            return;
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
            yield* downstream; // 透传；消费端提前 return() 也会经 yield* 转发给下游
            finish(state, rec, "ok");
          } catch (error) {
            if (options.signal?.aborted) {
              finish(state, rec, "cancelled", "aborted-streaming");
            } else {
              finish(state, rec, "error", String(error?.message ?? error));
            }
            throw error;
          }
        } finally {
          // 兜底收尾：正常路径 finish 已执行（幂等），此处覆盖"消费端弃流未走正常完成"的情况。
          finish(state, rec, rec.status === "waiting" ? "cancelled" : rec.status);
        }
      })();
    } catch (error) {
      logger.error(`[concurrency-guard] 监听器异常，已回退原链路: ${String(error?.stack ?? error)}`);
      return next();
    }
  });

  // ---------- 过期清扫（防消费端弃流导致的记录/并发位泄漏） ----------
  let sweepTimer = null;
  const sweep = () => {
    const now = Date.now();
    for (const rec of [...state.registry.values()]) {
      if (rec.phase === "waiting" && now - rec.createdAt > SWEEP_WAITING_MS) {
        logger.warn(
          `[concurrency-guard] 清理滞留记录（从未放行）: ${requestLabel(rec)} 已等待 ${Math.round((now - rec.createdAt) / 1000)}s`
        );
        finish(state, rec, "cancelled", "stale-waiting");
      } else if (rec.phase === "streaming" && now - rec.dispatchedAt > SWEEP_STREAMING_MS) {
        logger.warn(
          `[concurrency-guard] 清理滞留流（可能已被消费端弃流）: ${requestLabel(rec)} 已流式 ${Math.round((now - rec.dispatchedAt) / 1000)}s，释放并发位`
        );
        finish(state, rec, "cancelled", "stale-streaming");
      }
    }
  };
  sweepTimer = setInterval(sweep, 60_000);

  // ---------- 服务 ----------
  const api = createService(state, logger);
  try {
    ctx.provide("concurrencyGuard", api, (self) => typeof self.status === "function");
  } catch (error) {
    logger.warn(`[concurrency-guard] 无法提供 ctx.concurrencyGuard 服务: ${String(error?.message ?? error)}`);
  }

  // ---------- HTTP 端点 + 工具（惰性注册） ----------
  registerHttpRoutes(ctx, state, logger, () => api);
  registerTool(ctx, state, logger);

  logger.info(
    `[concurrency-guard] 已启动: mode=${state.cfg.mode} maxConcurrency=${state.cfg.maxConcurrency} ` +
      `warnAt=${state.cfg.warnAt} stateFile=${state.cfg.stateFile}`
  );

  // ---------- 卸载/断纤时清理定时器并写最终快照（cordis 无 dispose 事件，用 effect 清理函数挂靠生命周期） ----------
  ctx.effect(() => {
    return () => {
      try {
        if (sweepTimer !== null) clearInterval(sweepTimer);
        persister.flushSync();
      } catch { /* 无视 */ }
    };
  }, "concurrency-guard: teardown");
}