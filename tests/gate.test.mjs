/**
 * dsh-concurrency-guard — 离线门闩测试。
 *
 * 模拟 cordis ctx（on/effect/get/provide/logger + llm/stream 瀑布语义），
 * 不依赖真实 DSH，直接驱动 apply() 验证：
 *   FIFO 排队与位子转移 / monitor 模式 / 排队中 abort / fail-open 超时放行 /
 *   configure 热改 / reset 清零。
 *
 * 运行：node tests/gate.test.mjs（或 npm test）
 */
import { readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { apply } from "../lib/index.js";

// 预清理：无论 cwd 在哪，都清掉上次运行可能残留的 test-state-*.json，
// 避免启动接续（adoption）把旧计数（如 failOpen）带进本次运行。
for (const f of readdirSync(".")) {
  if (/^test-state-\d+\.json$/.test(f)) rmSync(f, { force: true });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function makeHold() {
  let release;
  const gate = new Promise((r) => { release = r; });
  return { gate, release };
}

function makeCtx() {
  const hooks = new Map();
  const provided = {};
  const logger = {
    info: (...a) => console.log("  [info]", ...a),
    warn: (...a) => console.log("  [warn]", ...a),
    error: (...a) => console.log("  [error]", ...a),
    debug: () => {}
  };
  return {
    logger,
    _hooks: hooks,
    provided,
    on(name, cb) { (hooks.get(name) ?? hooks.set(name, []).get(name)).push(cb); return () => {}; },
    effect(fn) { const cleanup = fn(); return () => cleanup?.(); },
    get(name) {
      if (name === "webServer") return { register: () => () => {} };
      if (name === "tools") return { register: () => {} };
      return undefined;
    },
    provide(name, value) { provided[name] = value; }
  };
}

/** 复刻 cordis waterfall：挂名 "llm/stream" 的监听链，最后一个参数是 next。
 *  inner 流挂起在 innerGate 上模拟"仍在流式"；无 gate 则立即完成。 */
function waterfall(ctx, options, innerGate) {
  const listeners = [...(ctx._hooks.get("llm/stream") ?? [])]; // 每次 dispatch 快照（同 cordis）
  const inner = () => (async function* () {
    if (innerGate) await innerGate;
    yield { type: "finish", reason: { kind: "done" } };
  })();
  const next = () => (listeners.shift() ?? inner)(options, next);
  return next();
}

const collect = async (stream) => {
  const out = [];
  for await (const c of stream) out.push(c);
  return out;
};

/** 复刻 waterfall 但内层流直接抛错（模拟供应商侧错误返回）。 */
function waterfallErr(ctx, options, message = "rate limit: 429 Too Many Requests") {
  const listeners = [...(ctx._hooks.get("llm/stream") ?? [])];
  const inner = () => (async function* () { throw new Error(message); })();
  const next = () => (listeners.shift() ?? inner)(options, next);
  return next();
}

let failures = 0;
function check(label, cond, detail = "") {
  console.log(`${cond ? "PASS" : "FAIL"}  ${label}${detail ? `  (${detail})` : ""}`);
  if (!cond) failures += 1;
}
function readStateFile(path) { return JSON.parse(readFileSync(path, "utf8")); }
async function waitState(path, predicate, timeoutMs = 3000, label = "") {
  const start = Date.now();
  let last;
  while (Date.now() - start < timeoutMs) {
    try {
      last = readStateFile(path);
      if (predicate(last)) return last;
    } catch {}
    await sleep(40);
  }
  throw new Error(`state not converged${label ? ` (${label})` : ""}: ${JSON.stringify(last).slice(0, 300)}`);
}
function rmState(...paths) { for (const p of paths) { try { rmSync(p, { force: true }); } catch {} } }

// ---------- 场景 1：队列模式上限 3；6 并发 → 3 活跃 + 3 排队；逐个释放 → FIFO 转移 ----------
{
  console.log("\n== 场景1: FIFO 排队与转移 ==");
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 3, mode: "queue", warnAt: 3, maxQueueWaitMs: 0, stateFile: "test-state-1.json" });
  const holds = [];
  const streams = [];
  for (let i = 0; i < 6; i++) {
    const h = makeHold();
    holds.push(h);
    streams.push(waterfall(ctx, { provider: "p", model: `m${i}`, sessionId: `s${i}` }, h.gate));
  }
  const results = streams.map((st) => collect(st));
  const mid = await waitState("test-state-1.json", (s) => s.gauges.active === 3 && s.gauges.waiting === 3, 3000, "场景1 mid");
  check("场景1: 6 个都已在途（registry 全占）", mid.activeRequests.length === 6, `in-flight=${mid.activeRequests.length}`);
  check("场景1: 活跃 3 / 排队 3", mid.gauges.active === 3 && mid.gauges.waiting === 3, `active=${mid.gauges.active} waiting=${mid.gauges.waiting}`);
  check("场景1: gateHeld === 3", mid.counters.gateHeld === 3, `gateHeld=${mid.counters.gateHeld}`);
  for (let i = 0; i < 6; i++) {
    holds[i].release();
    await sleep(60);
  }
  await Promise.all(results);
  const end = await waitState("test-state-1.json", (s) => s.counters.completed === 6 && s.gauges.active === 0 && s.gauges.queueDepth === 0 && s.gauges.waiting === 0, 3000, "场景1 end");
  check("场景1: 6 个全部完成", end.counters.completed === 6, `completed=${end.counters.completed}`);
  check("场景1: cancelled === 0", end.counters.cancelled === 0, `cancelled=${end.counters.cancelled}`);
  check("场景1: peakActive === 3（从不超限）", end.gauges.peakActive === 3, `peak=${end.gauges.peakActive}`);
  check("场景1: 结束后 active=0 队列空 waiting 归零", end.gauges.active === 0 && end.gauges.queueDepth === 0 && end.gauges.waiting === 0, `active=${end.gauges.active} q=${end.gauges.queueDepth} w=${end.gauges.waiting}`);
  rmState("test-state-1.json");
}

// ---------- 场景 2：monitor 模式只记不拦 ----------
{
  console.log("\n== 场景2: monitor 模式 ==");
  const ctx = makeCtx();
  apply(ctx, { mode: "monitor", maxConcurrency: 2, stateFile: "test-state-2.json" });
  const holds = [];
  const streams = [];
  for (let i = 0; i < 6; i++) {
    const h = makeHold();
    holds.push(h);
    streams.push(waterfall(ctx, { provider: "p", model: `m${i}` }, h.gate));
  }
  const results = streams.map((st) => collect(st));
  const mid = await waitState("test-state-2.json", (s) => s.gauges.active === 6, 3000, "场景2 mid");
  check("场景2: 活跃 6（不受上限约束）", mid.gauges.active === 6, `active=${mid.gauges.active}`);
  check("场景2: 未排队", mid.gauges.waiting === 0 && mid.counters.gateHeld === 0, `waiting=${mid.gauges.waiting}`);
  for (const h of holds) h.release();
  await Promise.all(results);
  const end = await waitState("test-state-2.json", (s) => s.counters.completed === 6, 3000, "场景2 end");
  check("场景2: 全部完成", end.counters.completed === 6, `completed=${end.counters.completed}`);
  check("场景2: peakActive 捕捉到了 6", end.gauges.peakActive === 6, `peak=${end.gauges.peakActive}`);
  rmState("test-state-2.json");
}

// ---------- 场景 3：排队中 abort → 取消不占位；位子转移给下一个 ----------
{
  console.log("\n== 场景3: 排队中 abort ==");
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 1, mode: "queue", maxQueueWaitMs: 0, stateFile: "test-state-3.json" });
  const aHold = makeHold();
  const a = waterfall(ctx, { provider: "p", model: "a" }, aHold.gate);
  const ac = collect(a);
  await sleep(60);
  const abort = new AbortController();
  const b = waterfall(ctx, { provider: "p", model: "b", signal: abort.signal });
  const bc = collect(b);
  const c = waterfall(ctx, { provider: "p", model: "c" });
  const cc = collect(c);
  await sleep(80);
  const mid = await waitState("test-state-3.json", (s) => s.gauges.active === 1 && s.gauges.waiting === 2, 3000, "场景3 mid");
  check("场景3: a 活跃，b/c 排队", mid.gauges.active === 1 && mid.gauges.waiting === 2, `active=${mid.gauges.active} waiting=${mid.gauges.waiting}`);
  abort.abort();
  const mid2 = await waitState("test-state-3.json", (s) => s.counters.cancelled === 1 && s.gauges.waiting === 1, 3000, "场景3 mid2");
  check("场景3: b 被取消（cancelled=1，waiting 降为 1）", mid2.counters.cancelled === 1 && mid2.gauges.waiting === 1, `cancelled=${mid2.counters.cancelled} waiting=${mid2.gauges.waiting}`);
  const br = await bc;
  check("场景3: b 拿到空流", br.length === 0, `b chunks=${br.length}`);
  aHold.release();
  const cr = await cc;
  const ar = await ac;
  const end = await waitState("test-state-3.json", (s) => s.counters.completed === 2 && s.gauges.active === 0 && s.gauges.queueDepth === 0 && s.gauges.waiting === 0, 3000, "场景3 end");
  check("场景3: c 完成", cr.length === 1 && cr[0].type === "finish", `c chunks=${cr.length}`);
  check("场景3: completed === 2（a+c）", end.counters.completed === 2, `completed=${end.counters.completed}`);
  check("场景3: active=0 队列空 waiting 归零", end.gauges.active === 0 && end.gauges.queueDepth === 0 && end.gauges.waiting === 0, `active=${end.gauges.active} q=${end.gauges.queueDepth} w=${end.gauges.waiting}`);
  rmState("test-state-3.json");
}

// ---------- 场景 4：fail-open —— 占位永不释放，仅最先排队者真超时，其余转移 ----------
{
  console.log("\n== 场景4: fail-open 超时放行 ==");
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 1, mode: "queue", maxQueueWaitMs: 150, stateFile: "test-state-4.json" });
  const holdH = makeHold();
  const hc = collect(waterfall(ctx, { provider: "p", model: "hold" }, holdH.gate));
  await sleep(60);
  const r1 = collect(waterfall(ctx, { provider: "p", model: "x1" }));
  const r2 = collect(waterfall(ctx, { provider: "p", model: "x2" }));
  const r3 = collect(waterfall(ctx, { provider: "p", model: "x3" }));
  await sleep(600);
  const mid = await waitState("test-state-4.json", (s) => s.gauges.active === 1 && s.counters.failOpen >= 1, 3000, "场景4 mid");
  check("场景4: 只有最先排队的 1 个 fail-open（其余转移，无二次触发）", mid.counters.failOpen === 1, `failOpen=${mid.counters.failOpen}`);
  check("场景4: peakActive === 2（1 占位 + 1 借位）", mid.gauges.peakActive === 2, `peak=${mid.gauges.peakActive}`);
  check("场景4: 队列清空、waiting 归零（转移泄露修复）", mid.gauges.waiting === 0 && mid.gauges.queueDepth === 0, `waiting=${mid.gauges.waiting} q=${mid.gauges.queueDepth}`);
  const [a1, a2, a3] = await Promise.all([r1, r2, r3]);
  check("场景4: fail-open 的流都能正常完成", a1.length === 1 && a2.length === 1 && a3.length === 1, `[${a1.length},${a2.length},${a3.length}]`);
  holdH.release();
  await hc;
  rmState("test-state-4.json");
}

// ---------- 场景 5：configure 热改上限 ----------
{
  console.log("\n== 场景5: configure 热改 ==");
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 1, mode: "queue", maxQueueWaitMs: 0, stateFile: "test-state-5.json" });
  const svc = ctx.provided.concurrencyGuard;
  check("场景5: ctx.concurrencyGuard 服务已提供", !!svc && typeof svc.status === "function", `svc=${!!svc}`);
  const holdH = makeHold();
  const hc = collect(waterfall(ctx, { provider: "p", model: "hold" }, holdH.gate));
  await sleep(60);
  const q1Hold = makeHold();
  const q1 = collect(waterfall(ctx, { provider: "p", model: "q1" }, q1Hold.gate)); // 排队
  const mid = await waitState("test-state-5.json", (s) => s.gauges.active === 1 && s.gauges.waiting === 1, 3000, "场景5 mid");
  check("场景5: hold 占位，q1 排队", mid.gauges.active === 1 && mid.gauges.waiting === 1, `active=${mid.gauges.active} waiting=${mid.gauges.waiting}`);
  const updated = svc.configure({ maxConcurrency: 2 });
  check("场景5: configure 立即生效（config.maxConcurrency=2）", updated.config.maxConcurrency === 2, `max=${updated.config.maxConcurrency}`);
  const q2Hold = makeHold();
  const q2 = collect(waterfall(ctx, { provider: "p", model: "q2" }, q2Hold.gate)); // 放宽后应直接放行
  const mid2 = await waitState("test-state-5.json", (s) => s.gauges.active === 2 && s.gauges.waiting === 1, 3000, "场景5 after-configure");
  check("场景5: 上限放宽后新请求直接放行（active=2），q1 仍在队列", mid2.gauges.active === 2 && mid2.gauges.waiting === 1, `active=${mid2.gauges.active} waiting=${mid2.gauges.waiting}`);
  q2Hold.release(); // q2 结束 → 位子转移给 q1
  await q2;
  const mid3 = await waitState("test-state-5.json", (s) => s.gauges.active === 2 && s.gauges.waiting === 0 && s.counters.completed === 1, 3000, "场景5 transfer");
  check("场景5: q2 完成后位子转移给 q1（q1 开始流式，completed=1）", mid3.gauges.active === 2 && mid3.gauges.waiting === 0 && mid3.counters.completed === 1, `active=${mid3.gauges.active} waiting=${mid3.gauges.waiting} completed=${mid3.counters.completed}`);
  q1Hold.release();
  holdH.release();
  await Promise.all([hc, q1]);
  const end = await waitState("test-state-5.json", (s) => s.counters.completed === 3 && s.gauges.active === 0, 3000, "场景5 end");
  check("场景5: 全部完成", end.counters.completed === 3, `completed=${end.counters.completed}`);
  rmState("test-state-5.json");
}

// ---------- 场景 6：reset 清零计数与历史 ----------
{
  console.log("\n== 场景6: reset ==");
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 5, mode: "queue", history: 10, stateFile: "test-state-6.json" });
  const a = collect(waterfall(ctx, { provider: "p", model: "a" }));
  const b = collect(waterfall(ctx, { provider: "p", model: "b" }));
  await Promise.all([a, b]);
  const mid = await waitState("test-state-6.json", (s) => s.counters.completed === 2, 3000, "场景6 mid");
  check("场景6: 初始 completed=2", mid.counters.completed === 2, `completed=${mid.counters.completed}`);
  const svc = ctx.provided.concurrencyGuard;
  const after = svc.reset();
  check("场景6: reset 后计数清零", after.counters.completed === 0 && after.counters.started === 0, `completed=${after.counters.completed} started=${after.counters.started}`);
  check("场景6: reset 后历史清空", after.recent === void 0, "recent omitted");
  const c = collect(waterfall(ctx, { provider: "p", model: "c" }));
  await c;
  const end = await waitState("test-state-6.json", (s) => s.counters.completed === 1, 3000, "场景6 end");
  check("场景6: reset 后新记录正常计数", end.counters.completed === 1, `completed=${end.counters.completed}`);
  rmState("test-state-6.json");
}

// ---------- 场景 7：来源分类（kind） ----------
{
  console.log("\n== 场景7: 来源分类 ==");
  // 测试环境中 @deepseek-ai/dsh-llm 不可解析 → isAgentLoopRequest 降级（null）→
  // 走启发式：sessionId "session-" 前缀 → main；有其它 sessionId → subagent；无 → plugin。
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 5, mode: "queue", historyTtlMs: 0, stateFile: "test-state-7.json" });
  const main = collect(waterfall(ctx, { provider: "p", model: "main", sessionId: "session-abc123" }));
  const sub = collect(waterfall(ctx, { provider: "p", model: "sub", sessionId: "297411c2-01eb-4661-bd87" }));
  const plug = collect(waterfall(ctx, { provider: "p", model: "plug" }));
  const title = collect(waterfall(ctx, { provider: "p", model: "t", purpose: "session-title" }));
  const compact = collect(waterfall(ctx, { provider: "p", model: "c", purpose: "compaction" }));
  await Promise.all([main, sub, plug, title, compact]);
  await waitState("test-state-7.json", (s) => s.counters.completed === 5, 3000, "场景7 end");
  // 状态文件为 full=false 快照，kind 从服务 status(true) 校验
  const svc = ctx.provided.concurrencyGuard;
  const fullSnap = svc.status(true);
  const recKinds = fullSnap.recent.map((r) => r.kind).sort();
  check("场景7: 主会话 → main", recKinds.includes("main"), `kinds=${recKinds.join(",")}`);
  check("场景7: 子代理 → subagent", recKinds.includes("subagent"), `kinds=${recKinds.join(",")}`);
  check("场景7: 无 sessionId → plugin", recKinds.includes("plugin"), `kinds=${recKinds.join(",")}`);
  check("场景7: 标题 → session-title", recKinds.includes("session-title"), `kinds=${recKinds.join(",")}`);
  check("场景7: 压缩 → compaction", recKinds.includes("compaction"), `kinds=${recKinds.join(",")}`);
  check("场景7: byKind 聚合结构存在（在途时才有值）", Array.isArray(fullSnap.byKind), `byKind=${JSON.stringify(fullSnap.byKind)}`);
  rmState("test-state-7.json");
}

// ---------- 场景 8：历史清理（clearHistory + TTL） ----------
{
  console.log("\n== 场景8: 历史清理 ==");
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 5, mode: "queue", history: 100, historyTtlMs: 150, stateFile: "test-state-8.json" });
  const svc = ctx.provided.concurrencyGuard;
  const a = collect(waterfall(ctx, { provider: "p", model: "a", sessionId: "session-a" }));
  await a;               // t≈0 完成
  await sleep(400);      // a 已远超 TTL
  const b = collect(waterfall(ctx, { provider: "p", model: "b", sessionId: "session-b" }));
  await b;               // t≈400 完成 → pushHistory 时按 TTL 清掉 a
  const afterB = svc.status(true);
  check("场景8: TTL 后内存历史只剩 b（a 已超龄被清理）", afterB.recent.length === 1 && afterB.recent[0].model === "b", `recent=${JSON.stringify(afterB.recent.map((r) => r.model))}`);
  const cleared = svc.clearHistory();
  check("场景8: clearHistory 返回清理条数", cleared.cleared === 1, `cleared=${cleared.cleared}`);
  const after = svc.status(true);
  check("场景8: 清空后历史为空", after.recent.length === 0, `recent=${after.recent.length}`);
  rmState("test-state-8.json");
}

// ---------- 场景 9：会话活跃聚合（bySession） ----------
{
  console.log("\n== 场景9: 会话活跃聚合 ==");
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 5, mode: "queue", history: 100, historyTtlMs: 0, stateFile: "test-state-9.json" });
  const svc = ctx.provided.concurrencyGuard;
  // A 两笔完成；B 一笔完成；C 在途（hold 挂起）
  const a1 = collect(waterfall(ctx, { provider: "p", model: "a", sessionId: "session-A" }));
  const a2 = collect(waterfall(ctx, { provider: "p", model: "b", sessionId: "session-A" }));
  await Promise.all([a1, a2]);
  const b = collect(waterfall(ctx, { provider: "p", model: "c", sessionId: "agent-123" }));
  await b;
  const holdC = makeHold();
  const cDone = collect(waterfall(ctx, { provider: "p", model: "d", sessionId: "session-C" }, holdC.gate));
  await waitState("test-state-9.json", (s) => s.counters.completed === 3, 3000, "场景9 mid");
  await sleep(100); // 让 C 进入 streaming（消费端已拉流，挂在 hold 上）
  const snap = svc.status(true);
  const sess = Object.fromEntries(snap.bySession.map((s) => [s.sessionId, s]));
  check("场景9: A 两笔完成 → recentCount=2 且 lastEndMs>0", sess["session-A"]?.recentCount === 2 && sess["session-A"].lastEndMs > 0, JSON.stringify(snap.bySession));
  check("场景9: B 一笔完成 → recentCount=1", sess["agent-123"]?.recentCount === 1, `rc=${sess["agent-123"]?.recentCount}`);
  check("场景9: C 在途 → active=1", sess["session-C"]?.active === 1, `active=${sess["session-C"]?.active}`);
  check("场景9: 排序在途优先（C 排第一）", snap.bySession[0].sessionId === "session-C", `first=${snap.bySession[0].sessionId}`);
  holdC.release();
  await cDone;
  rmState("test-state-9.json");
}

// ---------- 场景 10：持久化统计（跨重启接续 + 异常分类） ----------
{
  console.log("\n== 场景10: 持久化统计（重启接续 + 异常分类） ==");
  const file = "test-state-10.json";
  rmState(file);
  // 第一次"进程"
  const ctx1 = makeCtx();
  apply(ctx1, { maxConcurrency: 5, mode: "queue", history: 10, historyTtlMs: 0, stateFile: file });
  await collect(waterfall(ctx1, { provider: "p", model: "a", sessionId: "session-A" }));
  await collect(waterfallErr(ctx1, { provider: "p", model: "b", sessionId: "session-B" })).catch(() => {});
  const s1 = await waitState(file, (s) => s.stats?.today?.errored === 1, 3000, "场景10 s1");
  check("场景10: 今日 started=2", s1.stats.today.started === 2, `started=${s1.stats.today.started}`);
  check("场景10: 今日 completed=1 errored=1", s1.stats.today.completed === 1 && s1.stats.today.errored === 1, `c=${s1.stats.today.completed} e=${s1.stats.today.errored}`);
  check("场景10: 异常分类 rate_limit=1", s1.stats.today.byErrKind?.rate_limit === 1, JSON.stringify(s1.stats.today.byErrKind));
  check("场景10: 来源 byKind.main=2", s1.stats.today.byKind?.main === 2, JSON.stringify(s1.stats.today.byKind));
  // 第二次"进程"（模拟重启）：从 state.json 读回统计并接续计数器
  const ctx2 = makeCtx();
  apply(ctx2, { maxConcurrency: 5, mode: "queue", history: 10, historyTtlMs: 0, stateFile: file });
  const svc2 = ctx2.provided.concurrencyGuard;
  const adopted = svc2.status(true);
  check("场景10: 重启后 stats.today 接续（started=2）", adopted.stats.today.started === 2, `started=${adopted.stats.today.started}`);
  check("场景10: 重启后内存计数接续（started=2）", adopted.counters.started === 2, `counter=${adopted.counters.started}`);
  // 重启后再来一笔 → 3
  await collect(waterfall(ctx2, { provider: "p", model: "c", sessionId: "session-C" }));
  const s2 = await waitState(file, (s) => s.stats?.today?.started === 3, 3000, "场景10 s2");
  check("场景10: 重启后累计到 3 且异常分类保留", s2.stats.today.started === 3 && s2.stats.totals.byErrKind?.rate_limit === 1, `started=${s2.stats.today.started} errKinds=${JSON.stringify(s2.stats.totals.byErrKind)}`);
  rmState(file);
}

// ---------- 场景 11：消费端提前弃流 → interrupted ----------
{
  console.log("\n== 场景11: 消费端提前弃流 ==");
  const file = "test-state-11.json";
  rmState(file);
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 5, mode: "queue", history: 10, historyTtlMs: 0, maxStreamStallMs: 60_000, stateFile: file });
  const hold = makeHold();
  const stream = waterfall(ctx, { provider: "p", model: "a", sessionId: "session-E" }, hold.gate);
  const iter = stream[Symbol.asyncIterator]();
  await sleep(80);           // wrapper 已挂在内层 await hold
  hold.release();            // 内层恢复（yield finish chunk）
  await iter.next();         // 拿到第一块 → streaming
  await iter.return();       // 消费端在流结束前主动放弃 → 回合中断
  const s = await waitState(file, (st) => (st.stats?.today?.interrupted ?? 0) === 1, 3000, "场景11");
  check("场景11: 提前弃流记为 interrupted=1", s.stats.today.interrupted === 1, JSON.stringify(s.stats.today));
  check("场景11: 未计入 ok/cancelled", s.stats.today.completed === 0 && s.stats.today.cancelled === 0, `c=${s.stats.today.completed} x=${s.stats.today.cancelled}`);
  rmState(file);
}

// ---------- 场景 12：停滞流 sweep 兜底 → interrupted ----------
{
  console.log("\n== 场景12: 停滞流 sweep ==");
  const file = "test-state-12.json";
  rmState(file);
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 5, mode: "queue", history: 10, historyTtlMs: 0, maxStreamStallMs: 300, sweepIntervalMs: 200, stateFile: file });
  const hold = makeHold();
  const stream = waterfall(ctx, { provider: "p", model: "b", sessionId: "session-S" }, hold.gate);
  const iter = stream[Symbol.asyncIterator]();
  await sleep(80);
  hold.release();
  await iter.next();         // 拿到一块后【不再消费、也不 return】——模拟消费端死亡/弃流
  const s = await waitState(file, (st) => (st.stats?.today?.interrupted ?? 0) >= 1, 4000, "场景12");
  check("场景12: 无活动超阈值被 sweep 记为 interrupted=1", s.stats.today.interrupted === 1, JSON.stringify(s.stats.today));
  rmState(file);
}

// ---------- 场景 13：启动遗留对账（上次异常退出留下的在途记录 → interrupted） ----------
{
  console.log("\n== 场景13: 启动遗留对账 ==");
  const file = "test-state-13.json";
  rmState(file);
  writeFileSync(file, JSON.stringify({
    activeRequests: [
      { phase: "streaming", provider: "p", model: "x" },
      { phase: "waiting", provider: "p", model: "y" }
    ]
  }));
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 5, mode: "queue", history: 10, historyTtlMs: 0, stateFile: file });
  const svc = ctx.provided.concurrencyGuard;
  const s = svc.status(true);
  check("场景13: 遗留 2 个在途 → interrupted=2", s.stats.today.interrupted === 2, JSON.stringify(s.stats.today));
  check("场景13: 内存计数同步", s.counters.interrupted === 2, `counter=${s.counters.interrupted}`);
  rmState(file);
}

// ---------- 场景 14：finish/error chunk（不抛异常）→ errored + 分类 ----------
{
  console.log("\n== 场景14: finish-error chunk 识别 ==");
  const file = "test-state-14.json";
  rmState(file);
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 5, mode: "queue", history: 10, historyTtlMs: 0, stateFile: file });
  const listeners = [...(ctx._hooks.get("llm/stream") ?? [])];
  // dsh 失败语义：错误以 finish chunk（reason.kind=error）正常流出、不抛异常
  const innerErr = () => (async function* () {
    yield { type: "finish", reason: { kind: "error", failure: { message: "DeepSeek API request to http://max66.xyz/v1 failed", code: "TRANSPORT" } } };
  })();
  const nextErr = () => (listeners.shift() ?? innerErr)({ provider: "p", model: "x" }, nextErr);
  const r = await collect(nextErr());
  check("场景14: 错误流正常透传（不 throw）", r.length === 1 && r[0].reason.kind === "error", `chunks=${r.length}`);
  // 正常一笔对照（重新快照监听器，确保也经过包装器统计）
  const listeners2 = [...(ctx._hooks.get("llm/stream") ?? [])];
  const okInner = () => (async function* () { yield { type: "finish", reason: { kind: "done" } }; })();
  const nextOk = () => (listeners2.shift() ?? okInner)({ provider: "p", model: "y" }, nextOk);
  await collect(nextOk());
  const s = await waitState(file, (st) => (st.stats?.today?.errored ?? 0) === 1, 3000, "场景14");
  check("场景14: error finish chunk 计入 errored=1（completed=1 只含正常笔）", s.stats.today.errored === 1 && s.stats.today.completed === 1, `e=${s.stats.today.errored} c=${s.stats.today.completed}`);
  check("场景14: 分类为 network（TRANSPORT/request failed）", s.stats.totals.byErrKind?.network === 1, JSON.stringify(s.stats.totals.byErrKind));
  rmState(file);
}

// ---------- 场景 15：异常明细聚合（errorDetails 按信息聚类 + 错误码 + 重启保留） ----------
{
  console.log("\n== 场景15: 异常明细聚合 ==");
  const file = "test-state-15.json";
  rmState(file);
  const ctx = makeCtx();
  apply(ctx, { maxConcurrency: 5, mode: "queue", history: 10, historyTtlMs: 0, stateFile: file });
  const emitErr = async (msg, code) => {
    const listeners = [...(ctx._hooks.get("llm/stream") ?? [])];
    const innerErr = () => (async function* () {
      yield { type: "finish", reason: { kind: "error", failure: { message: msg, code } } };
    })();
    const nextErr = () => (listeners.shift() ?? innerErr)({ provider: "p", model: "x" }, nextErr);
    await collect(nextErr());
  };
  // ① 两次 TRANSPORT（仅 URL 不同）→ 归一化后应合并为一条 count=2
  await emitErr("DeepSeek API request to http://a.example/v1 failed", "TRANSPORT");
  await emitErr("DeepSeek API request to http://b.example/v1 failed", "TRANSPORT");
  // ② 一条上游错误 → 独立第二条
  await emitErr("provider upstream 502 Bad Gateway", null);
  const s1 = await waitState(file, (st) => st.stats?.totals?.errored === 3, 3000, "场景15 s1");
  const det = s1.stats.errorDetails ?? {};
  const keys = Object.keys(det);
  check("场景15: 3 笔异常 → errored=3", s1.stats.totals.errored === 3, `e=${s1.stats.totals.errored}`);
  check("场景15: 归一化后聚类为 2 条明细（URL 差异被抹平）", keys.length === 2, `keys=${keys.length} ${JSON.stringify(keys)}`);
  const net = keys.find((k) => k.includes("<url>"));
  const prov = keys.find((k) => k.includes("502"));
  check("场景15: 不同 URL 的 TRANSPORT 合并 count=2 / kind=network / code=TRANSPORT",
    net && det[net].count === 2 && det[net].kind === "network" && det[net].code === "TRANSPORT",
    JSON.stringify(det));
  check("场景15: provider 错误独立一条 count=1 / kind=provider", prov && det[prov].count === 1 && det[prov].kind === "provider", JSON.stringify(det));
  check("场景15: byErrKind network=2 provider=1", s1.stats.totals.byErrKind?.network === 2 && s1.stats.totals.byErrKind?.provider === 1, JSON.stringify(s1.stats.totals.byErrKind));
  // ③ 重启后明细跨进程保留
  const ctx2 = makeCtx();
  apply(ctx2, { maxConcurrency: 5, mode: "queue", history: 10, historyTtlMs: 0, stateFile: file });
  const svc2 = ctx2.provided.concurrencyGuard;
  const s2 = svc2.status(true);
  const det2 = s2.stats.errorDetails ?? {};
  const net2 = Object.values(det2).find((d) => d.kind === "network");
  check("场景15: 重启后 errorDetails 保留（network count=2）", net2?.count === 2 && Object.keys(det2).length === 2, JSON.stringify(det2));
  rmState(file);
}

console.log(failures === 0 ? "\nALL PASS" : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);