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
import { readFileSync, rmSync } from "node:fs";
import { apply } from "../lib/index.js";

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

console.log(failures === 0 ? "\nALL PASS" : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);