/**
 * 运行时状态容器：计数器、FIFO 等待队列、在途注册表、最近完成历史、持久化统计。
 *
 * 各模块（gate/records/persist/api）共享这一个对象，避免全局变量与循环依赖。
 */
import { createStats } from "./stats.js";

export function createState(cfg) {
  return {
    pid: process.pid,
    startedAt: Date.now(),
    /** 当前有效配置（configure() 会整体替换为新的冻结配置） */
    cfg,

    // ---- 仪表 ----
    active: 0,                 // 正在流式（已放行、未结束）的请求数 = 真实 HTTP 并发
    waiting: 0,                // 门闩队列深度（与 waiters.length 一致）
    peakActive: 0,
    peakWaiting: 0,

    // ---- 计数（内存态；跨重启由 stats 接续，见 index.js 启动装载） ----
    started: 0,                // 进入瀑布的总数
    completed: 0,
    cancelled: 0,
    interrupted: 0,            // 弃流/滞留/遗留等"回合中断"（非正常完成、非错误）
    gateHeld: 0,               // 因上限而排队过的请求总数
    failOpen: 0,               // 排队超时强制放行数
    warnHits: 0,
    lastWarnAt: 0,
    lastQueueLogAt: 0,

    // ---- 统计（持久化：随 state.json 落盘，重启读回，按天汇总） ----
    stats: createStats(),

    // ---- 结构 ----
    waiters: [],               // FIFO：{ rec, signal, queuedAt, settle, timer, settled }
    registry: new Map(),       // id -> record（waiting/streaming 期）
    history: []                // 最近完成记录（环形，按 cfg.history 截断）
  };
}