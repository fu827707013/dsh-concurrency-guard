# dsh-concurrency-guard · 会话级并发控制设计（v2 提案）

> 目标：在现有**全局 FIFO 门闩**之上，新增**按在线活跃会话实时设置并发数**的能力——
> 面板/HTTP/工具/服务四通道热改，无需重启，配置持久化。
>
> 现状（v1.4.2）：`llm/stream` 瀑布单点挂钩，一把全局信号量（`maxConcurrency`），
> 已有来源分类（main/subagent/plugin/compaction/session-title）、按会话活跃聚合、
> 历史/异常/持久化/WebUI 面板。本设计**不推翻**现有模型，只在其上加"会话维度的第二道门"。

---

## 1. 核心语义：两道门（全局门 + 会话门）

一条模型请求要真正发出，需要**同时**通过两道门闩：

```
llm/stream 瀑布
   │
   ├─ ① 会话门（可选）：scopeKey 命中显式限额时才存在
   │      FIFO 信号量，cap 来自 sessionLimits[scopeKey]
   │      辅助请求（compaction/session-title）默认豁免
   │
   ├─ ② 全局门（现有）：maxConcurrency 全局 FIFO 信号量
   │
   ▼
   发出请求
```

- **固定获取顺序：会话门 → 全局门**。先占会话位、再等全局位；
  在等全局位期间被 abort → **先释放会话位**（转移给该会话队首）再返回取消，杜绝 ABBA 死锁。
- 未命中显式限额的会话：只过全局门（行为与 v1 完全一致，零回归）。
- 降低上限**不打断在途请求**，只影响后续准入（经典信号量语义）；提高上限即时生效。
- 会话门满员：该会话后续请求进**该会话自己的 FIFO 队列**（即使全局有空位），
  这是"限会话"的本质；排队超时同样 fail-open（复用现有 `maxQueueWaitMs` 语义）。
- `cap = 0` 定义为 **暂停该会话**：全部排队（fail-open 超时兜底仍生效，避免永久卡死），
  面板显示"已暂停"徽标并记 warn 日志。

### 关键不变式
| 不变式 | 说明 |
|---|---|
| 全局并发永不超限 | 全局门计数与 v1 相同（fail-open 短暂超限除外，语义不变） |
| 会话并发永不超限 | 会话门按 scopeKey 独立计数；辅助请求豁免是显式配置，不破坏该会话主请求的限流 |
| 无死锁 | 两门固定顺序获取；单请求同时至多持有一个会话位 + 一个全局位 |
| 幂等收尾 | `finish()` 归还时分别转移会话队首与全局队首，复用现有 settle 机制 |

---

## 2. 作用域模型：scopeKey 与 rootId

`llm/stream` 的 `options.sessionId` 是**请求直接归属**；但用户口中的"会话"是**顶层对话**。
子代理是独立 session（裸 UUID），其请求的 `sessionId` 不是根会话 id。

| 概念 | 来源 | 用途 |
|---|---|---|
| `scopeKey`（原始 sessionId） | `options.sessionId`，恒可得 | 门闩计数的**精确键** |
| `rootId`（顶层会话） | 经 `ctx.sessions.get(id).header.parentSession` 向上走链（动态获取、缓存 60s） | 面板分组 / 限额设置的**默认键** |
| `title`（会话标题） | `ctx.sessionQuery.listSessions()` 折叠标题（惰性、失败降级） | 面板可读性 |

**规则：**
- 限额按 `rootId` 设置为主（用户视角一致：限"对话 A"= 它的主循环 + 全部子代理 + 压缩/标题都受同一把锁）。
- 子代理 session 解析不到根（如会话已落盘、进程内 driver 未挂 live session）→ 降级用原始 `sessionId` 作为键，面板标注"未解析"。
- 高级能力：也可按**原始 sessionId** 设限（精确限某一个子代理），面板提供"原始 id"视图。
- 无 `sessionId` 的插件请求（modlens 转发等）归 `null` 键：永远只过全局门，不可会话限流（文档说明）。

**解析链缓存**：`Map<sessionId, {rootId, ts}>`，TTL 60s；`ctx.sessions` 服务缺失或查询抛错一律降级，不影响主链路（与插件"零硬依赖"哲学一致：动态 import + try/catch）。

---

## 3. 数据模型与持久化

### 运行时状态（state.js 增量）
```js
sessionLimits: Map<scopeKey, {           // 显式限额（含 0=暂停）
  cap: number,                           // 0..64；undefined/缺省 = 不限（只走全局门）
  source: "ui"|"http"|"tool"|"service"|"settings",
  note: string,                          // 可选备注
  updatedAt: number,
  lastUsedAt: number                     // 命中过期清扫
}>,
sessionWaiters: Map<scopeKey, Array<entry>>,   // 各会话自己的 FIFO 队（entry 形状同全局 waiters）
sessionGateHeld: 0, sessionFailOpen: 0,        // 会话门计数（并入快照与持久化统计）
ancestry: Map<sessionId, {rootId, ts}>          // rootId 解析缓存
```

### 持久化（state.json 增量）
```jsonc
{
  "sessionLimits": [
    { "scopeKey": "session-xxx…", "cap": 2, "source": "ui", "note": "压测", "updatedAt": 172… }
  ]
}
```
- 启动读回：`resolveConfig` 后合并，重启保留（与现有 `config` 接续同路径）。
- 可选自动过期：`sessionLimitTtlDays`（默认 0 = 永久保留；>0 时按 `lastUsedAt` 清扫未再活动的限额，防僵尸条目）。
- 手动清除：面板/API `clear`。

### 配置增量（config.js）
| 键 | 默认 | 说明 |
|---|---|---|
| `sessionLimitsEnabled` | `true` | 总开关；关掉后会话门完全跳过（只留全局门） |
| `sessionExemptAuxiliary` | `true` | compaction/session-title 豁免会话门（仍过全局门） |
| `sessionLimitTtlDays` | `0` | 限额条目自动过期天数（0=永久） |
| `onlineWindowMs` | `600_000` | 会话"在线"判定窗口（活动在窗口内即在线） |

---

## 4. 门闩实现（gate.js 重构）

把现有全局信号量**泛化成 `createSemaphore()`**，全局门与会话门共用同一实现：

```js
// 现在的 acquire/releaseSlot 逻辑原样泛化：
//   acquire(sem, state, rec, signal, cfg) → 立即/排队/fail-open/abort
//   releaseSlot(sem, state)              → 位子转移
const global = createSemaphore();        // 现状，行为零变化
const sessions = new Map();              // scopeKey -> { sem, cap }
```

**复合获取（stream 监听器内）**
```
1. scope = resolveScope(options)            // scopeKey + rootId + kind + 是否豁免
2. 豁免或未命中限额 → 直接进全局门（v1 路径）
3. 否则：
   a. await sessionAcquire(scope)            // 会话门；abort → 空流返回
   b. await globalAcquire()                  // 全局门；期间 abort → releaseSessionSlot(scope) 后返回
4. 放行 → 包流 → finish() 时双释放
```

**fail-open 双路径**：会话门等待超时 → 放行并记 `sessionFailOpen`；全局门等待超时 → 放行记 `failOpen`（现有）。两处超时日志都带 scopeKey。

**实时调限的并发安全**：
- 准入时**读取当前 cap**（Map.get），改动发生在准入边界之外 → 天然无锁；
- 唯一小竞态：cap 从 2 降到 1 的瞬间恰好有第 2 条在准入中 → 瞬时超限 1，可接受，文档注明；
- 降 cap 时**不主动打断**在途，面板提示"将在下次请求生效"。

---

## 5. 实时调整通道（四通道写同一服务方法）

| 通道 | 用法 | 新增 |
|---|---|---|
| WebUI 面板 | 新页签「会话并发」 | 在线会话表 + 每行 cap stepper / 暂停 / 恢复 / 清除限制；全局开关在配置弹窗 |
| HTTP | `POST /api/concurrency-guard/sessions` | body `{action:"set"\|"clear"\|"pause"\|"resume", sessionId, cap?}`；`GET /status` 扩展返回 `sessions`/`sessionLimits` |
| 服务 | `ctx.concurrencyGuard` | `setSessionLimit(scopeKey, cap, note?)` / `clearSessionLimit(scopeKey)` / `sessionStatus()` |
| 工具 | 模型侧 | `concurrency_session_list`（只读）+ `concurrency_session_set`（控制，`isConcurrencySafe: true`，不占并发位） |

所有通道最终调用 `setSessionLimit` → 更新 Map + `state.onChange()` 落盘 + 面板下一轮轮询（1.5s）即见。

### 面板「会话并发」页签（client.js 增量）
- 数据：`/status` 新字段 `sessions[]`（按活跃度排序，只列"在线"：registry 在途 + 近 `onlineWindowMs` 有活动 + 显式限额命中）
- 每行：标题（有则显）/ 短 id（rootId）/ 来源徽标（主/子代理/插件）/ 活跃·等待（分"会话门等待"与"全局门等待"两栏）/ 限额控件 / 最近活动时间
- 操作列：`− 当前值 +`（1..64）、`⏸ 暂停`（=0）、`▶ 恢复`（回退到原值，存 `pausedFrom`）、`✕ 清除限制`（回全局）
- 配置弹窗追加：`启用会话级限制`、`辅助请求豁免`、`限额过期天数` 三开关

---

## 6. 快照 / 统计 / 历史扩展

```jsonc
// snapshot() 新增
"sessions": [ { sessionId, rootId, title, kind, online, active, waiting,
                sessionWaiting, globalWaiting, cap, capped: true, paused: false,
                lastActivityMs, gateHeld, failOpen } ],
"sessionLimits": [ { scopeKey, cap, source, note, updatedAt } ]
// counters 新增
"sessionGateHeld", "sessionFailOpen"
// history 条目新增
"scopeKey", "gates": "session+global" | "global"
```
- 每会话计数并入 `stats`（今日/总计，跨重启连续），复用 `recordFinish` 路径。
- `bySession` 聚合升级为 `sessions`（补 cap/gate 细分），向后兼容保留旧字段。

---

## 7. 边界情况清单

| # | 场景 | 行为 |
|---|---|---|
| 1 | 会话 A 满员，全局有空位 | A 的新请求进 A 的队列（会话限流的本意） |
| 2 | 会话 A 满员且全局也满 | 进 A 队列；A 队首拿到 A 位后继续等全局位 |
| 3 | 降低 A 的上限 | 在途不打断；后续准入按新 cap |
| 4 | cap=0（暂停） | 全排队 + warn 日志；fail-open 超时仍兜底放行（防卡死） |
| 5 | abort 于会话门等待 | 立即出 A 队，不占位 |
| 6 | abort 于全局门等待（已持会话位） | 释放会话位（转移给 A 队首）再取消 |
| 7 | 会话结束/消失 | 限额条目保留（持久化）；可手动 clear 或按 TTL 自动过期 |
| 8 | 会话重启后同 id 恢复 | 限额自动续用（同 id = 同一会话） |
| 9 | 子代理解析不到根 | 降级以原始 sessionId 为键，面板标"未解析" |
| 10 | 无 sessionId 的插件请求 | 只过全局门，不可会话限流（文档注明） |
| 11 | 压缩/标题请求 | 默认豁免会话门（`sessionExemptAuxiliary`），仍过全局门 |
| 12 | 插件不可解析 dsh-session | 全部降级启发式（现状行为），会话门仅按原始 id 生效 |
| 13 | 两个会话同时降 cap | 各自独立，无全局耦合 |
| 14 | 面板轮询间隙内多次改 cap | 最后一次生效（Map 覆盖写），落盘防抖合并 |

---

## 8. 实施计划（分阶段，每阶段可独立验证）

| 阶段 | 内容 | 验证 |
|---|---|---|
| P1 门闩泛化 | gate.js → `createSemaphore()`；双门复合 acquire/release；测试用例 | `tests/gate.test.mjs` 扩展：双门、abort 释放、fail-open、降 cap 不打断 |
| P2 状态与持久化 | state.js 增量 + sessionLimits 读写 + TTL 清扫 + ancestry 解析（惰性 ctx.sessions） | 重启接续单测；解析降级路径 |
| P3 API/服务/工具 | api.js 扩展四通道；快照新字段；concurrency_session_list/set | HTTP curl 冒烟；工具注册日志 |
| P4 面板 | client.js 新页签 + 配置弹窗三开关 | 手工验证：设 1 → 该会话后续请求排队；暂停 → 徽标；清除 → 回全局 |
| P5 收尾 | README/CHANGELOG、版本 1.5.0、npm pack | `npm test` 全绿；dev_reload_package 热重载验证 |

**向后兼容**：P1–P5 全程 `sessionLimitsEnabled=true` 但无任何显式限额时，行为与 v1.4.2 完全一致（会话门无命中即跳过）；旧 state.json 无 `sessionLimits` 字段 → 读回为空 Map，无迁移成本。

---

## 10. 实施对照（v1.5.0 落地记录）

已按本设计实施（2026-08，版本 1.5.0），审核与实现中的修正：

1. **monitor 模式同时跳过会话门**（设计第 4 节未明确）：`mode!=="queue"` 时会话门整体跳过，
   与全局门语义一致——"只监控不拦"对两扇门都成立。
2. **`sessionLimitsEnabled=false`**：限额条目保留但惰性生效（面板可继续编辑，不产生拦截）。
3. **会话门计数并入持久化统计**：`stats.today/totals` 新增 `sessionGateHeld` / `sessionFailOpen`
   （EMPTY_DAY 增量），跨重启连续累计，快照 counters 同步暴露。
4. **实现结构**：新增 `lib/scope.js`（rootId 父链解析，`ctx.sessions` 惰性接入、同步、缓存 60s、
   缺失降级）与 `lib/session-limits.js`（限额唯一写入口：set/clear/resume/TTL 过期，四通道共用）。
5. **门闩泛化**：现有全局门 `acquire/releaseSlot` 原样保留；会话门为同构实现
   （`createSessionSem`/`acquireSessionSlot`/`releaseSessionSlot`/`settleSessionWaiters`/
   `gcSessionSem`），复合获取在 index.js 监听器内固定顺序 会话门→全局门。
6. **双重释放防护**：等全局门期间 abort → 先清 `holdsSessionSlot` 再手动还会话位，
   避免 `finish()` 二次归还（测试场景 22 覆盖）。
7. **`onlineWindowMs` / `sessionTitleRefreshMs`** 为配置字段（env/loader 可设），面板弹窗
   只暴露 3 个用户级开关（启用会话级限制 / 辅助请求豁免 / 限额过期天数）。
8. **会话标题**：后台 60s 经 `ctx.sessionQuery.listSessions()` 刷新（try/catch 静默降级），
   快照 `sessions[].title` 只读展示。
9. **面板**：新增「会话并发」页签（每行 cap 输入 + 应用 / 暂停 / 恢复 / 清除），
   配置弹窗追加 3 开关；`/status` 返回 `sessions` + `sessionLimits`。
10. **测试**：场景 18–22 覆盖 cap=2 会话排队 / cap=0 暂停 + 会话门 fail-open /
    clear 放行排队 / 根会话聚合（孙→子→根父链）/ 等全局门 abort 会话位转移；全量 22 场景 ALL PASS。

**已知实现边界**：clear 放行时已持会话位的在途请求由 finish() 幂等归还（sem 置 disabled 后
排空即 GC）；准入检查为同步 check-then-act，单 tick 内无并发窗口，降 cap 只影响后续准入。

## 9. 开放决策（已确认，见实施对照）

1. **限额键默认 rootId 还是原始 sessionId？**（本设计：rootId 为主、原始 id 为辅——最贴合"会话"直觉）
2. **cap=0 是否即"暂停"语义？**（本设计：是，且 fail-open 超时仍兜底）
3. **辅助请求是否豁免会话门？**（本设计：默认豁免）
4. 是否本次直接实施 P1–P5？（预计改动 gate/state/config/records/api/index/client 六个文件 + 测试）
