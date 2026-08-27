# dsh-concurrency-guard

DSH（DeepSeek Harness）并发请求监控与门闩插件。

挂钩 DSH 唯一的模型请求总线 `llm/stream` 瀑布，统计**全部**在途模型请求（主会话、
进程内子代理、workflow 派生代理、会话标题、压缩、以及任何调用 `ctx.llm.stream` 的
插件），并在并发到达上限后对后续请求 FIFO 排队——**从根上防止并发超限被供应商/
中继锁号**（如 429 / 风控封禁）。

自带 **WebUI 实时面板**（会话视图「并发监控」页签）、HTTP JSON 端点、落盘状态文件
与 `concurrency_status` 工具。

## 特性

- 🔎 **口径完整**：挂在 `llm/stream` 瀑布（所有模型调用的唯一入口），不存在旁路；
  每条请求带 `provider/model`、`sessionId`、用途（对话/压缩/标题），可精确归属。
- 🏷️ **来源分类**：在途/历史请求按来源标记——**主会话 / 子代理 / 插件 / 压缩 / 标题**
  （agent loop 标记 + sessionId 形状判别，面板有「来源」列与分类概览）。
- 👥 **会话活跃**：按会话聚合在途/排队/最近开始/近 60s 完成数——模型请求间隙
  （跑工具等）在途为 0 时，也能一眼看出"某个会话还在不在推进"（面板「会话活跃」表）。
- 🚦 **FIFO 门闩**：默认 `mode=queue, maxConcurrency=5`——并发满员后新请求排队，
  并发**永不超限**；排队中被取消立即出队；排队超时 **fail-open** 强制放行（宁可
  瞬时超限也不卡死请求）。
- 🧹 **历史自动清理**：最近完成记录双保险——条数上限（`history`，默认 30）+
  时间 TTL（`historyTtlMs`，默认 1h，超龄自动清理）；面板「🗑 清历史」一键清空。
- 🖥️ **WebUI 面板**：仪表卡 + 并发水位条 + 在途/分模型/分供应商/最近完成表；
  可一键热切「排队节制 ⇄ 仅监控」、调整并发上限、暂停轮询、清空历史。
- 📦 **零构建链、零依赖**：纯 node 内建 + cordis API 实现，手写 `__ModuleLoader__`
  client bundle，无需 npm install / tsdown / tsc。
- 🛡️ **fail-safe**：监听器任何异常一律回退原链路；消费端弃流由周期清扫兜底释放并发位。

## 安装

本机（已装 `dsh-super-injector` 提供 `dev_*` 工具）：

```text
git clone <本仓库> D:\Company\dsh-plugin
dev_inject_plugin D:\Company\dsh-plugin\dsh-concurrency-guard
```

其它机器：

```text
# 方式 A：克隆仓库后直接注入
git clone <本仓库> && dev_inject_plugin <克隆目录>/dsh-concurrency-guard

# 方式 B：npm 打包安装（插件目录内 npm pack 产出 tgz）
npm pack
npm i dsh-concurrency-guard-<ver>.tgz -w <目标 profile>
```

> ⚠️ 首次安装（含修改 `package.json` 的 `dsh.client`/`exports`）后需**重启 dsh 宿主**
> 使 client 行生效（Node 进程级缓存 package 元数据），重启后刷新 WebUI 即出现
> 「并发监控」页签。宿主门闩/HTTP/工具注入后立即生效，无需重启。

## 使用

### 实时监控（四选一）

| 方式 | 用法 |
|---|---|
| WebUI 面板 | 会话页顶部视图切换 →「并发监控」页签（1.5s 轮询，可暂停） |
| HTTP 端点 | `GET http://127.0.0.1:3080/api/concurrency-guard/status`（`?full=1` 带最近历史） |
| 状态文件 | `Get-Content $DSH_HOME\concurrency-guard\state.json`（防抖 250ms） |
| 工具 | 模型可直接调用 `concurrency_status`（`{"full": true}` 带历史） |

### 面板内/HTTP 热改

- 面板按钮：切换模式（排队节制/仅监控）、`上限− / 上限+`、`🗑 清历史`；
- `POST http://127.0.0.1:3080/api/concurrency-guard/config`，body 如
  `{"mode":"monitor"}`、`{"maxConcurrency":8}`；
- `POST http://127.0.0.1:3080/api/concurrency-guard/history`，body
  `{"action":"clear"}`（清空历史）或 `{"action":"prune"}`（按 TTL 清理）；
- 其它插件：`ctx.concurrencyGuard.configure({...})` / `.status()` / `.reset()` /
  `.clearHistory()` / `.pruneHistory()`。

## 配置

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `DSH_CG_MAX_CONCURRENCY` | 5 | 并发上限（供应商/中继限制数） |
| `DSH_CG_MODE` | `queue` | `queue`=排队节制；`monitor`=只监控不拦 |
| `DSH_CG_WARN_AT` | 4 | 活跃并发达到该值记 warn |
| `DSH_CG_STATE_FILE` | `$DSH_HOME/concurrency-guard/state.json` | 状态文件路径 |
| `DSH_CG_MAX_QUEUE_WAIT_MS` | 300000 | 排队超时强制放行；`0`=无限等待 |
| `DSH_CG_HISTORY` | 30 | 最近完成记录保留条数（硬上限） |
| `DSH_CG_HISTORY_TTL_MS` | 3600000 | 历史记录时间 TTL ms；`0`=关闭（只靠条数上限） |

优先级：运行时 `configure()` > loader config > 环境变量 > 默认值。

## 架构

```
宿主 lib/                              WebUI lib/client.js（手写 __ModuleLoader__ bundle）
─────────────────────                  ───────────────────────────────────────────
lib/index.js  入口：llm/stream 瀑布监听  conversation.view 槽 →「并发监控」页签
              门闩 acquire → 包流透传 →   1.5s 轮询 GET /status?full=1
              finish() 收尾（幂等）      仪表卡/水位条/三张表
lib/gate.js   FIFO 信号量：转移/abort/   模式切换 + 上限调节 → POST /config
              fail-open（定时器清理）    页面隐藏自动暂停轮询
lib/records.js 记录生命周期 + 快照组装（含 byKind/bySession / 历史 TTL 清理）
lib/classify.js 请求来源分类（main/subagent/plugin/compaction/session-title）
lib/persist.js 状态文件 250ms 防抖写（写盘前顺带 TTL 清理）
lib/api.js    服务 + HTTP 端点（/status /config /history）+ 工具
lib/config.js 配置解析（env/config/运行时）
```

## 开发

```text
npm test                  # 离线门闩测试（不依赖真实 DSH；mock cordis ctx）
dev_reload_package dsh-concurrency-guard   # host 热重载（改宿主代码后）
# 改 WebUI 面板：直接改 lib/client.js 后刷新页面即可（bundle 按 rev 缓存，重载 host 联动 rev）
```

测试覆盖：FIFO 排队与位子转移 / monitor 模式 / 排队中 abort / fail-open 无二次触发 /
`configure` 热改 / `reset` 清零 / 来源分类 / 历史清空与 TTL / 会话活跃聚合。

## 监控范围（谁会被统计）

| 来源 | 是否监控 |
|---|---|
| 主会话每一轮模型请求 | ✅（用途=对话） |
| `subagent` / `subagent_fork` 进程内子代理 | ✅（sessionId=agent id，可区分） |
| 会话标题生成 / 压缩 | ✅（用途=标题/压缩） |
| workflow 派生代理（模型调用回宿主进程） | ✅ |
| 任何走 `ctx.llm.stream()` 的插件（如 modlens 转发、super-injector 守护 agent） | ✅ |
| 插件直连自身 API（如 imagegen 直连 `/chat/completions`、mnemon 本地 Ollama embedding） | ❌（独立通道，不占中继并发；除非其端点指向同一中继才会绕过门闩） |
| 非模型请求（web 搜索 / MCP / SSH / 代码运行时） | ❌（与并发锁无关） |

**来源分类原理**：purpose（压缩/标题）→ 明确归类；否则用 dsh-llm 的 agent loop
标记（`isAgentLoopRequest`）判定是否会话代理构造——是则按 sessionId 形状区分
主会话（`session-` 前缀）与子代理（agent id）；非 loop 请求（插件自调）归为「插件」。
dsh-llm 不可解析时自动降级为纯 sessionId 启发式。

## 已知边界

- **单进程门闩**：多 dsh 实例并行时各自独立计数，请按实例数下调每实例上限；
  状态文件按 `pid` 区分实例。
- 浏览器侧直连提供商的通道不经宿主 `llm/stream`（本环境无此通道，不受影响）。

## License

MIT（见 [LICENSE](./LICENSE)）。# dsh-concurrency-guard