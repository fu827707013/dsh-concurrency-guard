# dsh-concurrency-guard

[![npm version](https://img.shields.io/npm/v/dsh-concurrency-guard?color=green)](https://www.npmjs.com/package/dsh-concurrency-guard)

DSH（DeepSeek Harness）并发请求监控与门闩插件。已发布 npm：`npm i dsh-concurrency-guard`。

挂钩 DSH 唯一的模型请求总线 `llm/stream` 瀑布，统计**全部**在途模型请求（主会话、
进程内子代理、workflow 派生代理、会话标题、压缩、以及任何调用 `ctx.llm.stream` 的
插件），并在并发到达上限后对后续请求 FIFO 排队——**从根上防止并发超限被供应商/
中继锁号**（如 429 / 风控封禁）。

自带 **WebUI 实时面板**（会话视图「并发监控」页签）、HTTP JSON 端点、落盘状态文件
与 `concurrency_status` 工具。

## 效果预览

![并发监控面板](img/demo.png)

会话视图「并发监控」页签：仪表卡 + 并发水位条 + 在途请求（含来源分类）/ 会话活跃
/ 分模型 / 最近完成（含历史清理）等一览无余。

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
- 🎯 **会话级并发控制（v1.5.0+）**：给**在线活跃会话实时设置并发数**——面板新页签
  「会话并发」/ `POST /api/concurrency-guard/sessions` / `ctx.concurrencyGuard.setSessionLimit()`
  / 工具 `concurrency_session_set` 四通道热改，无需重启，随 state.json 持久化。
  - **按顶层会话聚合**：请求的 sessionId 经 `header.parentSession` 父链解析归并到
    **根会话（rootId）**——限「对话 A」= 主循环 + 其全部子代理 + 压缩/标题共用同一把
    会话锁；子代理解析不到根时降级按原始 id 生效（面板标注）。
  - **会话标识**：每行显示「标题（session/title 事件）+ 短会话 id + 工作区目录」
    三行标识（`sessionQuery.readTitleSnapshots` 后台刷新，缺失降级），一眼认出是哪个会话。
  - 两道门：请求需同时通过 **会话门（可选）→ 全局门**，固定获取顺序无死锁；
    会话满员时即使全局有空位也按该会话自己的 FIFO 队列排队。
  - `cap=0` = **暂停该会话**（全部排队，排队超时仍 fail-open 兜底，不会永久卡死）；
    降低上限不打断在途请求，只影响后续准入；`clear` 清除后排队请求自动放行回全局门。
  - 辅助请求（压缩/标题）默认**豁免会话门**（`sessionExemptAuxiliary`，只过全局门），
    避免书签性请求被会话限流拖死；monitor 模式与 `sessionLimitsEnabled=false` 时
    会话门整体跳过（限额保留但惰性生效）。
- 🧹 **历史自动清理**：最近完成记录双保险——条数上限（`history`，默认 30）+
  时间 TTL（`historyTtlMs`，默认 1h，超龄自动清理）；面板「🗑 清历史」一键清空。
- 📈 **持久化统计**：按天汇总（请求/完成/异常/**中断**/取消/门闩/fail-open）+ **异常分类聚合**
  （限流/超时/网络/中断/鉴权/上游/其他）+ **异常明细双报表**：① 汇总——按 **会话×错误** 分组
  （次数 + 最近出现，URL/时间戳差异自动归一化）；② 明细——**每次错误一条**（按会话+时间排序，
  最近 200 条滚动窗口）；每条含错误码/HTTP status/供应商 requestId/Retry-After +
  最近会话/模型/供应商与各会话出现次数；随状态文件落盘，**重启不丢**——面板「今日统计」
  「每日历史」与「异常明细」跨进程连续累计。
  异常识别覆盖 DSH 的失败语义：请求错误既可能是抛异常，也可能以
  `finish` chunk（`reason.kind=error`）**正常流出不抛异常**——两者均计入异常（v1.3.3 修复，
  异常明细聚类 v1.3.4、会话上下文 v1.3.5、协议内字段 v1.3.8、逐条事件明细 v1.3.9）。
- 🚨 **回合中断检测**：消费端提前弃流（流式输出被掐断在中间）、进程被杀/fiber 重建的
  遗留在途、以及"长时间无输出"的僵死流——三路兜底统一记为**中断**（独立计数 + 面板
  「中断」列 + 启动日志告警），不再把这类事故悄悄算进"完成"。
- 🖥️ **WebUI 面板**：仪表卡 + 并发水位条 + **页签分组**（今日统计/异常明细/在途与模型/会话活跃/
  最近完成，避免长页平铺）+ 在途/分模型/分供应商/最近完成表；
  可一键热切「排队节制 ⇄ 仅监控」、调整并发上限、暂停轮询、清空历史、重置统计。
- ⚙️ **配置友好**：面板内置**配置面板**（并发上限/告警阈值/排队超时/历史条数/TTL/停滞判死
  直接数字编辑，保存即生效）；接入 **Settings → Plugins → Plugin configuration** 可视化编辑；
  **配置持久化**——运行时改的配置随 state.json 落盘，重启保留（v1.4.0 修复此前"重启丢配置"）。
- 📦 **零构建链、零依赖**：纯 node 内建 + cordis API 实现，手写 `__ModuleLoader__`
  client bundle，无需 npm install / tsdown / tsc；设置页经动态 import 接入
  `@deepseek-ai/dsh-settings`（可选 peer，缺失时自动降级跳过，不影响其余功能）。
- 🛡️ **fail-safe**：监听器任何异常一律回退原链路；消费端弃流由周期清扫兜底释放并发位。

## 安装

DSH 的插件装在 **profile** 里：每个 profile 是一个独立的 npm 项目目录
（默认 `~/.dsh/profiles/<profile名>`，本机示例 `C:\Users\pc\.dsh\profiles\web`），
装进哪个 profile，哪个 WebUI/会话就用上它。

### 方式 1：dsh 官方 CLI（推荐，已发布到官方源）

DSH 自带的插件管理命令（内部在 profile 目录跑 pnpm）：

```text
# ① 一条命令安装：自动写入 dependencies + 自动把本包加入 dsh.profile.bundles
dsh plugin --profile web add dsh-concurrency-guard
# 也支持 GitHub 地址 / 本地路径：dsh plugin --profile web add <GitHub地址> 等
```

（本包在 package.json 声明了 `dsh.bundle.patch`（cordis.patch.yml 装配补丁），
`dsh plugin add` 跑完后会自动把它挂进 `dsh.profile.bundles`，**无需手改
package.json**。）

```text
# ② 重启 dsh 宿主 → 刷新 WebUI
# 验证是否生效（三选一）：
#   - 会话视图顶部出现「并发监控」页签
#   - GET http://127.0.0.1:3080/api/concurrency-guard/status 返回 200
#   - 宿主日志出现 [concurrency-guard] 已启动
```

```text
# 卸载：dsh plugin --profile web remove dsh-concurrency-guard（同样自动清理 bundles），重启
```

> ℹ️ DSH 的插件管理基于 **pnpm**（profile 用 `pnpm-lock.yaml`）。请用 `dsh plugin`
> 安装/卸载，**不要**在 profile 目录里直接 `npm i`（会混入 npm 锁文件状态）。

### 方式 2：git clone + dev_inject_plugin（本机开发 / 调试）

```text
git clone https://github.com/fu827707013/dsh-concurrency-guard.git
dev_inject_plugin <克隆目录>           # 需本机装有 dsh-super-injector，热注入免重启
# 或：dsh plugin --profile web add <克隆目录>（CLI 也支持本地路径，见方式 1）
```

（注意：`dev_reload_package` 只对方式 2 的源码链接生效；CLI/pnpm 装的副本改代码后需
重装并重启。）

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

- 面板按钮：切换模式（排队节制/仅监控）、`🗑 清历史`；「会话并发」页签每行
  cap 输入 + 应用 / 暂停 / 恢复 / 清除限额；
- `POST http://127.0.0.1:3080/api/concurrency-guard/config`，body 如
  `{"mode":"monitor"}`、`{"maxConcurrency":8}`；
- `POST http://127.0.0.1:3080/api/concurrency-guard/history`，body
  `{"action":"clear"}`（清空历史）或 `{"action":"prune"}`（按 TTL 清理）；
- `POST http://127.0.0.1:3080/api/concurrency-guard/sessions`，body 如
  `{"action":"set","sessionId":"session-xxx","cap":2}`（设置/覆盖上限，0=暂停）、
  `{"action":"pause","sessionId":"..."}`、`{"action":"resume","sessionId":"..."}`、
  `{"action":"clear","sessionId":"..."}`（清除限额回退全局门）；
  `GET /status` 返回新增 `sessions`（在线会话并发视图）与 `sessionLimits`；
- 工具：模型可直接调用 `concurrency_session_list`（只读列出在线会话与限额）与
  `concurrency_session_set`（实时调整某会话上限/暂停/恢复/清除）；
- 其它插件：`ctx.concurrencyGuard.configure({...})` / `.status()` / `.reset()` /
  `.clearHistory()` / `.pruneHistory()` / `.setSessionLimit(id, cap)` /
  `.clearSessionLimit(id)` / `.resumeSession(id)` / `.sessionStatus()`。

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
| `DSH_CG_MAX_STREAM_STALL_MS` | 600000 | 流式请求"无输出"判死阈值 ms（弃流兜底记中断；`0`=仅靠提前弃流路径） |
| `DSH_CG_SESSION_LIMITS_ENABLED` | `true` | 会话级并发控制总开关（false=跳过会话门，限额保留但惰性生效） |
| `DSH_CG_SESSION_EXEMPT_AUXILIARY` | `true` | 压缩/标题请求豁免会话门（只过全局门） |
| `DSH_CG_SESSION_LIMIT_TTL_DAYS` | 0 | 会话限额条目自动过期天数（0=永久保留；>0 按未再使用天数清扫） |
| `DSH_CG_ONLINE_WINDOW_MS` | 600000 | 会话"在线"判定窗口 ms（在途或最近活动落在窗口内即在线） |
| `DSH_CG_SESSION_TITLE_REFRESH_MS` | 60000 | 会话标题后台刷新周期 ms（惰性接入 ctx.sessionQuery，缺失自动降级） |

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
              会话门（v1.5.0+）：与全局门同构的按 gateKey 独立 FIFO
lib/records.js 记录生命周期 + 快照组装（含 byKind/bySession / 历史 TTL 清理）
lib/stats.js  持久化统计：按天汇总 + 异常分类（重启读回接续）
lib/classify.js 请求来源分类（main/subagent/plugin/compaction/session-title）
lib/scope.js  sessionId → 根会话（rootId）父链解析（ctx.sessions 惰性接入 + 缓存降级）
lib/session-limits.js 会话限额唯一写入口（set/clear/resume/TTL 过期，四通道共用）
lib/persist.js 状态文件 250ms 防抖写（写盘前顺带 TTL 清理）
lib/api.js    服务 + HTTP 端点（/status /config /history /sessions）+ 工具
              （concurrency_status / concurrency_session_list / concurrency_session_set）
lib/config.js 配置解析（env/config/运行时）
```

## 开发

```text
npm test                  # 离线门闩测试（不依赖真实 DSH；mock cordis ctx）
dev_reload_package dsh-concurrency-guard   # host 热重载（改宿主代码后）
npm publish --registry https://registry.npmjs.org   # 发布新版（开 2FA 时加 --otp <6位验证码>）
# 改 WebUI 面板：直接改 lib/client.js 后刷新页面即可（bundle 按 rev 缓存，重载 host 联动 rev）
```

测试覆盖：FIFO 排队与位子转移 / monitor 模式 / 排队中 abort / fail-open 无二次触发 /
`configure` 热改 / `reset` 清零 / 来源分类 / 历史清空与 TTL / 会话活跃聚合 /
持久化统计（跨重启接续 + 异常分类计数）/ 中断检测（提前弃流 / 停滞 sweep / 启动遗留对账）/
finish-error chunk 识别（DSH 不抛异常的请求失败）/ 异常明细聚合（按信息聚类计数 + 错误码 +
会话上下文聚合 + 重启保留）/ 逐条错误事件（每次一条、会话时间排序、滚动上限、重启保留）/
会话级限额（cap=2 会话排队 / cap=0 暂停 + 会话门 fail-open / clear 放行排队 /
根会话聚合父链解析 / 等全局门时 abort 会话位转移）。

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
- **错误信息 = DSH 层归一化的 `LlmFailure`**：面板记录的 message/code/HTTP status/
  requestId/Retry-After 全部来自 DSH `finish` chunk 的 `failure` 字段（协议内最大值）。
  上游（含中转站）返回的**原始响应体**（如 `{"detail":"上游(maxapi)返回 400: ..."}`）被
  DSH 适配器保留在错误的 `cause` 里、**llm/stream 协议不透传**——任何挂在此瀑布的插件
  都拿不到，需 DSH 侧改进（如 `LlmFailure` 增加 `detail` 字段）。
- **子代理归并依赖 live session 表**：v1.5.0 起「会话并发」按 `header.parentSession`
  父链把子代理归并到根会话（限"对话 A"含其全部子代理）。该解析走 `ctx.sessions`
  live 表（同步、缓存 60s）；子代理已落盘/进程内 driver 未挂 live session 时
  降级按原始 sessionId 生效（面板标注），不阻断请求。

## 插件商店收录

已收录于 [awesome-dsh-plugin](https://github.com/awesome-dsh-plugin/awesome-dsh-plugin)
目录（条目文件 `data/plugins/fu827707013__dsh-concurrency-guard.yml`），
可在 [插件市场](https://github.com/dsh-market/dsh-market) 与
[awesome-dsh-plugin.com](https://awesome-dsh-plugin.com) 检索到。

- 截图：本仓库根 `screenshots.json` 声明（`img/demo.png`），推仓库即自动更新，无需再提 PR
- 更新条目（描述/分类）：改 `data/plugins/fu827707013__dsh-concurrency-guard.yml`
  后重新生成 READMEs（`npm ci && node scripts/generate-readme.mjs`）再提 PR
- 提交门槛（CI 自动检查）：仓库创建 ≥ 1 天、提交数 ≥ 10、声明 `dsh.bundle`、
  描述与实际功能一致

## License

MIT（见 [LICENSE](./LICENSE)）。