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
- 🧹 **历史自动清理**：最近完成记录双保险——条数上限（`history`，默认 30）+
  时间 TTL（`historyTtlMs`，默认 1h，超龄自动清理）；面板「🗑 清历史」一键清空。
- 📈 **持久化统计**：按天汇总（请求/完成/异常/**中断**/取消/门闩/fail-open）+ **异常分类聚合**
  （限流/超时/网络/中断/鉴权/上游/其他）+ **异常明细**（按错误信息聚类：相同异常聚合计数、
  含错误码、**最近会话/模型/供应商**与**各会话出现次数**，URL/时间戳差异自动归一化合并）+
  来源分布；随状态文件落盘，**重启不丢**——面板「今日统计」「每日历史」与「异常明细」
  跨进程连续累计。
  异常识别覆盖 DSH 的失败语义：请求错误既可能是抛异常，也可能以
  `finish` chunk（`reason.kind=error`）**正常流出不抛异常**——两者均计入异常（v1.3.3 修复，
  异常明细聚类 v1.3.4、会话上下文 v1.3.5）。
- 🚨 **回合中断检测**：消费端提前弃流（流式输出被掐断在中间）、进程被杀/fiber 重建的
  遗留在途、以及"长时间无输出"的僵死流——三路兜底统一记为**中断**（独立计数 + 面板
  「中断」列 + 启动日志告警），不再把这类事故悄悄算进"完成"。
- 🖥️ **WebUI 面板**：仪表卡 + 并发水位条 + **页签分组**（今日统计/异常明细/在途与模型/会话活跃/
  最近完成，避免长页平铺）+ 在途/分模型/分供应商/最近完成表；
  可一键热切「排队节制 ⇄ 仅监控」、调整并发上限、暂停轮询、清空历史。
- 📦 **零构建链、零依赖**：纯 node 内建 + cordis API 实现，手写 `__ModuleLoader__`
  client bundle，无需 npm install / tsdown / tsc。
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
| `DSH_CG_MAX_STREAM_STALL_MS` | 600000 | 流式请求"无输出"判死阈值 ms（弃流兜底记中断；`0`=仅靠提前弃流路径） |

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
lib/stats.js  持久化统计：按天汇总 + 异常分类（重启读回接续）
lib/classify.js 请求来源分类（main/subagent/plugin/compaction/session-title）
lib/persist.js 状态文件 250ms 防抖写（写盘前顺带 TTL 清理）
lib/api.js    服务 + HTTP 端点（/status /config /history）+ 工具
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
会话上下文聚合 + 重启保留）。

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