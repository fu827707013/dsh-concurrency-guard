/**
 * dsh-concurrency-guard — WebUI 实时面板（client bundle）
 *
 * 会话视图注册「并发监控」页签（conversation.view 槽，list 槽 → 新增页签），
 * 轮询宿主 API 实时渲染：活跃/上限、排队、峰值、计数、分模型/分供应商、在途明细、
 * 最近完成历史；并支持在面板内热切模式（排队/仅监控）与调整并发上限。
 *
 * 构建协议：手写 __ModuleLoader__ bundle（与 tsdown 产物同协议），**零构建链**：
 *   依赖仅 react（shell seed），服务经 ctx.slots 注入；package.json 的
 *   dsh.client.inject 已声明运行时种子。
 */
window.__ModuleLoader__.load({
	id: "dsh-concurrency-guard",
	factory: (require) => {
		var module = { exports: {} };
		var exports = module.exports;
		Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
		const React = require("react");
		const { useState, useEffect, useCallback, useRef } = React;
		const h = React.createElement;

		// ---------- 常量 ----------
		const STATUS_URL = "/api/concurrency-guard/status?full=1";
		const CONFIG_URL = "/api/concurrency-guard/config";
		const HISTORY_URL = "/api/concurrency-guard/history";
		const POLL_MS = 1500;
		const PURPOSE_LABEL = { compaction: "压缩", "session-title": "标题" };
		const KIND_LABEL = { main: "主会话", subagent: "子代理", plugin: "插件", compaction: "压缩", "session-title": "标题", unknown: "未知" };
		const PHASE_LABEL = { streaming: "流式中", waiting: "等待" };
		const STATUS_LABEL = { ok: "完成", cancelled: "取消", error: "错误", waiting: "等待" };
		const dsw = (name, fb) => `var(--dsw-alias-${name}, ${fb})`;

		// ---------- 工具 ----------
		const fmtAge = (ms) => (ms >= 1000 ? `${(ms / 1000).toFixed(ms < 10000 ? 1 : 0)}s` : `${ms}ms`);
		const fmtTs = (ts) => (ts ? new Date(ts).toLocaleTimeString("zh-CN", { hour12: false }) : "—");
		const fmtUptime = (sec) => {
			if (!Number.isFinite(sec)) return "—";
			const m = Math.floor(sec / 60);
			return m >= 60 ? `${Math.floor(m / 60)}h${m % 60}m` : `${m}m${sec % 60}s`;
		};
		const shortId = (id) => (id || "").slice(0, 8);
		const toneOf = (active, max, warnAt) =>
			active >= max ? dsw("state-error-primary", "#ff4f5e") : active >= (warnAt ?? 4) ? dsw("state-warn-primary", "#eab308") : dsw("state-success-primary", "#22c55e");
		const kindColor = (kind) =>
			kind === "main" ? dsw("label-primary-bluish", "#60a5fa")
				: kind === "subagent" ? dsw("state-warn-primary", "#eab308")
					: kind === "plugin" ? dsw("state-error-primary", "#ff4f5e")
						: kind === "compaction" || kind === "session-title" ? dsw("label-tertiary", "#8a8a99")
							: V.dim;

		const V = {
			panel: dsw("bg-module-platform", "#14141a"),
			border: dsw("border-l2", "#2a2a33"),
			border1: dsw("border-l1", "#23232b"),
			text: dsw("label-primary", "#e6e6ee"),
			dim: dsw("label-tertiary", "#8a8a99"),
			ok: dsw("state-success-primary", "#22c55e"),
			warn: dsw("state-warn-primary", "#eab308"),
			err: dsw("state-error-primary", "#ff4f5e")
		};

		const css = {
			wrap: { padding: "16px 20px 28px", color: V.text, fontFamily: "var(--dsw-font-family, inherit)" },
			header: { display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap", marginBottom: "12px" },
			title: { fontSize: "16px", fontWeight: 600, margin: 0 },
			badge: { fontSize: "12px", padding: "2px 10px", borderRadius: "999px", border: `1px solid ${V.border}` },
			meta: { fontSize: "12px", color: V.dim, marginLeft: "auto", display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" },
			btn: { fontSize: "12px", padding: "3px 10px", borderRadius: "8px", border: `1px solid ${V.border}`, background: "transparent", color: V.text, cursor: "pointer" },
			btnPrimary: { fontSize: "12px", padding: "3px 10px", borderRadius: "8px", border: "1px solid transparent", background: dsw("state-business-primary", "#3b82f6"), color: "#fff", cursor: "pointer" },
			grid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(150px, 1fr))", gap: "10px", marginBottom: "14px" },
			card: { border: `1px solid ${V.border}`, borderRadius: "10px", padding: "10px 12px", background: V.panel },
			cardLabel: { fontSize: "12px", color: V.dim, marginBottom: "4px" },
			cardValue: { fontSize: "22px", fontWeight: 700, fontVariantNumeric: "tabular-nums" },
			cardSub: { fontSize: "11px", color: V.dim, marginTop: "2px" },
			bar: { height: "6px", borderRadius: "999px", background: V.border1, marginTop: "6px", overflow: "hidden" },
			barFill: { height: "100%", borderRadius: "999px", transition: "width .3s" },
			section: { marginTop: "16px" },
			sectionTitle: { fontSize: "13px", fontWeight: 600, color: V.dim, margin: "0 0 8px", letterSpacing: ".02em" },
			table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontVariantNumeric: "tabular-nums" },
			th: { textAlign: "left", padding: "6px 10px", color: V.dim, borderBottom: `1px solid ${V.border}`, whiteSpace: "nowrap" },
			td: { padding: "6px 10px", borderBottom: `1px solid ${V.border1}`, whiteSpace: "nowrap" },
			err: { border: `1px solid ${V.err}`, borderRadius: "8px", padding: "8px 12px", fontSize: "12px", color: V.err, margin: "8px 0" }
		};

		const gauge = (label, value, color, sub) =>
			h("div", { style: css.card },
				h("div", { style: css.cardLabel }, label),
				h("div", { style: { ...css.cardValue, color } }, String(value)),
				sub ? h("div", { style: css.cardSub }, sub) : null
			);

		const table = (headers, rows, emptyText) => {
			if (!rows || rows.length === 0) {
				return h("div", { style: { fontSize: "12px", color: V.dim, padding: "8px 2px" } }, emptyText || "无");
			}
			return h("div", { style: { overflowX: "auto" } },
				h("table", { style: css.table },
					h("thead", null, h("tr", null, headers.map((hd, i) => h("th", { key: i, style: css.th }, hd)))),
					h("tbody", null, rows.map((row, ri) =>
						h("tr", { key: ri }, row.map((cell, ci) =>
							h("td", { key: ci, style: cell && cell.color ? { ...css.td, color: cell.color } : css.td }, cell && cell.text !== void 0 ? cell.text : String(cell))
						))
					))
				)
			);
		};

		// ---------- 主面板 ----------
		function ConcurrencyPanel() {
			const [data, setData] = useState(null);
			const [error, setError] = useState(null);
			const [paused, setPaused] = useState(false);
			const [lastOk, setLastOk] = useState(null);
			const [connected, setConnected] = useState(false);
			const [busy, setBusy] = useState(false);
			const mounted = useRef(true);

			const poll = useCallback(async () => {
				try {
					const res = await fetch(STATUS_URL, { cache: "no-store" });
					if (!res.ok) throw new Error(`HTTP ${res.status}`);
					const json = await res.json();
					if (!mounted.current) return;
					setData(json);
					setError(null);
					setConnected(true);
					setLastOk(Date.now());
				} catch (e) {
					if (!mounted.current) return;
					setError(e.message || String(e));
					setConnected(false);
				}
			}, []);

			const patchConfig = useCallback(async (partial) => {
				if (busy) return;
				setBusy(true);
				try {
					const res = await fetch(CONFIG_URL, {
						method: "POST",
						headers: { "content-type": "application/json" },
						body: JSON.stringify(partial)
					});
					const json = await res.json();
					if (!res.ok) throw new Error(json?.error || `HTTP ${res.status}`);
					if (mounted.current) setData(json);
					poll();
				} catch (e) {
					setError(`配置更新失败：${e.message || e}`);
				} finally {
					if (mounted.current) setBusy(false);
				}
			}, [busy, poll]);

			const patchHistory = useCallback(async (action) => {
				if (busy) return;
				setBusy(true);
				try {
					const res = await fetch(HISTORY_URL, {
						method: "POST",
						headers: { "content-type": "application/json" },
						body: JSON.stringify({ action })
					});
					const json = await res.json();
					if (!res.ok) throw new Error(json?.error || `HTTP ${res.status}`);
					poll();
				} catch (e) {
					setError(`历史操作失败：${e.message || e}`);
				} finally {
					if (mounted.current) setBusy(false);
				}
			}, [busy, poll]);

			useEffect(() => {
				mounted.current = true;
				const onVisible = () => { if (document.visibilityState === "visible") poll(); };
				document.addEventListener("visibilitychange", onVisible);
				const timer = setInterval(() => {
					if (!paused && document.visibilityState === "visible") poll();
				}, POLL_MS);
				poll();
				return () => {
					mounted.current = false;
					clearInterval(timer);
					document.removeEventListener("visibilitychange", onVisible);
				};
			}, [poll, paused]);

			const cfg = data?.config;
			const g = data?.gauges;
			const c = data?.counters;
			const max = cfg?.maxConcurrency ?? 5;
			const warnAt = cfg?.warnAt ?? 4;
			const active = g?.active ?? 0;
			const color = toneOf(active, max, warnAt);
			const pct = max > 0 ? Math.min(100, Math.round((active / max) * 100)) : 0;

			const activeRows = (data?.activeRequests || []).map((r) => [
				{ text: shortId(r.id), color: V.dim },
				{ text: KIND_LABEL[r.kind] ?? r.kind ?? "—", color: kindColor(r.kind) },
				`${r.provider}/${r.model}`,
				r.purpose ? (PURPOSE_LABEL[r.purpose] ?? r.purpose) : "—",
				r.sessionId ? shortId(r.sessionId) : "—",
				{ text: PHASE_LABEL[r.phase] ?? r.phase, color: r.phase === "streaming" ? V.ok : V.warn },
				{ text: fmtAge(r.ageMs ?? 0), color: V.dim },
				r.waitedMs ? `${r.waitedMs}ms` : "—"
			]);
			const modelRows = (data?.byModel || []).map((m) => [
				m.provider,
				m.model,
				{ text: String(m.active), color: m.active > 0 ? (m.active >= max ? V.err : V.ok) : V.dim },
				{ text: String(m.waiting), color: m.waiting > 0 ? V.warn : V.dim }
			]);
			const providerRows = (data?.byProvider || []).map((p) => [
				p.provider,
				{ text: String(p.active), color: p.active > 0 ? V.ok : V.dim },
				{ text: String(p.waiting), color: p.waiting > 0 ? V.warn : V.dim }
			]);
			const recentRows = (data?.recent || []).map((r) => [
				{ text: fmtTs(r.createdMs), color: V.dim },
				{ text: KIND_LABEL[r.kind] ?? r.kind ?? "—", color: kindColor(r.kind) },
				`${r.provider}/${r.model}`,
				r.purpose ? (PURPOSE_LABEL[r.purpose] ?? r.purpose) : "—",
				{ text: STATUS_LABEL[r.status] ?? r.status, color: r.status === "ok" ? V.ok : r.status === "cancelled" ? V.warn : V.err },
				{ text: `${r.durationMs ?? 0}ms` + (r.waitedMs ? `（等 ${r.waitedMs}ms）` : ""), color: V.dim }
			]);
			const kindSummary = (data?.byKind || []).map((k) => `${KIND_LABEL[k.kind] ?? k.kind} ${k.active + k.waiting}`).join(" · ") || "—";

			const mode = cfg?.mode ?? "queue";

			return h("div", { style: css.wrap },
				h("div", { style: css.header },
					h("span", { style: { fontSize: "18px", marginRight: "2px" } }, "🛡️"),
					h("h2", { style: css.title }, "并发监控"),
					h("span", { style: { ...css.badge, color: mode === "queue" ? V.ok : V.warn } },
						mode === "queue" ? "排队节制" : "仅监控"),
					h("span", { style: { color: V.dim } },
						`上限 ${max} · 警告 ${warnAt} · 运行 ${fmtUptime(data?.uptimeSec)} · pid ${data?.pid ?? "—"}`),
					h("div", { style: css.meta },
						h("span", { style: { color: connected ? V.ok : V.err } }, connected ? "● 实时" : "● 离线"),
						h("span", null, `更新 ${fmtTs(lastOk)}`),
						h("button", { style: css.btn, onClick: () => patchConfig({ mode: mode === "queue" ? "monitor" : "queue" }) },
							busy ? "…" : `切到${mode === "queue" ? "仅监控" : "排队节制"}`),
						h("button", { style: css.btn, onClick: () => patchConfig({ maxConcurrency: Math.max(1, max - 1) }) }, "上限−"),
						h("button", { style: css.btn, onClick: () => patchConfig({ maxConcurrency: max + 1 }) }, "上限+"),
						h("button", { style: css.btn, onClick: () => patchHistory("clear") }, "🗑 清历史"),
						h("button", { style: css.btn, onClick: () => setPaused((p) => !p) }, paused ? "▶ 继续" : "⏸ 暂停")
					)
				),
				error ? h("div", { style: css.err }, `${error}（每 ${POLL_MS / 1000}s 自动重试）`) : null,

				h("div", { style: css.grid },
					gauge("活跃并发", active, color, `/ ${max} · 峰值 ${g?.peakActive ?? 0}`),
					gauge("排队", g?.waiting ?? 0, (g?.waiting ?? 0) > 0 ? dsw("state-warn-primary", "#eab308") : V.text, `队列深度 ${g?.queueDepth ?? 0}`),
					gauge("已放行", c?.started ?? 0, V.text, `完成 ${c?.completed ?? 0}`),
					gauge("取消", c?.cancelled ?? 0, (c?.cancelled ?? 0) > 0 ? V.warn : V.text, `门闩拦截 ${c?.gateHeld ?? 0}`),
					gauge("fail-open", c?.failOpen ?? 0, (c?.failOpen ?? 0) > 0 ? V.err : V.text, "超时强制放行"),
					gauge("告警", c?.warnHits ?? 0, (c?.warnHits ?? 0) > 0 ? V.warn : V.text, "≥阈值次数")
				),

				h("div", { style: { ...css.card, marginBottom: "4px" } },
					h("div", { style: { display: "flex", justifyContent: "space-between", fontSize: "12px", color: V.dim } },
						h("span", null, "并发水位"),
						h("span", null, `${active}/${max}（${pct}%）`)
					),
					h("div", { style: css.bar },
						h("div", { style: { ...css.barFill, width: pct + "%", background: color } })
					),
					h("div", { style: { fontSize: "11px", color: V.dim, marginTop: "6px" } },
						`在途 ${data?.activeRequests?.length ?? 0}（${kindSummary}）；排队超时 ${cfg?.maxQueueWaitMs ? (cfg.maxQueueWaitMs / 1000).toFixed(0) + "s 强制放行" : "无限等待"}`
					)
				),

				h("div", { style: css.section },
					h("h3", { style: css.sectionTitle }, "在途请求"),
					table(["id", "来源", "provider/model", "用途", "session", "阶段", "已有", "排队等待"], activeRows, "当前无在途模型请求")
				),
				h("div", { style: css.section },
					h("h3", { style: css.sectionTitle }, "按模型 / 按供应商"),
					table(["provider", "model", "活跃", "排队"], modelRows, "无"),
					h("div", { style: { height: "6px" } }),
					table(["provider", "活跃", "排队"], providerRows, "无")
				),
				h("div", { style: css.section },
					h("h3", { style: css.sectionTitle }, `最近完成（${(data?.recent || []).length}，自动清理：条数≤${cfg?.history ?? 30} / TTL ${cfg?.historyTtlMs ? (cfg.historyTtlMs / 3600000).toFixed(1) + "h" : "关"}）`),
					table(["结束于", "来源", "provider/model", "用途", "状态", "耗时"], recentRows, "暂无完成记录")
				)
			);
		}

		// ---------- 客户端插件模块 ----------
		exports.inject = ["slots"];
		exports.apply = (ctx) => {
			try {
				if (!ctx.slots || typeof ctx.slots.inject !== "function") {
					console.warn("[concurrency-guard] client: 无 slots 服务，跳过面板注册");
					return;
				}
				ctx.slots.inject("conversation.view", () =>
					ctx.slots.register({
						name: "conversation.view",
						id: "concurrency-guard-view",
						order: 300,
						label: () => "并发监控"
					}, ConcurrencyPanel)
				);
			} catch (e) {
				console.error("[concurrency-guard] client: 面板注册失败", e);
			}
		};

		return module.exports;
	}
});