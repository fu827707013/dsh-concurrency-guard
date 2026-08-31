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
		const ERR_LABEL = { rate_limit: "限流", timeout: "超时", network: "网络", aborted: "中断", auth: "鉴权", provider: "上游", other: "其他", unknown: "未知" };
		const PHASE_LABEL = { streaming: "流式中", waiting: "等待" };
		const STATUS_LABEL = { ok: "完成", cancelled: "取消", error: "错误", interrupted: "中断", waiting: "等待" };
		const dsw = (name, fb) => `var(--dsw-alias-${name}, ${fb})`;

		// ---------- 工具 ----------
		const fmtAge = (ms) => (ms >= 1000 ? `${(ms / 1000).toFixed(ms < 10000 ? 1 : 0)}s` : `${ms}ms`);
		const fmtTs = (ts) => (ts ? new Date(ts).toLocaleTimeString("zh-CN", { hour12: false }) : "—");
		const fmtUptime = (sec) => {
			if (!Number.isFinite(sec)) return "—";
			const m = Math.floor(sec / 60);
			return m >= 60 ? `${Math.floor(m / 60)}h${m % 60}m` : `${m}m${sec % 60}s`;
		};
		const shortId = (id) => {
			const s = String(id || "");
			// 主会话 "session-<uuid>"：去掉前缀再截，避免显示成 "session-"（前缀把本体挤掉）
			return (s.startsWith("session-") ? s.slice(8) : s).slice(0, 8);
		};
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
			err: { border: `1px solid ${V.err}`, borderRadius: "8px", padding: "8px 12px", fontSize: "12px", color: V.err, margin: "8px 0" },
			tabBar: { display: "flex", gap: "4px", borderBottom: `1px solid ${V.border}`, marginTop: "14px", flexWrap: "wrap" },
			tabBtn: { fontSize: "12px", padding: "6px 14px", borderRadius: "8px 8px 0 0", border: `1px solid transparent`, borderBottom: "none", background: "transparent", color: V.dim, cursor: "pointer", marginBottom: "-1px" },
			tabBtnActive: { background: V.panel, borderColor: V.border, color: V.text, fontWeight: 600 },
			modalOverlay: { position: "fixed", inset: "0", background: "rgba(0,0,0,.55)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000, padding: "20px" },
			modalCard: { background: V.panel, border: `1px solid ${V.border}`, borderRadius: "12px", maxWidth: "600px", width: "100%", maxHeight: "86vh", overflow: "auto", padding: "18px 22px", boxShadow: "0 12px 44px rgba(0,0,0,.5)" },
			modalClose: { border: "none", background: "transparent", color: V.dim, fontSize: "18px", cursor: "pointer", lineHeight: "1", padding: "2px 6px" }
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
						h("tr", { key: ri }, row.map((cell, ci) => {
							const c = cell && typeof cell === "object" ? cell : { text: cell };
							return h("td", {
								key: ci,
								title: c.title,
								style: { ...css.td, ...(c.color ? { color: c.color } : {}), ...(c.style || {}) }
							}, c.text);
						}))
					))
				)
			);
		};

		// 配置弹窗字段定义（每项带说明）
		const CFG_FIELDS = [
			{ key: "maxConcurrency", label: "并发上限", unit: "个", desc: "供应商/中继允许的最大同时模型请求数；达到后新请求进入 FIFO 排队，防止并发超限被锁号" },
			{ key: "warnAt", label: "告警阈值", unit: "个", desc: "活跃并发达到该值时记 warn 日志并在面板高亮（建议略低于并发上限）" },
			{ key: "maxQueueWaitSec", label: "排队超时", unit: "秒", desc: "请求排队最长等待时间；超时强制放行（fail-open，瞬时并发可能超限）；0 = 无限等待" },
			{ key: "history", label: "历史条数", unit: "条", desc: "「最近完成」列表保留条数，超出自动截断" },
			{ key: "historyTtlMin", label: "历史TTL", unit: "分钟", desc: "完成记录超龄自动清理；0 = 关闭（只靠条数上限）" },
			{ key: "maxStreamStallMin", label: "停滞判死", unit: "分钟", desc: "流式请求无输出超过该时长视为消费端已弃流，记入中断并释放并发位" }
		];

		const cfgField = (label, unit, desc, value, onChange) =>
			h("div", { style: { marginBottom: "14px" } },
				h("div", { style: { display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: "3px" } },
					h("span", { style: { fontSize: "13px", color: V.text, fontWeight: 600 } }, label),
					h("span", { style: { fontSize: "11px", color: V.dim } }, `单位：${unit}`)
				),
				h("div", { style: { fontSize: "11px", color: V.dim, lineHeight: "1.5", marginBottom: "5px" } }, desc),
				h("input", {
					type: "number",
					value: value === null || value === void 0 ? "" : value,
					onChange: (e) => onChange(e.target.value),
					style: { width: "100%", boxSizing: "border-box", background: V.panel, color: V.text, border: `1px solid ${V.border}`, borderRadius: "6px", padding: "6px 10px", fontSize: "13px", fontVariantNumeric: "tabular-nums" }
				})
			);

		// ---------- 主面板 ----------
		function ConcurrencyPanel() {
			const [data, setData] = useState(null);
			const [error, setError] = useState(null);
			const [paused, setPaused] = useState(false);
			const [tab, setTab] = useState("stats");
			const [showCfg, setShowCfg] = useState(false);
			const [draft, setDraft] = useState(null);
			const [cfgBusy, setCfgBusy] = useState(false);
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
			const nowMs = Date.now();
			const sessionRows = (data?.bySession || []).map((s) => {
				const act = s.active ?? 0;
				const lastAct = Math.max(s.lastStartMs || 0, s.lastEndMs || 0);
				const ago = lastAct ? nowMs - lastAct : null;
				const stateCell = act > 0
					? { text: "● 请求中", color: V.ok }
					: ago !== null && ago < 30_000
						? { text: `${Math.max(1, Math.round(ago / 1000))}s 前活跃`, color: V.ok }
						: ago !== null
							? { text: `${Math.max(1, Math.round(ago / 60000))}m 前活跃`, color: V.dim }
							: { text: "—", color: V.dim };
				return [
					{ text: s.sessionId ? shortId(s.sessionId) : "(无会话)", color: V.text },
					{ text: s.kind ? (KIND_LABEL[s.kind] ?? s.kind) : "—", color: kindColor(s.kind) },
					{ text: String(act), color: act > 0 ? V.ok : V.dim },
					{ text: String(s.waiting ?? 0), color: (s.waiting ?? 0) > 0 ? V.warn : V.dim },
					stateCell,
					{ text: fmtTs(s.lastStartMs), color: V.dim },
					{ text: (s.recentCount ?? 0) > 0 ? String(s.recentCount) : "—", color: (s.recentCount ?? 0) > 0 ? V.text : V.dim }
				];
			});

			const st = data?.stats;
			const todaySt = st?.today;
			const ellipsis = { overflow: "hidden", textOverflow: "ellipsis" };
			// ── 报表①：错误汇总（按 会话×错误 分组）——数据源 errorDetails（全历史聚合）──
			const errSummaryRows = (() => {
				const rows = [];
				for (const [key, d] of Object.entries(st?.errorDetails ?? {})) {
					let info = d.message ?? key;
					if (d.code && !info.includes(`(code=${d.code})`)) info = `${info} (code=${d.code})`;
					const metaBits = [];
					if (d.status) metaBits.push(`HTTP ${d.status}`);
					if (d.requestId) metaBits.push(`req:${d.requestId}`);
					if (d.retryAfterMs) metaBits.push(`retry-after ${Math.round(d.retryAfterMs / 1000)}s`);
					if (metaBits.length) info = `${info} [${metaBits.join(" · ")}]`;
					const hasCode = !!d.code;
					const sourceKind = d.lastSourceKind ?? null;
					const sessEntries = Object.entries(d.sessions ?? {});
					if (!sessEntries.length) {
						// 旧数据（v1.3.5 前，无会话维度）→ 归入 "(无会话)" 一行，次数 = 全量
						rows.push({ sessionId: null, sourceKind, kind: d.kind ?? "unknown", info, hasCode, count: d.count ?? 1, lastAt: d.lastAt ?? 0 });
					} else {
						for (const [sid, sc] of sessEntries) {
							const c = typeof sc === "number" ? sc : (sc?.count ?? 1);
							const lastAt = typeof sc === "number" ? (d.lastAt ?? 0) : (sc?.lastAt ?? d.lastAt ?? 0);
							rows.push({ sessionId: sid === "(none)" ? null : sid, sourceKind, kind: d.kind ?? "unknown", info, hasCode, count: c, lastAt });
						}
					}
				}
				return rows
					// 排序：先按会话分组（相同会话在一起，无会话组排最后），组内按最近出现降序
					.sort((a, b) => {
						const ga = a.sessionId ?? "";
						const gb = b.sessionId ?? "";
						if (!ga && gb) return 1;
						if (ga && !gb) return -1;
						if (ga === gb) return b.lastAt - a.lastAt;
						return ga.localeCompare(gb);
					})
					.slice(0, 50)
					.map((r) => [
						{ text: r.sessionId ? shortId(r.sessionId) : "(无会话)", color: r.sessionId ? V.text : V.dim, title: `完整会话 id: ${r.sessionId ?? "无"}`, style: { maxWidth: "120px", ...ellipsis } },
						{ text: KIND_LABEL[r.sourceKind] ?? "—", color: kindColor(r.sourceKind), title: `来源: ${KIND_LABEL[r.sourceKind] ?? r.sourceKind ?? "未知"}` },
						{ text: ERR_LABEL[r.kind] ?? r.kind ?? "未知", color: V.dim },
						{ text: String(r.count), color: r.count >= 3 ? V.err : r.count === 2 ? V.warn : V.dim, title: `累计 ${r.count} 次` },
						{ text: r.info.length > 120 ? r.info.slice(0, 120) + "…" : r.info, color: r.hasCode ? dsw("state-warn-primary", "#eab308") : V.text, title: r.info, style: { maxWidth: "440px", ...ellipsis } },
						{ text: fmtTs(r.lastAt), color: V.dim, title: `最近出现: ${new Date(r.lastAt).toLocaleString("zh-CN")}` }
					]);
			})();
			// ── 报表②：错误明细（每次一条）——数据源 errorEvents（最近 200 条，滚动）──
			// 排序：先按会话分组（id 排序），组内按时间升序
			const errEventRows = (() => {
				const bySess = new Map();
				for (const e of st?.errorEvents || []) {
					const k = e.sessionId ?? "(none)";
					const arr = bySess.get(k);
					if (arr) arr.push(e); else bySess.set(k, [e]);
				}
				const ordered = [];
				for (const k of [...bySess.keys()].sort()) {
					ordered.push(...bySess.get(k).sort((a, b) => a.at - b.at));
				}
				return ordered.map((e) => {
					let info = e.message ?? "";
					if (e.code && !info.includes(`(code=${e.code})`)) info = `${info} (code=${e.code})`;
					const metaBits = [];
					if (e.status) metaBits.push(`HTTP ${e.status}`);
					if (e.requestId) metaBits.push(`req:${e.requestId}`);
					if (metaBits.length) info = `${info} [${metaBits.join(" · ")}]`;
					const title = [
						`完整会话 id: ${e.sessionId ?? "无"}`,
						`来源: ${KIND_LABEL[e.sourceKind] ?? e.sourceKind ?? "未知"}`,
						`模型: ${e.provider ?? "?"}/${e.model ?? "?"}`,
						`时间: ${new Date(e.at).toLocaleString("zh-CN")}`,
						info
					].join("\n");
					return [
						{ text: e.sessionId ? shortId(e.sessionId) : "(无会话)", color: e.sessionId ? V.text : V.dim, title: `完整会话 id: ${e.sessionId ?? "无"}\n来源: ${KIND_LABEL[e.sourceKind] ?? e.sourceKind ?? "未知"}\n模型: ${e.provider ?? "?"}/${e.model ?? "?"}`, style: { maxWidth: "120px", ...ellipsis } },
						{ text: KIND_LABEL[e.sourceKind] ?? "—", color: kindColor(e.sourceKind) },
						{ text: ERR_LABEL[e.kind] ?? e.kind ?? "未知", color: V.dim },
						{ text: info.length > 200 ? info.slice(0, 200) + "…" : info, color: e.code ? dsw("state-warn-primary", "#eab308") : V.text, title, style: { maxWidth: "540px", ...ellipsis } },
						{ text: fmtTs(e.at), color: V.dim, title: `时间: ${new Date(e.at).toLocaleString("zh-CN")}` }
					];
				});
			})();
			const errEventCount = (st?.errorEvents || []).length;
			// 异常明细 tab 顶部统计
			const errStatsChips = (() => {
				const tot = st?.totals ?? {};
				const tdy = st?.today ?? {};
				const sessions = new Set();
				for (const d of Object.values(st?.errorDetails ?? {})) {
					for (const s of Object.keys(d.sessions ?? {})) sessions.add(s === "(none)" ? "(无会话)" : s);
				}
				for (const e of st?.errorEvents || []) sessions.add(e.sessionId ?? "(无会话)");
				return [
					["总异常", tot.errored ?? 0, (tot.errored ?? 0) > 0 ? V.err : V.dim],
					["今日异常", tdy.errored ?? 0, (tdy.errored ?? 0) > 0 ? V.err : V.dim],
					["涉及会话", sessions.size, sessions.size > 0 ? V.text : V.dim],
					["明细事件", errEventCount, errEventCount > 0 ? V.text : V.dim]
				];
			})();
			const errKindTotalLine = st?.totals?.byErrKind && Object.keys(st.totals.byErrKind).length
				? Object.entries(st.totals.byErrKind).sort((a, b) => b[1] - a[1]).map(([k, n]) => `${ERR_LABEL[k] ?? k} ${n}`).join(" · ")
				: "—";
			const errBreakdown = st && todaySt?.byErrKind && Object.keys(todaySt.byErrKind).length
				? Object.entries(todaySt.byErrKind).sort((a, b) => b[1] - a[1]).map(([k, n]) => `${ERR_LABEL[k] ?? k} ${n}`).join(" · ")
				: "无";
			const kindBreakdown = st && todaySt?.byKind && Object.keys(todaySt.byKind).length
				? Object.entries(todaySt.byKind).sort((a, b) => b[1] - a[1]).map(([k, n]) => `${KIND_LABEL[k] ?? k} ${n}`).join(" · ")
				: "—";
			const statChips = st ? [
				["请求", todaySt?.started ?? 0, V.text],
				["完成", todaySt?.completed ?? 0, V.ok],
				["异常", todaySt?.errored ?? 0, V.err],
				["中断", todaySt?.interrupted ?? 0, (todaySt?.interrupted ?? 0) > 0 ? dsw("state-warn-primary", "#eab308") : V.dim],
				["取消", todaySt?.cancelled ?? 0, V.warn],
				["门闩", todaySt?.gateHeld ?? 0, (todaySt?.gateHeld ?? 0) > 0 ? V.warn : V.dim],
				["fail-open", todaySt?.failOpen ?? 0, (todaySt?.failOpen ?? 0) > 0 ? V.err : V.dim]
			] : [];
			const dayRows = [];
			if (st?.todayKey) {
				const d = todaySt;
				dayRows.push([
					{ text: `今天 ${st.todayKey}`, color: V.text },
					String(d?.started ?? 0),
					{ text: String(d?.completed ?? 0), color: (d?.completed ?? 0) > 0 ? V.ok : V.dim },
					{ text: String(d?.errored ?? 0), color: (d?.errored ?? 0) > 0 ? V.err : V.dim },
					{ text: String(d?.interrupted ?? 0), color: (d?.interrupted ?? 0) > 0 ? dsw("state-warn-primary", "#eab308") : V.dim },
					{ text: String(d?.cancelled ?? 0), color: (d?.cancelled ?? 0) > 0 ? V.warn : V.dim },
					{ text: d?.byErrKind && Object.keys(d.byErrKind).length ? Object.keys(d.byErrKind).map((k) => ERR_LABEL[k] ?? k).join("/") : "—", color: V.dim }
				]);
			}
			for (const [date, d] of Object.entries(st?.byDay ?? {}).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 6)) {
				dayRows.push([
					{ text: date, color: V.dim },
					String(d.started ?? 0),
					{ text: String(d.completed ?? 0), color: (d.completed ?? 0) > 0 ? V.ok : V.dim },
					{ text: String(d.errored ?? 0), color: (d.errored ?? 0) > 0 ? V.err : V.dim },
					{ text: String(d.interrupted ?? 0), color: (d.interrupted ?? 0) > 0 ? dsw("state-warn-primary", "#eab308") : V.dim },
					{ text: String(d.cancelled ?? 0), color: (d.cancelled ?? 0) > 0 ? V.warn : V.dim },
					{ text: d.byErrKind && Object.keys(d.byErrKind).length ? Object.keys(d.byErrKind).map((k) => ERR_LABEL[k] ?? k).join("/") : "—", color: V.dim }
				]);
			}

			const mode = cfg?.mode ?? "queue";
			const TABS = [
				["stats", "今日统计"],
				["errors", "异常明细"],
				["inflight", "在途 / 模型"],
				["sessions", "会话活跃"],
				["recent", "最近完成"]
			];

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
						h("button", { style: showCfg ? { ...css.btnPrimary } : css.btn, onClick: () => {
							if (!showCfg && cfg) {
								setDraft({
									mode: cfg.mode ?? "queue",
									maxConcurrency: cfg.maxConcurrency ?? 5,
									warnAt: cfg.warnAt ?? 4,
									maxQueueWaitSec: Math.round((cfg.maxQueueWaitMs ?? 300000) / 1000),
									history: cfg.history ?? 30,
									historyTtlMin: Math.round((cfg.historyTtlMs ?? 3600000) / 60000),
									maxStreamStallMin: Math.round((cfg.maxStreamStallMs ?? 600000) / 60000)
								});
							}
							setShowCfg((v) => !v);
						} }, showCfg ? "✕" : "⚙ 配置"),
						h("button", { style: css.btn, onClick: () => {
							if (window.confirm("确认清零全部计数、统计与历史？")) patchHistory("reset");
						} }, "↺ 重置"),
						h("button", { style: css.btn, onClick: () => patchHistory("clear") }, "🗑 清历史"),
						h("button", { style: css.btn, onClick: () => setPaused((p) => !p) }, paused ? "▶ 继续" : "⏸ 暂停")
					)
				),
				error ? h("div", { style: css.err }, `${error}（每 ${POLL_MS / 1000}s 自动重试）`) : null,

				showCfg && draft ? h("div", { style: css.modalOverlay, onClick: () => setShowCfg(false) },
					h("div", { style: css.modalCard, onClick: (e) => e.stopPropagation() },
						h("div", { style: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "14px" } },
							h("div", null,
								h("div", { style: { fontSize: "15px", fontWeight: 700, color: V.text } }, "⚙ 并发监控配置"),
								h("div", { style: { fontSize: "11px", color: V.dim, marginTop: "2px" } }, "保存后立即生效并持久化（state.json，重启保留）")
							),
							h("button", { style: css.modalClose, onClick: () => setShowCfg(false), title: "关闭" }, "✕")
						),
						h("div", { style: { marginBottom: "14px" } },
							h("div", { style: { fontSize: "13px", color: V.text, fontWeight: 600, marginBottom: "3px" } }, "运行模式"),
							h("div", { style: { fontSize: "11px", color: V.dim, lineHeight: "1.5", marginBottom: "5px" } }, "排队节制（默认）：并发达到上限时新请求 FIFO 排队；仅监控：只统计不拦截"),
							h("div", { style: { display: "flex", gap: "8px" } },
								h("button", { style: draft.mode === "queue" ? css.btnPrimary : css.btn, onClick: () => setDraft({ ...draft, mode: "queue" }) }, "排队节制"),
								h("button", { style: draft.mode === "monitor" ? css.btnPrimary : css.btn, onClick: () => setDraft({ ...draft, mode: "monitor" }) }, "仅监控")
							)
						),
						CFG_FIELDS.map((f) => cfgField(f.label, f.unit, f.desc, draft[f.key], (v) => setDraft({ ...draft, [f.key]: v }))),
						h("div", { style: { display: "flex", gap: "8px", alignItems: "center", marginTop: "6px", borderTop: `1px solid ${V.border1}`, paddingTop: "14px" } },
							h("button", { style: css.btnPrimary, disabled: cfgBusy, onClick: async () => {
								setCfgBusy(true);
								try {
									await patchConfig({
										mode: draft.mode === "monitor" ? "monitor" : "queue",
										maxConcurrency: Math.max(1, Math.floor(Number(draft.maxConcurrency) || 5)),
										warnAt: Math.max(0, Math.floor(Number(draft.warnAt) || 0)),
										maxQueueWaitMs: Math.max(0, Math.floor(Number(draft.maxQueueWaitSec) || 0) * 1000),
										history: Math.max(0, Math.floor(Number(draft.history) || 0)),
										historyTtlMs: Math.max(0, Math.floor(Number(draft.historyTtlMin) || 0) * 60000),
										maxStreamStallMs: Math.max(1000, Math.floor(Number(draft.maxStreamStallMin) || 10) * 60000)
									});
									setShowCfg(false);
								} finally {
									setCfgBusy(false);
								}
							} }, cfgBusy ? "保存中…" : "保存配置"),
							h("button", { style: css.btn, onClick: () => setShowCfg(false) }, "取消"),
							h("span", { style: { fontSize: "11px", color: V.dim, marginLeft: "auto" } }, "也可在 Settings → Plugins → Plugin configuration 编辑")
						)
					)
				) : null,

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

				h("div", { style: css.tabBar },
					TABS.map(([key, label]) =>
						h("button", {
							key,
							style: tab === key ? { ...css.tabBtn, ...css.tabBtnActive } : css.tabBtn,
							onClick: () => setTab(key)
						}, label)
					)
				),

				tab === "stats" ? h("div", { style: css.section },
					h("h3", { style: css.sectionTitle }, `今日统计（${st?.todayKey ?? "—"}）· 持久化，重启保留`),
					h("div", { style: { display: "flex", flexWrap: "wrap", gap: "8px", marginBottom: "8px" } },
						statChips.map(([k, v, col]) => h("span", { style: { border: `1px solid ${V.border}`, padding: "2px 10px", borderRadius: "999px", fontSize: "12px", color: col } }, `${k} ${v}`))
					),
					h("div", { style: { fontSize: "11px", color: V.dim, marginBottom: "10px" } },
						`异常分类：${errBreakdown} ｜ 来源：${kindBreakdown}`
					),
					table(["日期", "请求", "完成", "异常", "中断", "取消", "异常分类"], dayRows, "暂无每日记录")
				) : null,

				tab === "errors" ? h("div", { style: css.section },
					h("div", { style: { display: "flex", flexWrap: "wrap", gap: "8px", marginBottom: "8px" } },
						errStatsChips.map(([k, v, col]) => h("span", { style: { border: `1px solid ${V.border}`, padding: "2px 10px", borderRadius: "999px", fontSize: "12px", color: col } }, `${k} ${v}`)),
						h("span", { style: { border: `1px solid ${V.border}`, padding: "2px 10px", borderRadius: "999px", fontSize: "12px", color: V.dim } }, `分类分布：${errKindTotalLine}`)
					),
					h("h3", { style: css.sectionTitle }, `① 错误汇总（按 会话 × 错误信息 分组 · 全历史累计 · 最多 50 行）`),
					table(["会话", "来源", "分类", "次数", "错误信息", "最近出现"], errSummaryRows, "暂无异常记录（发生过的异常会按 会话×错误 聚合在此，含错误码/HTTP status/requestId）"),
					h("h3", { style: { ...css.sectionTitle, marginTop: "16px" } }, `② 错误明细（每次错误一条 · 按会话+时间排序 · 最近 ${errEventCount} / 200 条滚动窗口）`),
					table(["会话", "来源", "分类", "详细错误信息", "时间"], errEventRows, "暂无明细（逐条事件自 v1.3.9 起采集：重启宿主后新错误才会逐条记录；旧错误仅汇总无明细）")
				) : null,

				tab === "inflight" ? h("div", { style: css.section },
					h("h3", { style: css.sectionTitle }, "在途请求"),
					table(["id", "来源", "provider/model", "用途", "session", "阶段", "已有", "排队等待"], activeRows, "当前无在途模型请求")
				) : null,
				tab === "sessions" ? h("div", { style: css.section },
					h("h3", { style: css.sectionTitle }, "会话活跃（近 60s 完成数）"),
					table(["会话", "主要来源", "在途", "排队", "状态", "最近开始", "60s完成"], sessionRows, "暂无会话活动")
				) : null,
				tab === "inflight" ? h("div", { style: css.section },
					h("h3", { style: css.sectionTitle }, "按模型 / 按供应商"),
					table(["provider", "model", "活跃", "排队"], modelRows, "无"),
					h("div", { style: { height: "6px" } }),
					table(["provider", "活跃", "排队"], providerRows, "无")
				) : null,
				tab === "recent" ? h("div", { style: css.section },
					h("h3", { style: css.sectionTitle }, `最近完成（${(data?.recent || []).length}，自动清理：条数≤${cfg?.history ?? 30} / TTL ${cfg?.historyTtlMs ? (cfg.historyTtlMs / 3600000).toFixed(1) + "h" : "关"}）`),
					table(["结束于", "来源", "provider/model", "用途", "状态", "耗时"], recentRows, "暂无完成记录")
				) : null
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