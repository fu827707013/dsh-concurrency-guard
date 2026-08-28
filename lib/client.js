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
			err: { border: `1px solid ${V.err}`, borderRadius: "8px", padding: "8px 12px", fontSize: "12px", color: V.err, margin: "8px 0" },
			tabBar: { display: "flex", gap: "4px", borderBottom: `1px solid ${V.border}`, marginTop: "14px", flexWrap: "wrap" },
			tabBtn: { fontSize: "12px", padding: "6px 14px", borderRadius: "8px 8px 0 0", border: `1px solid transparent`, borderBottom: "none", background: "transparent", color: V.dim, cursor: "pointer", marginBottom: "-1px" },
			tabBtnActive: { background: V.panel, borderColor: V.border, color: V.text, fontWeight: 600 }
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

		// ---------- 主面板 ----------
		function ConcurrencyPanel() {
			const [data, setData] = useState(null);
			const [error, setError] = useState(null);
			const [paused, setPaused] = useState(false);
			const [tab, setTab] = useState("stats");
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
			const errDetailRows = st?.errorDetails && Object.keys(st.errorDetails).length
				? Object.entries(st.errorDetails)
					.sort((a, b) => (b[1].count - a[1].count) || (b[1].lastAt - a[1].lastAt))
					.slice(0, 20)
					.map(([key, d]) => {
						const cnt = d.count ?? 1;
						let info = d.message ?? key;
						if (d.code && !info.includes(`(code=${d.code})`)) info = `${info} (code=${d.code})`;
						// LlmFailure 协议内补充诊断：HTTP 状态 / 供应商 requestId / Retry-After
						const metaBits = [];
						if (d.status) metaBits.push(`HTTP ${d.status}`);
						if (d.requestId) metaBits.push(`req:${d.requestId}`);
						if (d.retryAfterMs) metaBits.push(`retry-after ${Math.round(d.retryAfterMs / 1000)}s`);
						if (metaBits.length) info = `${info} [${metaBits.join(" · ")}]`;
						const sid = d.lastSessionId ?? null;
						const sessSummary = [
							`最近: ${sid || "(无会话)"}`,
							`模型: ${d.lastProvider ?? "?"}/${d.lastModel ?? "?"}`,
							...Object.entries(d.sessions ?? {})
								.sort((a, b) => b[1] - a[1])
								.map(([s, c]) => `${s === "(none)" ? "(无会话)" : s} × ${c}`)
						].join("\n");
						const sessKeys = Object.keys(d.sessions ?? {});
						let sessCell;
						if (sessKeys.length > 1) sessCell = { text: `${sessKeys.length} 个会话`, color: V.warn, title: sessSummary };
						else if (sid) sessCell = { text: shortId(sid), color: V.text, title: sessSummary };
						else if (sessKeys.length === 1 && sessKeys[0] !== "(none)") sessCell = { text: shortId(sessKeys[0]), color: V.text, title: sessSummary };
						else sessCell = { text: "—", color: V.dim, title: sessSummary };
						return [
							{ text: String(cnt), color: cnt >= 3 ? V.err : cnt === 2 ? V.warn : V.dim, title: cnt > 1 ? `累计出现 ${cnt} 次` : "出现 1 次" },
							{ text: ERR_LABEL[d.kind] ?? d.kind ?? "未知", color: V.dim },
							{ ...sessCell, style: { maxWidth: "120px", overflow: "hidden", textOverflow: "ellipsis" } },
							{ text: info.length > 140 ? info.slice(0, 140) + "…" : info, color: d.code ? dsw("state-warn-primary", "#eab308") : V.text, title: info, style: { maxWidth: "460px", overflow: "hidden", textOverflow: "ellipsis" } },
							{ text: fmtTs(d.lastAt), color: V.dim }
						];
					})
				: [];
			// 按会话汇总（反向聚合 errorDetails.sessions）：会话 → 该会话出现过的错误 × 次数
			const bySessionErrRows = (() => {
				const map = new Map();
				for (const [key, d] of Object.entries(st?.errorDetails ?? {})) {
					for (const [sidRaw, n] of Object.entries(d.sessions ?? {})) {
						const sid = sidRaw === "(none)" ? null : sidRaw;
						let row = map.get(sid);
						if (!row) { row = { sessionId: sid, total: 0, kinds: new Map(), errors: [] }; map.set(sid, row); }
						row.total += n;
						row.kinds.set(d.kind ?? "unknown", (row.kinds.get(d.kind ?? "unknown") ?? 0) + n);
						row.errors.push({ msg: d.message ?? key, code: d.code, status: d.status, requestId: d.requestId, count: n });
					}
				}
				return [...map.values()]
					.sort((a, b) => b.total - a.total)
					.slice(0, 20)
					.map((r) => {
						const kinds = [...r.kinds.entries()].sort((a, b) => b[1] - a[1]).map(([k, n]) => `${ERR_LABEL[k] ?? k}×${n}`).join(" ");
						const sorted = [...r.errors].sort((a, b) => b.count - a.count);
						const errSummary = sorted.map((e) => {
							const m = e.msg.length > 60 ? e.msg.slice(0, 60) + "…" : e.msg;
							return `${m} ×${e.count}`;
						}).join("；");
						const errTitle = sorted.map((e) => {
							const bits = [e.msg];
							if (e.code) bits.push(`code=${e.code}`);
							if (e.status) bits.push(`HTTP ${e.status}`);
							if (e.requestId) bits.push(`req:${e.requestId}`);
							return `${bits.join(" ")} ×${e.count}`;
						}).join("\n");
						return [
							{ text: r.sessionId ? shortId(r.sessionId) : "(无会话)", color: r.sessionId ? V.text : V.dim, title: `完整会话 id: ${r.sessionId ?? "无"}\n异常总数: ${r.total}`, style: { maxWidth: "120px", overflow: "hidden", textOverflow: "ellipsis" } },
							{ text: String(r.total), color: r.total >= 3 ? V.err : r.total === 2 ? V.warn : V.dim, title: `累计 ${r.total} 次` },
							{ text: kinds || "—", color: V.dim },
							{ text: errSummary, color: V.text, title: errTitle, style: { maxWidth: "520px", overflow: "hidden", textOverflow: "ellipsis" } }
						];
					});
			})();
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
					h("h3", { style: css.sectionTitle }, `异常明细（全历史累计 · 按错误信息聚类 · 最多显示 20 条）`),
					table(["次数", "分类", "会话", "错误信息", "最近出现"], errDetailRows, "暂无异常记录（发生过的异常会按错误信息聚合在此，含错误码与会话）"),
					h("h3", { style: { ...css.sectionTitle, marginTop: "16px" } }, `按会话汇总（会话 → 该会话出现过的错误 × 次数 · 最多显示 20 个会话）`),
					table(["会话", "异常次数", "分类", "错误信息(×次数)"], bySessionErrRows, "暂无会话维度数据（会话上下文自 v1.3.5 起采集：重启宿主后，新发生的异常才会带会话信息；旧异常无会话可追溯）")
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