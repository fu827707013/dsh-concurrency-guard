/**
 * 请求来源分类。
 *
 * 依据（按优先级）：
 *  1. purpose：'compaction' → 压缩；'session-title' → 标题（核心辅助请求，带显式用途标签）。
 *  2. agent loop 标记（@deepseek-ai/dsh-llm 的 isAgentLoopRequest）：主会话与子代理
 *     的请求都由 agent loop 构造并标记；二者以 sessionId 形状区分——
 *       主会话 sessionId 以 "session-" 开头；子代理 sessionId 即 agent id（裸 UUID）。
 *  3. 非 loop 请求（插件自调 ctx.llm.stream，如 modlens 转发、super-injector 守护 agent）
 *     → 'plugin'。
 *
 * 注意：当 dsh-llm 不可导入（隔离环境/兼容降级）时，isLoop 为 null，退化为纯启发式：
 *   sessionId "session-" 前缀 → main；有 sessionId → subagent；无 → plugin。
 */
export const KIND_LABELS = Object.freeze({
  main: "主会话",
  subagent: "子代理",
  plugin: "插件",
  compaction: "压缩",
  "session-title": "标题",
  unknown: "未知"
});

/**
 * @param {object} options - llm/stream 瀑布收到的请求包（GenerateOptions 形状）。
 * @param {((request: object) => boolean)|null} isLoop - dsh-llm 的 isAgentLoopRequest，
 *   null 表示不可用（启发式降级）。
 * @returns {keyof typeof KIND_LABELS}
 */
export function classifyKind(options, isLoop) {
  if (options.purpose === "compaction") return "compaction";
  if (options.purpose === "session-title") return "session-title";

  const sid = options.sessionId;
  const isAgent = isLoop ? isLoop(options) : null;

  if (isAgent === true) {
    // loop 构造：主会话 vs 子代理（sessionId 形状启发式）
    return typeof sid === "string" && sid.startsWith("session-") ? "main" : "subagent";
  }
  if (isAgent === false) return "plugin";

  // 降级启发式
  if (typeof sid === "string" && sid.startsWith("session-")) return "main";
  if (typeof sid === "string" && sid !== "") return "subagent";
  return "plugin";
}