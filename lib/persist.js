/**
 * 状态文件持久化：把最新快照防抖写入磁盘，供终端 tail / 其它工具读取。
 *
 * - 写盘触发：任一状态变更后 250ms 防抖合并（高频变更期间只写一次）。
 * - 卸载/断纤时 flushSync() 立即写一次最终快照。
 * - 写入失败只记 warn，绝不影响主流程。
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { pruneHistoryByTtl, snapshot } from "./records.js";

export function createPersister(state, logger) {
  let timer = null;
  let scheduled = false;

  const writeNow = () => {
    scheduled = false;
    try {
      // 落盘前顺带做时间 TTL 清理：即使没有新请求，过期历史也会被清掉
      pruneHistoryByTtl(state);
      const file = state.cfg.stateFile;
      mkdirSync(dirname(file), { recursive: true });
      writeFileSync(file, JSON.stringify(snapshot(state, false), null, 2), "utf8");
    } catch (error) {
      logger.warn(`[concurrency-guard] 状态文件写入失败: ${String(error?.message ?? error)}`);
    }
  };

  /** 防抖排程写盘（幂等：已有排程则跳过）。 */
  const schedule = () => {
    if (scheduled) return;
    scheduled = true;
    timer = setTimeout(writeNow, 250);
  };

  /** 立即写盘并取消防抖排程（卸载/断纤时调用）。 */
  const flushSync = () => {
    if (timer !== null) clearTimeout(timer);
    writeNow();
  };

  return { schedule, flushSync };
}