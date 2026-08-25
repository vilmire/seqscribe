// SPEC §15 — JSONL export/import. Identity-preserving: provided chains are
// verified against recomputation through the ordinary apply path (same-id-
// different-content and mismatches route to the fork path), contig and
// finality rules apply unchanged.

import { validateEntry } from "./codec.js";
import { SeqscribeError } from "./errors.js";
import type { LogCore } from "./log.js";
import type { Store } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type { Constants, FinalityCert, LogEntry, Topic } from "./types.js";

interface ExportHeader {
  seqscribe: "export/v1";
  topic: Topic;
  base: "genesis" | { order: FinalityCert["order"]; cut: FinalityCert["cut"] };
}

export interface ExportDeps {
  core: LogCore;
  store: Store;
  topics: TopicRegistry;
  constants: Constants;
}

const EXPORT_BATCH = 500;

export function exportTopic(deps: ExportDeps, topic: Topic): AsyncIterable<string> {
  deps.topics.get(topic);
  return (async function* () {
    // partial exports chain from the declared cut base; with no pruning yet the
    // base is genesis unless some writer's earliest held seq is above 1
    const cert = deps.core.getCert(topic);
    let base: ExportHeader["base"] = "genesis";
    if (cert) {
      for (const writer of deps.store.topicWriters(topic)) {
        const first = deps.store.entriesRange(topic, writer, 1, 1);
        if (first.length === 0) {
          base = { order: cert.order, cut: cert.cut };
          break;
        }
      }
    }
    const header: ExportHeader = { seqscribe: "export/v1", topic, base };
    yield JSON.stringify(header);
    let afterRowid = 0;
    for (;;) {
      const rows = deps.store.entriesForTopicFromRowid(topic, afterRowid, EXPORT_BATCH);
      if (rows.length === 0) return;
      for (const { entry, rowid } of rows) {
        yield JSON.stringify(entry);
        afterRowid = rowid;
      }
    }
  })();
}

export async function importTopic(
  deps: ExportDeps,
  topic: Topic,
  lines: AsyncIterable<string>,
): Promise<number> {
  deps.topics.get(topic);
  let header: ExportHeader | null = null;
  let applied = 0;
  const pending: Promise<unknown>[] = [];
  for await (const line of lines) {
    const trimmed = line.trim();
    if (trimmed === "") continue;
    if (header === null) {
      const h = JSON.parse(trimmed) as ExportHeader;
      if (h.seqscribe !== "export/v1")
        throw new SeqscribeError("ERR_ENTRY_ENCODING", "not a seqscribe export/v1 stream");
      if (h.topic !== topic)
        throw new SeqscribeError("ERR_MISUSE", `export is for topic ${h.topic}, not ${topic}`);
      header = h;
      continue;
    }
    const entry = validateEntry(JSON.parse(trimmed) as LogEntry, deps.constants);
    if (entry.topic !== topic)
      throw new SeqscribeError("ERR_ENTRY_ENCODING", "entry topic differs from export topic");
    pending.push(
      deps.core.applyExternal(entry, "import").then((r) => {
        if (r === "applied") applied++;
      }),
    );
  }
  if (header === null) throw new SeqscribeError("ERR_ENTRY_ENCODING", "empty export stream");
  await Promise.all(pending);
  return applied;
}
