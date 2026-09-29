// SPEC §15 — JSONL export/import. Identity-preserving: provided chains are
// verified against recomputation through the ordinary apply path (same-id-
// different-content and mismatches route to the fork path), contig and
// finality rules apply unchanged.

import { assertWriter, validateEntry } from "./codec.js";
import { SeqscribeError } from "./errors.js";
import type { LogCore } from "./log.js";
import type { Store } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type { Constants, FinalityCert, LogEntry, Topic } from "./types.js";

interface ExportHeader {
  seqscribe: "export/v1";
  topic: Topic;
  base: "genesis" | { order: FinalityCert["order"]; cut: FinalityCert["cut"] };
  // Acknowledged retention (host-guide §4.8): per-stream retention floors of
  // the exporting node — rows at or below them are not in the stream. An
  // importer adopts them first so the rows above chain from floorChain
  // instead of parking in sq_pending behind a gap. Absent when no floor
  // exists (byte-identical to a pre-§4.8 export); pre-§4.8 importers ignore it.
  floors?: Record<string, { seq: number; chain: string }>;
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
    const floors = deps.store.floorsForTopic(topic);
    if (floors.length > 0) {
      header.floors = {};
      for (const f of floors) header.floors[f.writer] = { seq: f.seq, chain: f.chain };
    }
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
      if (h.floors !== undefined && typeof h.floors === "object" && h.floors !== null) {
        for (const [writer, f] of Object.entries(h.floors)) {
          if (!Number.isSafeInteger(f?.seq) || f.seq < 1 || typeof f.chain !== "string") continue;
          try {
            assertWriter(writer); // charter (and __proto__) check, as for any entry's writer
          } catch {
            continue;
          }
          await deps.core.adoptFloor(topic, writer, f.seq, f.chain);
        }
      }
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
