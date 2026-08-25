// SPEC §14 CI gate: a fixture that ACTUALLY imports from "seqscribe" (via Node
// package self-reference through the exports map) — a bare tsc pass over the
// sources does not prove consumability.

import { createSeqscribe, type SeqscribeNode, type TopicPolicy } from "seqscribe";
import type { Channel, FinalityCert, LogEntry, SqliteHandle, WriterDirective } from "seqscribe";

const policy: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

export function wire(storage: SqliteHandle): SeqscribeNode {
  const node = createSeqscribe({ writerId: "fixture-w1", storage });
  node.defineTopic("fixture.topic", policy);
  return node;
}

export type Surface = {
  entry: LogEntry;
  cert: FinalityCert;
  directive: WriterDirective;
  channel: Channel;
};
