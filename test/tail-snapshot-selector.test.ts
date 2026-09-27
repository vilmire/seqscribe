// Tail-snapshot selector (host-guide §4.6): a host hook choosing which rows a
// built-in "tail" SNAP carries. Pins: the selected rows ARE the SNAP body;
// DELTAs/cursors are untouched; null and a throwing selector both fall back
// to the default window; the backward `page` walk is newest-first with a
// strictly decreasing rowid for both `full` and `ring` topics.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe } from "../src/index.js";
import type { LogEntry, Row, SeqscribeNodeExt, TailSource, TopicPolicy } from "../src/index.js";

const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "subscribe-only",
  access: "metadata",
};
const RING: TopicPolicy = {
  kind: "append",
  retention: { mode: "ring", size: 50 },
  replication: "subscribe-only",
  access: "metadata",
};

function makeNode(sched: Scheduler, writerId: string): SeqscribeNodeExt {
  return createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.21,
  });
}

// "Everything from the newest `begin` row onward" — the shape a revisioned
// topic (self-contained begin…commit groups) needs.
function fromLastBegin(src: TailSource): LogEntry[] | null {
  const picked: LogEntry[] = [];
  let before: number | null = null;
  let scanned = 0;
  while (scanned < src.defaultLimit) {
    const page = src.page(before, 7);
    if (page.length === 0) return null;
    for (const { entry, rowid } of page) {
      scanned++;
      picked.push(entry);
      before = rowid;
      if (entry.kind === "begin") return picked.reverse();
    }
  }
  return null;
}

async function harness(topic: string, policy: TopicPolicy) {
  const sched = new Scheduler(0);
  const rng = new SeededRng(77);
  const server = makeNode(sched, "server");
  const client = makeNode(sched, "client");
  server.defineTopic(topic, policy);
  client.defineTopic(topic, policy);
  const link = new VirtualLink(sched, rng);
  server.attach(link.a, { peerId: "client", peerClass: "metadata", grants: { [topic]: "serve" } });
  const handle = client.attach(link.b, { peerId: "server", peerClass: "metadata", grants: { [topic]: "none" } });
  await sched.run({ untilMs: 200 });
  const appendRevision = (rev: number, chunks: number) => {
    void server.log(topic).append("begin", { rev });
    for (let i = 0; i < chunks; i++) void server.log(topic).append("chunk", { rev, i });
    void server.log(topic).append("commit", { rev });
  };
  const subscribe = () => {
    const got: { snaps: { rows: Row[]; reset: boolean }[]; deltas: Row[][] } = { snaps: [], deltas: [] };
    const sub = client.subscribe(handle, { view: "tail", params: { topic } });
    sub.onSnapshot((rows, reset) => got.snaps.push({ rows, reset }));
    sub.onDelta((d) => got.deltas.push(d.upserts));
    return got;
  };
  return { sched, server, client, appendRevision, subscribe };
}

const kinds = (rows: Row[]) => rows.map((r) => `${r.kind}:${JSON.parse(String(r.payload)).rev}`);

describe("tail snapshot selector", () => {
  for (const [label, policy] of [
    ["full", FULL],
    ["ring", RING],
  ] as const) {
    it(`${label}: the SNAP carries exactly the selected rows; DELTAs are unaffected`, async () => {
      const topic = `session.sel-${label}.transcript`;
      const h = await harness(topic, policy);
      for (let rev = 1; rev <= 4; rev++) h.appendRevision(rev, 3);
      void h.server.log(topic).append("begin", { rev: 5 }); // in-flight revision
      void h.server.log(topic).append("chunk", { rev: 5, i: 0 });
      await h.sched.run({ untilMs: 400 });

      const seen: TailSource[] = [];
      h.server.setTailSnapshotSelector((src) => {
        seen.push(src);
        return fromLastBegin(src);
      });
      const got = h.subscribe();
      await h.sched.run({ untilMs: 1_000 });

      expect(seen[0]!.topic).toBe(topic);
      expect(seen[0]!.retention).toBe(label);
      expect(seen[0]!.defaultLimit).toBe(label === "ring" ? 50 : 500);
      expect(got.snaps).toHaveLength(1);
      expect(got.snaps[0]!.reset).toBe(true);
      expect(kinds(got.snaps[0]!.rows)).toEqual(["begin:5", "chunk:5"]);

      void h.server.log(topic).append("commit", { rev: 5 });
      await h.sched.run({ untilMs: 1_400 });
      expect(got.deltas.map(kinds)).toEqual([["commit:5"]]);
    });
  }

  it("page() walks newest-first with strictly decreasing rowids", async () => {
    const topic = "session.sel-page.transcript";
    const h = await harness(topic, FULL);
    for (let i = 0; i < 10; i++) void h.server.log(topic).append("chunk", { rev: 0, i });
    await h.sched.run({ untilMs: 400 });
    let walked: number[] = [];
    h.server.setTailSnapshotSelector((src) => {
      const ids: number[] = [];
      let before: number | null = null;
      for (;;) {
        const page = src.page(before, 3);
        if (page.length === 0) break;
        for (const p of page) ids.push(p.entry.seq);
        const last = page[page.length - 1]!.rowid;
        expect(before === null || last < before).toBe(true);
        before = last;
      }
      walked = ids;
      return null;
    });
    const got = h.subscribe();
    await h.sched.run({ untilMs: 1_000 });
    expect(walked).toEqual([10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
    // null → default window
    expect(got.snaps[0]!.rows.map((r) => r.seq)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it("a throwing selector falls back to the default window", async () => {
    const topic = "session.sel-throw.transcript";
    const h = await harness(topic, FULL);
    h.appendRevision(1, 2);
    await h.sched.run({ untilMs: 400 });
    h.server.setTailSnapshotSelector(() => {
      throw new Error("selector bug");
    });
    const got = h.subscribe();
    await h.sched.run({ untilMs: 1_000 });
    expect(kinds(got.snaps[0]!.rows)).toEqual(["begin:1", "chunk:1", "chunk:1", "commit:1"]);
  });
});
