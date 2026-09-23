// G2b — Node.pruneTopic(topic, {olderThanMs?, keepNewest?}): local-only
// durable-row prune for `full`-retention subscribe-only topics (the primitive
// `writer-gc.ts`-shaped hosts need for a topic that accumulates unboundedly
// between finality certs). Design: docs/design/2026-09-23-wiring-unification
// .md §7e G2/G2b (root repo); full rationale in log.ts's pruneTopic()/
// processPruneTopic() doc comments.
//
// Follows retire-topic.test.ts's file-header conventions throughout: virtual-
// time scheduler, fire-then-sched.run()-then-await for queue-backed promises,
// runRejecting for a call expected to reject (unhandled-rejection race with
// vitest's own microtask drain — see that helper's doc comment).

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { fileHandle, memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe, SeqscribeError } from "../src/index.js";
import type { SeqscribeNodeExt, TopicPolicy } from "../src/index.js";

const dir = mkdtempSync(join(tmpdir(), "seqscribe-prune-topic-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

const FULL_SUBSCRIBE_ONLY: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "subscribe-only",
  access: "metadata",
};

const FULL_SYNC: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

const RING: TopicPolicy = {
  kind: "append",
  retention: { mode: "ring", size: 8 },
  replication: "subscribe-only",
  access: "metadata",
};

function makeNode(sched: Scheduler, writerId = "w1"): SeqscribeNodeExt {
  return createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
  }) as SeqscribeNodeExt;
}

// See retire-topic.test.ts's identical helper doc comment for why this
// pairing (fire, drain, await) is required for every queue-backed promise.
async function run<T>(sched: Scheduler, p: Promise<T>, untilMs?: number): Promise<T> {
  await sched.run(untilMs !== undefined ? { untilMs } : undefined);
  return p;
}

async function runRejecting<T>(sched: Scheduler, p: Promise<T>, untilMs?: number): Promise<T> {
  p.catch(() => {});
  await sched.run(untilMs !== undefined ? { untilMs } : undefined);
  return p;
}

describe("pruneTopic", () => {
  it("keepNewest: 10,000-row topic bounded to the newest N, oldest rows removed", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.p1.transcript";
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    for (let i = 0; i < 10_000; i++) void node.log(topic).append("chunk", { i });
    await sched.run();
    expect(node.stats().topics[topic]?.logRows).toBe(10_000);

    const r = await run(sched, node.pruneTopic(topic, { keepNewest: 100 }));
    expect(r.prunedRows).toBe(9_900);
    expect(node.stats().topics[topic]?.logRows).toBe(100);

    // the SURVIVING rows are the newest 100, not an arbitrary 100
    const scan = node.scanEntries(topic, { writer: "w1", fromSeq: 1, toSeq: 10_000, limit: 10_000 });
    expect(scan.entries.map((e) => e.seq)).toEqual(
      Array.from({ length: 100 }, (_, i) => 9_901 + i),
    );
  }, 20_000);

  it("olderThanMs: prunes only entries older than the cutoff, using hlc_l as the age axis", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.p2.transcript";
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);

    for (let i = 0; i < 5; i++) void node.log(topic).append("chunk", { i });
    await sched.run();
    await sched.run({ untilMs: sched.now() + 10_000 }); // 10s gap between the two bursts
    for (let i = 5; i < 10; i++) void node.log(topic).append("chunk", { i });
    await sched.run();
    expect(node.stats().topics[topic]?.logRows).toBe(10);

    const r = await run(sched, node.pruneTopic(topic, { olderThanMs: 5_000 }));
    expect(r.prunedRows).toBe(5);
    const scan = node.scanEntries(topic, { writer: "w1", fromSeq: 1, toSeq: 10, limit: 10 });
    expect(scan.entries.map((e) => e.seq)).toEqual([6, 7, 8, 9, 10]);
  });

  it("both bounds given: the intersection applies (never prunes more than either bound alone would)", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.p3.transcript";
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    for (let i = 0; i < 5; i++) void node.log(topic).append("chunk", { i });
    await sched.run();
    await sched.run({ untilMs: sched.now() + 10_000 });
    for (let i = 5; i < 10; i++) void node.log(topic).append("chunk", { i });
    await sched.run();

    // olderThanMs alone would prune the first 5 (age); keepNewest=8 alone
    // would prune the first 2 (count) — the tighter (smaller-deletion)
    // bound wins, so only the first 2 rows are removed.
    const r = await run(sched, node.pruneTopic(topic, { olderThanMs: 5_000, keepNewest: 8 }));
    expect(r.prunedRows).toBe(2);
    const scan = node.scanEntries(topic, { writer: "w1", fromSeq: 1, toSeq: 10, limit: 10 });
    expect(scan.entries.map((e) => e.seq)).toEqual([3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it("never prunes past a registered onEntry consumer's cursor", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.p4.transcript";
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    for (let i = 0; i < 20; i++) void node.log(topic).append("chunk", { i });
    await sched.run();

    // A consumer that has only read the first 5 entries — cursor sits at the
    // rowid of seq 5.
    let delivered = 0;
    const unsub = node.onEntry(topic, "reader-a", (e) => {
      delivered++;
      if (e.seq >= 5) throw new Error("stall deliberately at seq 5"); // never advances past 5
    });
    await sched.run({ untilMs: 200 });
    expect(delivered).toBeGreaterThanOrEqual(5);

    const before = node.stats().topics[topic]!;
    const consumerLag = before.consumers["reader-a"];
    expect(consumerLag).toBeDefined();

    // Ask to keep only the newest 2 — WITHOUT the cursor floor this would
    // prune down to 2 rows; the cursor floor must win. Bounded run(): the
    // stalled consumer's backoff retry keeps rescheduling forever (it always
    // throws), so an unbounded sched.run() here would never return.
    const r = await run(sched, node.pruneTopic(topic, { keepNewest: 2 }), sched.now() + 500);
    const after = node.stats().topics[topic]!;
    expect(after.logRows).toBeGreaterThan(2);
    expect(r.prunedRows).toBeLessThan(18);

    // Every row still >= the consumer's unread floor must remain — the
    // consumer's own next read (once unstuck) must find its next entry.
    const scan = node.scanEntries(topic, { writer: "w1", fromSeq: 1, toSeq: 20, limit: 20 });
    expect(scan.entries.some((e) => e.seq === 5)).toBe(true); // the not-yet-delivered boundary entry survives

    unsub();
  });

  it("an active tail subscriber blocks pruning outright", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(60);
    const server = makeNode(sched, "server");
    const client = makeNode(sched, "client");
    const topic = "session.p5.transcript";
    server.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    client.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    for (let i = 0; i < 10; i++) void server.log(topic).append("chunk", { i });
    await sched.run({ untilMs: 100 });

    const link = new VirtualLink(sched, rng);
    server.attach(link.a, { peerId: "client", peerClass: "metadata", grants: { [topic]: "serve" } });
    const handle = client.attach(link.b, {
      peerId: "server",
      peerClass: "metadata",
      grants: { [topic]: "none" },
    });
    await sched.run({ untilMs: 400 });

    const sub = client.subscribe(handle, { view: "tail", params: { topic } });
    const unsub = sub.onSnapshot(() => {});
    await sched.run({ untilMs: 800 });

    const attempt = runRejecting(sched, server.pruneTopic(topic, { keepNewest: 1 }), sched.now() + 200);
    await expect(attempt).rejects.toThrow(SeqscribeError);
    expect(server.stats().topics[topic]?.logRows).toBe(10);

    unsub();
  });

  it("refuses a full-sync topic", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "mesh.m1.events";
    node.defineTopic(topic, FULL_SYNC);
    void node.log(topic).append("evt", { x: 1 });
    await sched.run();

    await expect(node.pruneTopic(topic, { keepNewest: 1 })).rejects.toThrow(SeqscribeError);
    expect(node.stats().topics[topic]?.logRows).toBe(1);
  });

  it("refuses a register-kind topic", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "reg.things";
    node.defineTopic(topic, {
      kind: "register",
      retention: { mode: "full" },
      replication: "full-sync",
      access: "content",
    });
    await expect(node.pruneTopic(topic, { keepNewest: 1 })).rejects.toThrow(SeqscribeError);
  });

  it("refuses a ring-retention topic (nothing durable for it to shrink)", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.p6.transcript";
    node.defineTopic(topic, RING);
    void node.log(topic).append("chunk", { i: 0 });
    await sched.run();

    const attempt = runRejecting(sched, node.pruneTopic(topic, { keepNewest: 1 }));
    await expect(attempt).rejects.toThrow(SeqscribeError);
  });

  it("refuses when neither olderThanMs nor keepNewest is given", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.p7.transcript";
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    void node.log(topic).append("chunk", { i: 0 });
    await sched.run();

    await expect(node.pruneTopic(topic, {})).rejects.toThrow(SeqscribeError);
  });

  it("is idempotent: pruning an already-bounded topic is a successful no-op", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.p8.transcript";
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    for (let i = 0; i < 5; i++) void node.log(topic).append("chunk", { i });
    await sched.run();

    const first = await run(sched, node.pruneTopic(topic, { keepNewest: 3 }));
    expect(first.prunedRows).toBe(2);
    const second = await run(sched, node.pruneTopic(topic, { keepNewest: 3 }));
    expect(second.prunedRows).toBe(0);
    expect(node.stats().topics[topic]?.logRows).toBe(3);
  });

  it("restart safety: prune, close, reopen the same DB — surviving rows and writer head both intact", async () => {
    const sched = new Scheduler(0);
    const dbPath = join(dir, "restart-safety.db");
    const topic = "session.p9.transcript";
    let node = createSeqscribe({
      writerId: "w1",
      storage: fileHandle(dbPath),
      clock: sched.clock(),
      timers: sched.timers(),
    }) as SeqscribeNodeExt;
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    for (let i = 0; i < 10; i++) void node.log(topic).append("chunk", { i });
    await sched.run();
    await run(sched, node.pruneTopic(topic, { keepNewest: 3 }));
    expect(node.stats().topics[topic]?.logRows).toBe(3);
    await node.close();

    node = createSeqscribe({
      writerId: "w1",
      storage: fileHandle(dbPath),
      clock: sched.clock(),
      timers: sched.timers(),
    }) as SeqscribeNodeExt;
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    expect(node.stats().topics[topic]?.logRows).toBe(3);
    const scan = node.scanEntries(topic, { writer: "w1", fromSeq: 1, toSeq: 10, limit: 10 });
    expect(scan.entries.map((e) => e.seq)).toEqual([8, 9, 10]);

    // writer head is untouched by prune (only sq_log rows moved) — a fresh
    // append continues the SAME contiguous stream, not a reset to seq 1.
    const id = await run(sched, node.log(topic).append("chunk", { i: 99 }));
    expect(id[2]).toBe(11); // EntryId = [topic, writer, seq]
    await node.close();
  });

  it("prunedRows is 0, not an error, when the topic already has fewer rows than keepNewest", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.p10.transcript";
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    void node.log(topic).append("chunk", { i: 0 });
    await sched.run();

    const r = await run(sched, node.pruneTopic(topic, { keepNewest: 500 }));
    expect(r.prunedRows).toBe(0);
    expect(node.stats().topics[topic]?.logRows).toBe(1);
  });
});
