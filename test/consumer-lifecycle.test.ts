// proposals-v3.5 P17–P19, P21 — durable-consumer lifecycle (reset/replay,
// delete/GC, deterministic caught-up) and bounded read-only scans. P20 is
// documentation-only (host-guide multi-process section).

import { describe, expect, it } from "vitest";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe, orderCompare, orderOf } from "../src/index.js";
import type {
  Constants,
  FinalityCert,
  LogEntry,
  SeqscribeNodeExt,
  TopicPolicy,
} from "../src/index.js";

const T = "t.ledger";
const AUTH = "test:authority";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
const CERTIFIED: TopicPolicy = { ...FULL, finalityAuthority: AUTH };
const TEST_CONSTANTS: Partial<Constants> = { FINALITY_WINDOW_MS: 50_000 };

function makeNode(sched: Scheduler, policy: TopicPolicy = FULL): SeqscribeNodeExt {
  const node = createSeqscribe({
    writerId: "wA",
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.5,
    constants: TEST_CONSTANTS,
    authority: { verifyFinality: (c) => c.sig === "valid-sig" },
  });
  node.defineTopic(T, policy);
  return node;
}

async function appendN(sched: Scheduler, node: SeqscribeNodeExt, n: number, kind = "e"): Promise<void> {
  for (let i = 0; i < n; i++) void node.log(T).append(kind, { i });
  await sched.run({ untilMs: sched.now() + 200 });
}

// Age existing rows past FINALITY_WINDOW_MS so they qualify for the §7.2
// cutoff. Register consumers AFTER aging — a cursor idle that long would be
// dropped as consumer_abandoned during the archive pass (§7.6).
async function age(sched: Scheduler): Promise<void> {
  await sched.run({ untilMs: sched.now() + TEST_CONSTANTS.FINALITY_WINDOW_MS! + 10_000 });
}

// §7.6 archive driver: certify, then let the archive pass run
async function certify(sched: Scheduler, node: SeqscribeNodeExt): Promise<FinalityCert> {
  const cert: FinalityCert = { ...node.proposeFinality(T)!, sig: "valid-sig" };
  const p = node.ingestFinality(cert);
  await sched.run({ untilMs: sched.now() + 1_000 });
  await p;
  return cert;
}

describe("P17 — consumer cursor reset / explicit replay", () => {
  it("reset(earliest-retained) replays the retained log under the SAME stable name", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    await appendN(sched, node, 5);

    let delivered = 0;
    const unsub = node.onEntry(T, "read-model", () => void delivered++);
    await sched.run({ untilMs: sched.now() + 100 });
    expect(delivered).toBe(5);

    // the P17 failure shape: re-registering the same name delivers nothing
    unsub();
    let redelivered = 0;
    const unsub2 = node.onEntry(T, "read-model", () => void redelivered++);
    await sched.run({ untilMs: sched.now() + 100 });
    expect(redelivered).toBe(0);
    unsub2();

    // the supported rebuild: reset, then re-register the stable name
    const r = node.resetConsumer(T, "read-model");
    expect(r).toEqual({ existed: true, from: "earliest-retained", replayFromRowid: 0, archivedRows: 0 });
    node.onEntry(T, "read-model", () => void redelivered++);
    await sched.run({ untilMs: sched.now() + 100 });
    expect(redelivered).toBe(5);
    await node.close();
  });

  it("reset(head) skips the backlog; reset while active throws", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    await appendN(sched, node, 5);

    const unsub = node.onEntry(T, "c", () => {});
    expect(() => node.resetConsumer(T, "c")).toThrowError(/active consumer/);
    unsub();

    const r = node.resetConsumer(T, "c", { from: "head" });
    expect(r.from).toBe("head");
    expect(r.replayFromRowid).toBeGreaterThan(0);
    const seen: number[] = [];
    node.onEntry(T, "c", (e) => void seen.push((e.payload as { i: number }).i));
    await sched.run({ untilMs: sched.now() + 100 });
    expect(seen).toEqual([]); // backlog skipped
    void node.log(T).append("e", { i: 99 });
    await sched.run({ untilMs: sched.now() + 200 });
    expect(seen).toEqual([99]); // fresh appends flow
    await node.close();
  });

  it("reports cold-archived rows as the coverage gap of 'earliest-retained'", async () => {
    const sched = new Scheduler(1_000_000);
    const node = makeNode(sched, CERTIFIED);
    await appendN(sched, node, 5, "old");
    await age(sched);
    await certify(sched, node);
    await appendN(sched, node, 1, "new"); // post-cut row; archive already ran

    const r = node.resetConsumer(T, "rebuild");
    expect(r.archivedRows).toBe(5); // "start" is the archive floor, not genesis
    const kinds: string[] = [];
    node.onEntry(T, "rebuild", (e) => void kinds.push(e.kind));
    await sched.run({ untilMs: sched.now() + 100 });
    expect(kinds).toEqual(["new"]); // only the hot log replays
    await node.close();
  });
});

describe("P18 — durable consumer deletion & GC", () => {
  it("deleteConsumer removes the cursor row and reports existence", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    await appendN(sched, node, 2);
    const unsub = node.onEntry(T, "gone", () => {});
    await sched.run({ untilMs: sched.now() + 100 });

    expect(() => node.deleteConsumer(T, "gone")).toThrowError(/active consumer/);
    unsub();
    expect(node.listConsumers(T)).toEqual([
      { consumer: "gone", lastRowid: 2, updatedAt: expect.any(String), active: false },
    ]);
    expect(node.deleteConsumer(T, "gone")).toEqual({ existed: true });
    expect(node.listConsumers(T)).toEqual([]);
    expect(node.deleteConsumer(T, "gone")).toEqual({ existed: false });
    await node.close();
  });

  it("pruneConsumers GCs versioned names, skipping active ones", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    await appendN(sched, node, 1);
    node.onEntry(T, "model#1", () => {})(); // registered then unsubscribed
    node.onEntry(T, "model#2", () => {})();
    node.onEntry(T, "model#3", () => {}); // stays active
    node.onEntry(T, "other", () => {})();
    await sched.run({ untilMs: sched.now() + 100 });

    expect(node.pruneConsumers(T, { prefix: "model#" }).sort()).toEqual(["model#1", "model#2"]);
    const left = node.listConsumers(T).map((c) => c.consumer).sort();
    expect(left).toEqual(["model#3", "other"]);
    await node.close();
  });

  it("deleting a lagging cursor releases the §7.6 archive gate", async () => {
    const sched = new Scheduler(1_000_000);
    const node = makeNode(sched, CERTIFIED);
    await appendN(sched, node, 5);
    await age(sched);
    // a consumer that registered but never consumed pins the hot log at rowid 0
    node.onEntry(T, "laggard", () => {})();
    await certify(sched, node);
    expect(node.stats().topics[T]!.archived).toBe(0); // gated by the laggard

    node.deleteConsumer(T, "laggard");
    await sched.run({ untilMs: sched.now() + 100 });
    expect(node.stats().topics[T]!.archived).toBe(5); // gate released, archive ran
    await node.close();
  });
});

describe("P19 — deterministic caught-up signal", () => {
  it("snapshots the head at call time and resolves when the cursor reaches it", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    await appendN(sched, node, 5);

    let delivered = 0;
    node.onEntry(T, "gate", () => void delivered++);
    const p = node.consumerCaughtUp(T, "gate"); // called before the drain ran
    let settled = false;
    void p.then(() => (settled = true));
    expect(settled).toBe(false);
    await sched.run({ untilMs: sched.now() + 100 });
    await expect(p).resolves.toEqual({ throughRowid: 5 });
    expect(delivered).toBe(5); // resolution implies the callbacks completed

    // already caught up → immediate; later appends start a new interval
    await expect(node.consumerCaughtUp(T, "gate")).resolves.toEqual({ throughRowid: 5 });
    await node.close();
  });

  it("rejects for unregistered consumers and on unsubscribe", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    await expect(node.consumerCaughtUp(T, "nobody")).rejects.toThrowError(/unregistered/);

    await appendN(sched, node, 3);
    let block = true;
    const unsub = node.onEntry(T, "quit", () => {
      if (block) throw new Error("not yet"); // backoff keeps the cursor parked
    });
    const p = node.consumerCaughtUp(T, "quit");
    p.catch(() => {}); // settled below
    await sched.run({ untilMs: sched.now() + 50 });
    block = false;
    unsub(); // give up before catch-up
    await expect(p).rejects.toThrowError(/unsubscribed before catch-up/);
    await node.close();
  });
});

describe("P21 — bounded read-only scans", () => {
  it("canonical scan pages under `limit` and honors a pinned `through`", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    await appendN(sched, node, 10);

    const pinned = node.headOrder(T)!;
    await appendN(sched, node, 2); // arrive after the pin — must be excluded

    const collected: LogEntry[] = [];
    let after: typeof pinned | undefined;
    for (;;) {
      const page = node.scanEntries(T, { ...(after ? { after } : {}), through: pinned, limit: 4 });
      collected.push(...page.entries);
      expect(page.entries.length).toBeLessThanOrEqual(4);
      expect(page.truncatedBelow).toBe(false);
      if (page.complete) break;
      after = page.nextAfter!;
    }
    expect(collected.length).toBe(10); // the two post-pin rows are outside the interval
    for (let i = 1; i < collected.length; i++)
      expect(orderCompare(orderOf(collected[i - 1]!), orderOf(collected[i]!))).toBeLessThan(0);
    expect(orderCompare(orderOf(collected[9]!), pinned)).toBeLessThanOrEqual(0);

    // no durable side effects: scans register nothing
    expect(node.listConsumers(T)).toEqual([]);
    await node.close();
  });

  it("writer form scans a seq range, spans the cold archive, and rejects mixed forms", async () => {
    const sched = new Scheduler(1_000_000);
    const node = makeNode(sched, CERTIFIED);
    await appendN(sched, node, 5, "old");
    await age(sched);
    node.onEntry(T, "advance", () => {}); // lets archiving proceed once caught up
    const gate = node.consumerCaughtUp(T, "advance");
    await sched.run({ untilMs: sched.now() + 100 });
    await gate;
    await certify(sched, node);
    await appendN(sched, node, 2, "new");
    expect(node.stats().topics[T]!.archived).toBe(5); // covered region went cold

    // canonical scans are hot-log-only — the archive gap is declared
    expect(node.scanEntries(T).truncatedBelow).toBe(true);

    // the writer form reads through the archive: the full range comes back
    const all = node.scanEntries(T, { writer: "wA" });
    expect(all.entries.map((e) => e.seq)).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(all.complete).toBe(true);
    expect(all.truncatedBelow).toBe(false);

    const mid = node.scanEntries(T, { writer: "wA", fromSeq: 2, toSeq: 6, limit: 3 });
    expect(mid.entries.map((e) => e.seq)).toEqual([2, 3, 4]);
    expect(mid.complete).toBe(false);
    expect(mid.nextFromSeq).toBe(5);

    expect(() => node.scanEntries(T, { writer: "wA", after: orderOf(all.entries[0]!) })).toThrowError(
      /writer form/,
    );
    expect(() => node.scanEntries(T, { fromSeq: 1 })).toThrowError(/require writer/);
    await node.close();
  });
});
