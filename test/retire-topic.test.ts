// C7-7 writer-row GC: retireTopic(topic) / gcWriters({topicPrefix, idleForMs,
// isIdle}). Design: docs/design/2026-09-23-wiring-unification.md §5 C7-7;
// full rationale in the (root-repo) scratchpad seqscribe-retire-topic-design.md.
//
// Scope: session-transcript-shaped topics — subscribe-only, ring or none
// retention, append kind, zero durable sq_log rows. Every test below builds
// its own ring topic rather than reusing the file-wide FULL/T fixture other
// test files share, because retireTopic's preconditions are precisely about
// what distinguishes a ring topic from those full-sync fixtures.
//
// Timing: like every other file in this suite, all queue-backed promises
// (append/retireTopic/gcWriters) resolve only once the virtual-time
// scheduler's flush timer fires — fire the call, `await sched.run()`, THEN
// await the promise (log.test.ts's authorEntries() is the precedent this
// file follows throughout).

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

// Restart-safety test needs a real reopenable file — memoryHandle()'s
// :memory: connection dies with node.close() (same reason durability.test.ts
// uses a file, not memoryHandle, for its own reopen-after-crash coverage).
const dir = mkdtempSync(join(tmpdir(), "seqscribe-retire-topic-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

const RING: TopicPolicy = {
  kind: "append",
  retention: { mode: "ring", size: 8 },
  replication: "subscribe-only",
  access: "metadata",
};

const FULL_SYNC: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

const FULL_SUBSCRIBE_ONLY: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
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

/**
 * Fire, drain the scheduler, then await — see file header. `untilMs` is
 * required once any peer link is attached: a connected VirtualLink keeps
 * re-scheduling anti-entropy indefinitely, so an unbounded sched.run() never
 * returns (every connected-peer test in subs.test.ts/sync.test.ts bounds
 * every run() call for the same reason).
 */
async function run<T>(sched: Scheduler, p: Promise<T>, untilMs?: number): Promise<T> {
  await sched.run(untilMs !== undefined ? { untilMs } : undefined);
  return p;
}

/**
 * Same as run(), for a call expected to reject: attaches a no-op .catch
 * immediately so the rejection is "handled" before sched.run() settles it,
 * then returns the (still-rejecting) promise for `expect(...).rejects...`.
 * Without this, sched.run() -> microtask drain settles the rejection before
 * expect() has attached its own handler, and vitest reports an unhandled
 * rejection racing the assertion (observed: 3 spurious "Unhandled Errors"
 * even though the corresponding expect() passed).
 */
async function runRejecting<T>(sched: Scheduler, p: Promise<T>, untilMs?: number): Promise<T> {
  p.catch(() => {});
  await sched.run(untilMs !== undefined ? { untilMs } : undefined);
  return p;
}

describe("retireTopic", () => {
  it("deletes the sq_writers row and drops the topic from vectors()", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.s1.transcript";
    node.defineTopic(topic, RING);
    for (let i = 0; i < 5; i++) void node.log(topic).append("chunk", { i });
    await sched.run();
    expect(node.vectors()[topic]).toBeDefined();

    const r = await run(sched, node.retireTopic(topic));
    expect(r.writersRemoved).toBe(1);
    expect(node.vectors()[topic]).toBeUndefined();
  });

  it("1,000 dead session topics -> one gcWriters call -> sq_writers equals live sessions", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const deadTopics: string[] = [];
    for (let i = 0; i < 1000; i++) {
      const t = `session.dead-${i}.transcript`;
      deadTopics.push(t);
      node.defineTopic(t, RING);
      void node.log(t).append("chunk", { i });
    }
    const liveTopics: string[] = [];
    for (let i = 0; i < 5; i++) {
      const t = `session.live-${i}.transcript`;
      liveTopics.push(t);
      node.defineTopic(t, RING);
      void node.log(t).append("chunk", { i });
    }
    await sched.run();

    const live = new Set(liveTopics);
    const result = await run(
      sched,
      node.gcWriters({
        topicPrefix: "session.",
        idleForMs: 7 * 24 * 60 * 60 * 1000,
        isIdle: (topic) => !live.has(topic),
      }),
    );

    expect(result.retired.sort()).toEqual([...deadTopics].sort());
    expect(result.skipped).toEqual([]);

    const stats = node.stats();
    for (const t of liveTopics) expect(stats.topics[t]?.writers).toBe(1);
    // retireTopic never undefines the topic (§2.3 step 6 of the design) —
    // topics.list() still names it, so stats().topics[t] exists with
    // writers:0, not undefined; vectors() is the surface that drops it
    // entirely (asserted above in the single-topic test).
    for (const t of deadTopics) expect(stats.topics[t]?.writers).toBe(0);
  }, 20_000);

  it("a live subscriber blocks retirement", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(1);
    const server = makeNode(sched, "server");
    const client = makeNode(sched, "client");
    const topic = "session.s2.transcript";
    server.defineTopic(topic, RING);
    client.defineTopic(topic, RING);
    for (let i = 0; i < 3; i++) void server.log(topic).append("chunk", { i });
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

    const attempt = runRejecting(sched, server.retireTopic(topic), sched.now() + 200);
    await expect(attempt).rejects.toThrow(SeqscribeError);
    expect(server.stats().topics[topic]?.writers).toBe(1);

    unsub();
  });

  it("refuses a full-sync topic (precondition ordering — row untouched)", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "mesh.m1.events";
    node.defineTopic(topic, FULL_SYNC);
    void node.log(topic).append("evt", { x: 1 });
    await sched.run();

    // full-sync is rejected synchronously at the enqueue boundary (before
    // the item ever reaches the queue) — no sched.run() needed to observe it.
    await expect(node.retireTopic(topic)).rejects.toThrow(SeqscribeError);
    expect(node.stats().topics[topic]?.writers).toBe(1);
  });

  it("refuses a topic with durable sq_log entries", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "assistant.journal";
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    void node.log(topic).append("evt", { x: 1 });
    await sched.run();
    expect(node.stats().topics[topic]?.logRows).toBeGreaterThan(0);

    const attempt = runRejecting(sched, node.retireTopic(topic));
    await expect(attempt).rejects.toThrow(SeqscribeError);
    const stats = node.stats().topics[topic]!;
    expect(stats.writers).toBe(1);
    expect(stats.logRows).toBeGreaterThan(0);
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
    // register-kind is also rejected synchronously at enqueue.
    await expect(node.retireTopic(topic)).rejects.toThrow(SeqscribeError);
  });

  it("never undefines the topic: a later subscriber gets a normal empty SNAP, not ERR_UNKNOWN_TOPIC", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(2);
    const server = makeNode(sched, "server");
    const topic = "session.s3.transcript";
    server.defineTopic(topic, RING);
    for (let i = 0; i < 4; i++) void server.log(topic).append("chunk", { i });
    await sched.run();
    const retirePromise = server.retireTopic(topic);
    await sched.run();
    await retirePromise;

    // A late peer that never attached before retirement subscribes after —
    // this proves retireTopic did NOT remove the topic definition itself.
    const client = makeNode(sched, "lateClient");
    client.defineTopic(topic, RING);
    const link = new VirtualLink(sched, rng);
    server.attach(link.a, { peerId: "lateClient", peerClass: "metadata", grants: { [topic]: "serve" } });
    const handle = client.attach(link.b, {
      peerId: "server",
      peerClass: "metadata",
      grants: { [topic]: "none" },
    });
    await sched.run({ untilMs: 400 });

    let snapshotRows: unknown[] | undefined;
    const sub = client.subscribe(handle, { view: "tail", params: { topic } });
    const unsub = sub.onSnapshot((rows) => {
      snapshotRows = rows;
    });
    await sched.run({ untilMs: 800 });

    expect(snapshotRows).toEqual([]); // empty ring, not an ERR_UNKNOWN_TOPIC refusal
    unsub();

    // And a fresh append after retirement starts a brand-new writer at seq 1
    // — no resurrection of the old head.
    const appendPromise = server.log(topic).append("chunk", { i: 99 });
    await sched.run({ untilMs: sched.now() + 200 });
    const id = await appendPromise;
    expect(id[2]).toBe(1); // EntryId = [topic, writer, seq]
  });

  it("is idempotent: a second retireTopic call is a successful no-op", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "session.s4.transcript";
    node.defineTopic(topic, RING);
    void node.log(topic).append("chunk", { i: 0 });
    await sched.run();

    const first = await run(sched, node.retireTopic(topic));
    expect(first.writersRemoved).toBe(1);
    const second = await run(sched, node.retireTopic(topic));
    expect(second.writersRemoved).toBe(0);
  });

  it("restart safety: retire, close, reopen the same DB — still empty, fresh writer starts at seq 1", async () => {
    const sched = new Scheduler(0);
    const dbPath = join(dir, "restart-safety.db");
    const topic = "session.s5.transcript";
    let node = createSeqscribe({
      writerId: "w1",
      storage: fileHandle(dbPath),
      clock: sched.clock(),
      timers: sched.timers(),
    }) as SeqscribeNodeExt;
    node.defineTopic(topic, RING);
    for (let i = 0; i < 3; i++) void node.log(topic).append("chunk", { i });
    await sched.run();
    await run(sched, node.retireTopic(topic));
    await node.close();

    node = createSeqscribe({
      writerId: "w1",
      storage: fileHandle(dbPath),
      clock: sched.clock(),
      timers: sched.timers(),
    }) as SeqscribeNodeExt;
    node.defineTopic(topic, RING);
    expect(node.stats().topics[topic]?.writers ?? 0).toBe(0);

    const id = await run(sched, node.log(topic).append("chunk", { i: 100 }));
    expect(id[2]).toBe(1); // EntryId = [topic, writer, seq]
    await node.close();
  });

  it("shrinks vectors()/Beacon report proportionally, not just by key count", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const deadTopics: string[] = [];
    for (let i = 0; i < 50; i++) {
      const t = `session.dead-${i}.transcript`;
      deadTopics.push(t);
      node.defineTopic(t, RING);
      void node.log(t).append("chunk", { blob: "x".repeat(64) });
    }
    const liveTopics: string[] = [];
    for (let i = 0; i < 5; i++) {
      const t = `session.live-${i}.transcript`;
      liveTopics.push(t);
      node.defineTopic(t, RING);
      void node.log(t).append("chunk", { i });
    }
    await sched.run();

    const before = JSON.stringify(node.vectors());
    const live = new Set(liveTopics);
    await run(
      sched,
      node.gcWriters({
        topicPrefix: "session.",
        idleForMs: 0,
        isIdle: (topic) => !live.has(topic),
      }),
    );
    const after = node.vectors();
    const afterKeys = Object.keys(after).filter((t) => t.startsWith("session."));
    expect(afterKeys.sort()).toEqual([...liveTopics].sort());
    expect(JSON.stringify(after).length).toBeLessThan(before.length / 5);
  });

  it("a live onEntry consumer blocks retirement of a full-retention subscribe-only topic", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const topic = "assistant.journal2";
    node.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    const unsub = node.onEntry(topic, "watcher", () => {});
    await sched.run();

    // durable entries precondition also blocks it (no entries here), so
    // this exercises the consumer check specifically: no entries appended,
    // only the active-consumer registration.
    const attempt = runRejecting(sched, node.retireTopic(topic));
    await expect(attempt).rejects.toThrow(SeqscribeError);
    unsub();
  });
});
