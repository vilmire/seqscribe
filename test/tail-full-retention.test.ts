// G2b — "tail" SUB view extended to serve `full`-retention subscribe-only
// topics (session.<id>.transcript's durable-retention delivery path), not
// just `ring`-retention topics. Design: docs/design/2026-09-23-wiring-
// unification.md §7e G2/G2b (root repo). Full rationale in subs.ts's
// resolveGroup "tail" branch and node.ts's fullTail()/handleTailApplied
// doc comments.
//
// Every test compares a `full` topic against an equivalent `ring` topic run
// through the identical subscribe/append/SNAP/DELTA sequence, to prove wire
// shape and delivery semantics are unchanged — a TranscriptReplicaStore-
// shaped consumer written against the ring "tail" view needs zero code
// change, only the topic's retention policy.
//
// Timing/harness conventions follow retire-topic.test.ts's file header:
// virtual-time scheduler, fire-then-sched.run()-then-await for queue-backed
// promises, and every connected-peer sched.run() call is bounded (an
// attached VirtualLink keeps re-scheduling anti-entropy indefinitely).

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe } from "../src/index.js";
import type { Row, SeqscribeNode, Subscription, TopicPolicy } from "../src/index.js";

const RING: TopicPolicy = {
  kind: "append",
  retention: { mode: "ring", size: 500 },
  replication: "subscribe-only",
  access: "metadata",
};

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

function makeNode(sched: Scheduler, writerId: string): SeqscribeNode {
  return createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
  });
}

interface Collected {
  snapshots: { rows: Row[]; reset: boolean }[];
  deltas: { upserts: Row[]; deletes: string[] }[];
}

function collect(sub: Subscription): Collected {
  const c: Collected = { snapshots: [], deltas: [] };
  sub.onSnapshot((rows, reset) => c.snapshots.push({ rows, reset }));
  sub.onDelta((d) => c.deltas.push(d));
  return c;
}

function connect(
  sched: Scheduler,
  rng: SeededRng,
  server: SeqscribeNode,
  client: SeqscribeNode,
  topic: string,
) {
  const link = new VirtualLink(sched, rng);
  server.attach(link.a, { peerId: "client", peerClass: "metadata", grants: { [topic]: "serve" } });
  const handle = client.attach(link.b, {
    peerId: "server",
    peerClass: "metadata",
    grants: { [topic]: "none" },
  });
  return handle;
}

describe("tail view on full-retention topics (G2b)", () => {
  it("serves a full-retention topic's durable rows through the built-in tail view", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(50);
    const topic = "session.f1.transcript";
    const server = makeNode(sched, "server");
    const client = makeNode(sched, "client");
    server.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    client.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    for (let i = 0; i < 5; i++) void server.log(topic).append("chunk", { i });
    await sched.run({ untilMs: 100 });

    const handle = connect(sched, rng, server, client, topic);
    await sched.run({ untilMs: 400 });

    const sub = client.subscribe(handle, { view: "tail", params: { topic } });
    const got = collect(sub);
    await sched.run({ untilMs: 800 });

    expect(got.snapshots).toHaveLength(1);
    expect(got.snapshots[0]!.rows.map((r) => r.seq)).toEqual([1, 2, 3, 4, 5]);
    expect(got.snapshots[0]!.rows.map((r) => r.kind)).toEqual(Array(5).fill("chunk"));

    void server.log(topic).append("chunk", { i: 5 });
    await sched.run({ untilMs: 1_200 });
    expect(got.deltas).toHaveLength(1);
    expect(got.deltas[0]!.upserts[0]!.seq).toBe(6);
  });

  it("ring vs full parity: identical SNAP/DELTA row shape and sequencing for the same append sequence", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(51);
    const ringTopic = "t.ring-parity";
    const fullTopic = "session.f2.transcript";
    const server = makeNode(sched, "server");
    const client = makeNode(sched, "client");
    for (const [topic, policy] of [
      [ringTopic, RING],
      [fullTopic, FULL_SUBSCRIBE_ONLY],
    ] as const) {
      server.defineTopic(topic, policy);
      client.defineTopic(topic, policy);
    }
    for (let i = 0; i < 3; i++) {
      void server.log(ringTopic).append("chunk", { i });
      void server.log(fullTopic).append("chunk", { i });
    }
    await sched.run({ untilMs: 100 });

    const link = new VirtualLink(sched, rng);
    server.attach(link.a, {
      peerId: "client",
      peerClass: "metadata",
      grants: { [ringTopic]: "serve", [fullTopic]: "serve" },
    });
    const handle = client.attach(link.b, {
      peerId: "server",
      peerClass: "metadata",
      grants: { [ringTopic]: "none", [fullTopic]: "none" },
    });
    await sched.run({ untilMs: 400 });

    const ringSub = client.subscribe(handle, { view: "tail", params: { topic: ringTopic } });
    const fullSub = client.subscribe(handle, { view: "tail", params: { topic: fullTopic } });
    const ringGot = collect(ringSub);
    const fullGot = collect(fullSub);
    await sched.run({ untilMs: 800 });

    expect(fullGot.snapshots).toHaveLength(1);
    expect(fullGot.snapshots[0]!.reset).toBe(ringGot.snapshots[0]!.reset);
    // shape parity: same row keys, same seq/kind content, only `key` (writer:seq) differs by topic name embedding — compare seq/kind/writer/hlc columns
    const strip = (rows: Row[]) => rows.map((r) => ({ seq: r.seq, kind: r.kind, writer: r.writer }));
    expect(strip(fullGot.snapshots[0]!.rows)).toEqual(strip(ringGot.snapshots[0]!.rows));

    void server.log(ringTopic).append("chunk", { i: 3 });
    void server.log(fullTopic).append("chunk", { i: 3 });
    await sched.run({ untilMs: 1_200 });

    expect(fullGot.deltas).toHaveLength(1);
    expect(fullGot.deltas).toHaveLength(ringGot.deltas.length);
    expect(strip(fullGot.deltas[0]!.upserts)).toEqual(strip(ringGot.deltas[0]!.upserts));
  });

  it("still refuses full-sync topics (ERR_UNKNOWN_VIEW) — tail is subscribe-only", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(52);
    const topic = "mesh.m1.events";
    const server = makeNode(sched, "server");
    const client = makeNode(sched, "client");
    server.defineTopic(topic, FULL_SYNC);
    client.defineTopic(topic, FULL_SYNC);
    void server.log(topic).append("evt", { x: 1 });
    await sched.run({ untilMs: 100 });

    const link = new VirtualLink(sched, rng);
    server.attach(link.a, { peerId: "client", peerClass: "content", grants: { [topic]: "serve" } });
    const handle = client.attach(link.b, {
      peerId: "server",
      peerClass: "content",
      grants: { [topic]: "none" },
    });
    await sched.run({ untilMs: 400 });

    const sub = client.subscribe(handle, { view: "tail", params: { topic } });
    const got = collect(sub);
    // SUB_ERR is control-plane, not a promise rejection — assert absence of
    // any snapshot/delta (the group never resolves), matching subs.test.ts's
    // "denies subscriptions on ungranted topics" negative-result pattern.
    await sched.run({ untilMs: 900 });
    expect(got.snapshots).toHaveLength(0);
    expect(got.deltas).toHaveLength(0);
  });

  it("a resuming subscriber past the delta journal retention gets a SNAP reset (gap → resync, never a silent skip)", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(53);
    const topic = "session.f3.transcript";
    const server = makeNode(sched, "server");
    const client = makeNode(sched, "client");
    server.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    client.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    void server.log(topic).append("chunk", { i: 0 });
    await sched.run({ untilMs: 100 });

    const handle = connect(sched, rng, server, client, topic);
    await sched.run({ untilMs: 400 });

    const sub = client.subscribe(handle, { view: "tail", params: { topic } });
    const got = collect(sub);
    await sched.run({ untilMs: 800 });
    expect(got.snapshots).toHaveLength(1);
    const staleCursor = sub.cursor;
    expect(staleCursor).toBeDefined();
    sub.close();

    // Push far more deltas than SUB_DELTA_RETAIN (256) so the held cursor's
    // deltaSeq falls below the journal's retained floor.
    for (let i = 1; i <= 260; i++) void server.log(topic).append("chunk", { i });
    await sched.run({ untilMs: 1_200 });

    const resumed = client.subscribe(handle, {
      view: "tail",
      params: { topic },
      fromCursor: staleCursor!,
    });
    const resumedGot = collect(resumed);
    await sched.run({ untilMs: 1_600 });

    // beyond-retention resume → fresh SNAP reset (subs.ts handleSub: "fresh
    // or beyond retention or epoch mismatch → SNAP reset"), never a partial
    // DELTA replay that silently drops the entries the journal evicted.
    expect(resumedGot.snapshots).toHaveLength(1);
    expect(resumedGot.snapshots[0]!.reset).toBe(true);
    expect(resumedGot.deltas).toHaveLength(0);
  });

  it("ACL denial still applies to a full-retention tail subscription", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(54);
    const topic = "session.f4.transcript";
    const server = makeNode(sched, "server");
    const client = makeNode(sched, "client");
    server.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    client.defineTopic(topic, FULL_SUBSCRIBE_ONLY);
    void server.log(topic).append("chunk", { i: 0 });
    await sched.run({ untilMs: 100 });

    const link = new VirtualLink(sched, rng);
    server.attach(link.a, { peerId: "client", peerClass: "metadata", grants: { [topic]: "none" } });
    const handle = client.attach(link.b, {
      peerId: "server",
      peerClass: "metadata",
      grants: { [topic]: "none" },
    });
    await sched.run({ untilMs: 400 });

    const sub = client.subscribe(handle, { view: "tail", params: { topic } });
    const got = collect(sub);
    await sched.run({ untilMs: 900 });
    expect(got.snapshots).toHaveLength(0);
    expect(got.deltas).toHaveLength(0);
  });

});
