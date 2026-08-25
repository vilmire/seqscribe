// Tier-1 sync over virtual links: HAVE/WANT/ENTRIES convergence, eager push,
// relay, loss tolerance with host-style reconnect, schema refusal, ACL guard,
// stall close, HAVE-contradiction forks.

import { describe, expect, it } from "vitest";
import { VirtualLink, type LinkFaults } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { chainOf, coreOf, createSeqscribe, seedOf } from "../src/index.js";
import type {
  Anomaly,
  Constants,
  LogEntry,
  PeerHandle,
  SeqscribeNode,
  TopicPolicy,
} from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

// compressed timing for tests — the library reads all timing from Constants
const TEST_CONSTANTS: Partial<Constants> = {
  ANTI_ENTROPY_MS: 2_000,
  CONTROL_RETRY_MS: 200,
  CHANNEL_STALL_MS: 3_000,
  HELLO_TIMEOUT_MS: 1_000,
};

interface TestNode {
  node: SeqscribeNode;
  writerId: string;
  anomalies: Anomaly[];
}

function makeNode(sched: Scheduler, writerId: string, topics: string[] = [T]): TestNode {
  const anomalies: Anomaly[] = [];
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    constants: TEST_CONSTANTS,
  });
  node.onAnomaly((a) => anomalies.push(a));
  for (const t of topics) node.defineTopic(t, FULL);
  return { node, writerId, anomalies };
}

function connect(
  sched: Scheduler,
  rng: SeededRng,
  a: TestNode,
  b: TestNode,
  faults?: Partial<LinkFaults>,
  topics: string[] = [T],
): { link: VirtualLink; ha: PeerHandle; hb: PeerHandle } {
  const link = new VirtualLink(sched, rng, faults);
  const grants = Object.fromEntries(topics.map((t) => [t, "full" as const]));
  const ha = a.node.attach(link.a, { peerId: b.writerId, peerClass: "content", grants });
  const hb = b.node.attach(link.b, { peerId: a.writerId, peerClass: "content", grants });
  return { link, ha, hb };
}

// Host-contract reconnect: on close, bring up a fresh link (mids/credits reset).
function connectWithReconnect(
  sched: Scheduler,
  rng: SeededRng,
  a: TestNode,
  b: TestNode,
  faults?: Partial<LinkFaults>,
): void {
  let generation = 0;
  const dial = () => {
    const gen = ++generation;
    const { ha } = connect(sched, rng.substream(`dial${gen}`), a, b, faults);
    ha.onStateChange((s) => {
      if (s === "closed") sched.schedule(sched.now() + 100, dial);
    });
  };
  dial();
}

function streamOf(n: TestNode, writer: string, topic = T): LogEntry[] {
  const core = coreOf(n.node);
  const head = core.getStream(topic, writer);
  return core.entries(topic, writer, 1, head.contigSeq);
}

function contigOf(n: TestNode, writer: string, topic = T): number {
  return coreOf(n.node).getStream(topic, writer).contigSeq;
}

describe("two-node convergence", () => {
  it("syncs a pre-connect backlog via HAVE/WANT/ENTRIES", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(42);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    for (let i = 0; i < 25; i++) void a.node.log(T).append("note", { i });
    await sched.run({ untilMs: 1_000_500 });
    expect(contigOf(a, "wA")).toBe(25);

    connect(sched, rng, a, b);
    await sched.run({ untilMs: 1_010_000 });
    expect(contigOf(b, "wA")).toBe(25);
    expect(streamOf(b, "wA")).toEqual(streamOf(a, "wA"));
  });

  it("delivers live appends by eager push, faster than anti-entropy", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(43);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    connect(sched, rng, a, b);
    await sched.run({ untilMs: 500 });

    void a.node.log(T).append("note", { live: true });
    await sched.run({ untilMs: 900 }); // < ANTI_ENTROPY_MS
    expect(contigOf(b, "wA")).toBe(1);
  });

  it("converges bidirectionally", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(44);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    for (let i = 0; i < 5; i++) {
      void a.node.log(T).append("note", { from: "a", i });
      void b.node.log(T).append("note", { from: "b", i });
    }
    connect(sched, rng, a, b);
    await sched.run({ untilMs: 10_000 });
    expect(contigOf(a, "wB")).toBe(5);
    expect(contigOf(b, "wA")).toBe(5);
    expect(streamOf(a, "wB")).toEqual(streamOf(b, "wB"));
    expect(streamOf(b, "wA")).toEqual(streamOf(a, "wA"));
  });
});

describe("fault tolerance (P4-lite)", () => {
  it("converges under 25% loss + duplication with host reconnect", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(4242);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    for (let i = 0; i < 30; i++) void a.node.log(T).append("note", { i });
    connectWithReconnect(sched, rng, a, b, { lossP: 0.25, dupP: 0.05 });

    // live appends trickle in during turbulence
    for (let i = 0; i < 10; i++) {
      sched.schedule(2_000 + i * 500, () => void a.node.log(T).append("note", { live: i }));
    }
    await sched.run({ untilMs: 60_000 });
    expect(contigOf(b, "wA")).toBe(40);
    expect(streamOf(b, "wA")).toEqual(streamOf(a, "wA"));
  });
});

describe("relay (§6.3)", () => {
  it("propagates a live entry across a line topology before anti-entropy", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(7);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    const c = makeNode(sched, "wC");
    connect(sched, rng.substream("ab"), a, b);
    connect(sched, rng.substream("bc"), b, c);
    await sched.run({ untilMs: 500 });

    void a.node.log(T).append("note", { hop: true });
    await sched.run({ untilMs: 1_500 }); // < ANTI_ENTROPY_MS
    expect(contigOf(c, "wA")).toBe(1);
  });
});

describe("negotiation & ACL", () => {
  it("refuses a schema-mismatched topic but syncs matching ones", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(9);
    const T2 = "t.other";
    const a = makeNode(sched, "wA", [T, T2]);

    const anomalies: Anomaly[] = [];
    const bNode = createSeqscribe({
      writerId: "wB",
      storage: memoryHandle(),
      clock: sched.clock(),
      timers: sched.timers(),
      constants: TEST_CONSTANTS,
      authority: { verifyFinality: () => true },
    });
    bNode.onAnomaly((x) => anomalies.push(x));
    bNode.defineTopic(T, { ...FULL, finalityAuthority: "auth:x" }); // differing schemaHash
    bNode.defineTopic(T2, FULL);
    const b: TestNode = { node: bNode, writerId: "wB", anomalies };

    void a.node.log(T).append("note", { x: 1 });
    void a.node.log(T2).append("note", { x: 2 });
    connect(sched, rng, a, b, undefined, [T, T2]);
    await sched.run({ untilMs: 10_000 });

    expect(contigOf(b, "wA", T)).toBe(0); // refused
    expect(contigOf(b, "wA", T2)).toBe(1); // synced
  });

  it("throws at attach when a metadata peer is granted a content topic", () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(10);
    const a = makeNode(sched, "wA");
    const link = new VirtualLink(sched, rng);
    expect(() =>
      a.node.attach(link.a, { peerId: "cloud", peerClass: "metadata", grants: { [T]: "full" } }),
    ).toThrowError(/metadata-class/);
  });
});

describe("stall (§5.2)", () => {
  it("closes the session when unACKed data outlives CHANNEL_STALL_MS", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(11);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    const { link, ha } = connect(sched, rng, a, b);
    await sched.run({ untilMs: 500 });
    expect(ha.state()).toBe("ready");

    link.cut(true); // silent partition
    void a.node.log(T).append("note", { stuck: true }); // eager push → unACKed
    await sched.run({ untilMs: 10_000 });
    expect(ha.state()).toBe("closed");
  });
});

describe("fork path ② — HAVE contradiction (§6.4)", () => {
  it("seals both sides and records probe evidence", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(12);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");

    // real branch on A (its own stream)
    void a.node.log(T).append("note", { real: 1 });
    void a.node.log(T).append("note", { real: 2 });
    // forged wA branch injected into B (as if from a byzantine peer earlier)
    let prev = seedOf(T, "wA");
    for (let seq = 1; seq <= 2; seq++) {
      const forged: LogEntry = {
        topic: T,
        writer: "wA",
        seq,
        hlc: { l: 10 + seq, c: 0 },
        kind: "note",
        payload: { forged: seq },
        chain: "",
      };
      forged.chain = chainOf(prev, forged);
      prev = forged.chain;
      void coreOf(b.node).applyExternal(forged, "byzantine");
    }
    await sched.run({ untilMs: 500 });
    expect(contigOf(b, "wA")).toBe(2);

    connect(sched, rng, a, b);
    await sched.run({ untilMs: 5_000 });

    expect(coreOf(a.node).getStream(T, "wA").sealReason).toBe("fork");
    expect(coreOf(b.node).getStream(T, "wA").sealReason).toBe("fork");
    expect(a.anomalies.some((x) => x.kind === "writer_forked")).toBe(true);
    expect(b.anomalies.some((x) => x.kind === "writer_forked")).toBe(true);

    // first-applied retention: neither side switched branches
    expect(streamOf(a, "wA")[0]!.payload).toEqual({ real: 1 });
    expect(streamOf(b, "wA")[0]!.payload).toEqual({ forged: 1 });

    // divergence from seq 1 ⇒ no common point
    const store = (coreOf(a.node) as unknown as { store: { metaGet: (k: string) => string | undefined } })
      .store;
    const ev = store.metaGet(`fork_evidence:${T}:wA:wB`);
    expect(ev).toBeDefined();
    expect((JSON.parse(ev!) as { lastCommon: number }).lastCommon).toBe(0);
  });
});
