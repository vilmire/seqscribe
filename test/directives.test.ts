// §12–13: writer directives — retire/unretire lifecycle, rgen discipline,
// propagation, and five-step fork canonicalization with recovery.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { chainOf, coreOf, createSeqscribe, seedOf } from "../src/index.js";
import type {
  Anomaly,
  Constants,
  LogEntry,
  SeqscribeNode,
  TopicPolicy,
  WriterDirective,
} from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
const TEST_CONSTANTS: Partial<Constants> = {
  ANTI_ENTROPY_MS: 1_000,
  CONTROL_RETRY_MS: 200,
  CHANNEL_STALL_MS: 30_000,
};

function makeNode(
  sched: Scheduler,
  writerId: string,
): { node: SeqscribeNode; anomalies: Anomaly[] } {
  const anomalies: Anomaly[] = [];
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    constants: TEST_CONSTANTS,
    authority: {
      verifyWriterDirective: (d) => d.sig === "valid-sig",
      issueWriterDirective: async (u) => ({ ...u, authority: "test:auth", sig: "valid-sig" }),
    },
  });
  node.onAnomaly((a) => anomalies.push(a));
  node.defineTopic(T, FULL);
  return { node, anomalies };
}

function connect(sched: Scheduler, rng: SeededRng, a: SeqscribeNode, b: SeqscribeNode) {
  const link = new VirtualLink(sched, rng);
  a.attach(link.a, { peerId: "pB", peerClass: "content", grants: { [T]: "full" } });
  b.attach(link.b, { peerId: "pA", peerClass: "content", grants: { [T]: "full" } });
  return link;
}

// directive application settles on the commit queue — drive the scheduler
async function drive<T2>(sched: Scheduler, p: Promise<T2>): Promise<T2> {
  p.catch(() => {}); // observed here; the caller re-awaits below
  await sched.run({ untilMs: sched.now() + 2_000 });
  return p;
}

describe("retire / unretire (§13)", () => {
  it("retires a writer, propagates the tombstone, blocks appends, unretires", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(61);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    connect(sched, rng, a.node, b.node);
    for (let i = 0; i < 3; i++) void a.node.log(T).append("note", { i });
    await sched.run({ untilMs: 3_000 });
    expect(coreOf(b.node).getStream(T, "wA").contigSeq).toBe(3);

    await drive(sched, a.node.retire("wA"));
    await sched.run({ untilMs: sched.now() + 3_000 });

    const headA = coreOf(a.node).getStream(T, "wA");
    expect(headA.sealReason).toBe("retired");
    expect(headA.finalSeq).toBe(3);
    expect(coreOf(b.node).getStream(T, "wA").sealReason).toBe("retired"); // propagated

    // sealed: further appends reject
    await expect(drive(sched, a.node.log(T).append("note", { late: 1 }))).rejects.toThrow(
      /ERR_WRITER_SEALED/,
    );

    // tombstone rides HAVE
    const vec = a.node.vectors()[T]!.writers.wA!;
    expect("retired" in vec && vec.finalSeq).toBe(3);

    await drive(sched, a.node.unretire("wA"));
    await sched.run({ untilMs: sched.now() + 3_000 });
    expect(coreOf(a.node).getStream(T, "wA").sealReason).toBeNull();
    expect(coreOf(b.node).getStream(T, "wA").sealReason).toBeNull();
    expect(coreOf(a.node).getStream(T, "wA").rgen).toBe(2);

    // and the stream continues
    void a.node.log(T).append("note", { back: true });
    await sched.run({ untilMs: sched.now() + 2_000 });
    expect(coreOf(b.node).getStream(T, "wA").contigSeq).toBe(4);
  });

  it("refuses equal-rgen directives with different content", async () => {
    const sched = new Scheduler(1_000);
    const a = makeNode(sched, "wA");
    void a.node.log(T).append("note", {});
    await sched.run({ untilMs: 1_500 });
    const head = coreOf(a.node).getStream(T, "wA");
    const d1: WriterDirective = {
      topic: T,
      writer: "wA",
      state: "retired",
      rgen: 1,
      finalSeq: head.contigSeq,
      finalChain: head.contigChain,
      authority: "test:auth",
      sig: "valid-sig",
    };
    await drive(sched, a.node.publishWriterDirective(d1));
    const d2: WriterDirective = { ...d1, finalSeq: 0, finalChain: seedOf(T, "wA") };
    await expect(drive(sched, a.node.publishWriterDirective(d2))).rejects.toThrow();
    expect(a.anomalies.some((x) => x.kind === "bad_directive")).toBe(true);
  });

  it("rejects directives failing verification", async () => {
    const sched = new Scheduler(1_000);
    const a = makeNode(sched, "wA");
    void a.node.log(T).append("note", {});
    await sched.run({ untilMs: 1_500 });
    const head = coreOf(a.node).getStream(T, "wA");
    const forged: WriterDirective = {
      topic: T,
      writer: "wA",
      state: "retired",
      rgen: 1,
      finalSeq: head.contigSeq,
      finalChain: head.contigChain,
      authority: "test:auth",
      sig: "forged",
    };
    await expect(drive(sched, a.node.publishWriterDirective(forged))).rejects.toThrow();
    expect(coreOf(a.node).getStream(T, "wA").sealReason).toBeNull();
  });
});

describe("fork canonicalization (§12)", () => {
  it("recovers the divergent node onto the canonical branch through the seal", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(62);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");

    // real branch on A (its own stream, 3 entries)
    for (let i = 1; i <= 3; i++) void a.node.log(T).append("note", { real: i });
    await sched.run({ untilMs: 1_500 });
    const canonical = coreOf(a.node).entries(T, "wA", 1, 3);

    // forged wA branch on B: same seq 1 as real, divergent from the start
    let prev = seedOf(T, "wA");
    for (let seq = 1; seq <= 2; seq++) {
      const forged: LogEntry = {
        topic: T,
        writer: "wA",
        seq,
        hlc: { l: 1_100 + seq, c: 0 },
        kind: "note",
        payload: { forged: seq },
        chain: "",
      };
      forged.chain = chainOf(prev, forged);
      prev = forged.chain;
      void coreOf(b.node).applyExternal(forged, "byzantine");
    }
    await sched.run({ untilMs: 2_000 });
    expect(coreOf(b.node).getStream(T, "wA").contigSeq).toBe(2);

    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 4_000 });
    // HAVE contradiction sealed both sides
    expect(coreOf(a.node).getStream(T, "wA").sealReason).toBe("fork");
    expect(coreOf(b.node).getStream(T, "wA").sealReason).toBe("fork");

    // the host adjudicates: A's branch is canonical (finalSeq 3)
    const directive: WriterDirective = {
      topic: T,
      writer: "wA",
      state: "retired",
      rgen: 1,
      finalSeq: 3,
      finalChain: canonical[2]!.chain,
      authority: "test:auth",
      sig: "valid-sig",
    };
    await drive(sched, a.node.publishWriterDirective(directive));
    await sched.run({ untilMs: sched.now() + 8_000 });

    // A agreed with canonical → sealed retired directly
    const headA = coreOf(a.node).getStream(T, "wA");
    expect(headA.sealReason).toBe("retired");
    expect(headA.contigSeq).toBe(3);

    // B ran the five-step recovery: forged suffix quarantined, canonical fetched
    const headB = coreOf(b.node).getStream(T, "wA");
    expect(headB.sealReason).toBe("retired");
    expect(headB.contigSeq).toBe(3);
    expect(headB.contigChain).toBe(canonical[2]!.chain);
    expect(coreOf(b.node).entries(T, "wA", 1, 3)).toEqual(canonical);
    expect(b.anomalies.some((x) => x.kind === "entry_quarantined")).toBe(true);
  });

  it("enters the absorbing state when no peer holds the canonical range", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(63);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");

    // B holds a forged wZ branch; nobody holds the canonical wZ entries
    let prev = seedOf(T, "wZ");
    for (let seq = 1; seq <= 2; seq++) {
      const forged: LogEntry = {
        topic: T,
        writer: "wZ",
        seq,
        hlc: { l: 1_050 + seq, c: 0 },
        kind: "note",
        payload: { forged: seq },
        chain: "",
      };
      forged.chain = chainOf(prev, forged);
      prev = forged.chain;
      void coreOf(b.node).applyExternal(forged, "byzantine");
    }
    await sched.run({ untilMs: 1_500 });

    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 2_500 });

    // canonical branch (never replicated anywhere) is declared by directive
    const directive: WriterDirective = {
      topic: T,
      writer: "wZ",
      state: "retired",
      rgen: 1,
      finalSeq: 2,
      finalChain: "0".repeat(64), // a chain nobody can produce
      authority: "test:auth",
      sig: "valid-sig",
    };
    await drive(sched, b.node.publishWriterDirective(directive));
    await sched.run({ untilMs: sched.now() + 6_000 });

    expect(coreOf(b.node).getStream(T, "wZ").sealReason).toBe("fork"); // absorbing
    expect(b.anomalies.some((x) => x.kind === "canonical_unavailable")).toBe(true);

    // escape: a superseding directive shrinks canonical to what exists (nothing)
    const supersede: WriterDirective = {
      topic: T,
      writer: "wZ",
      state: "retired",
      rgen: 2,
      finalSeq: 0,
      finalChain: seedOf(T, "wZ"),
      authority: "test:auth",
      sig: "valid-sig",
    };
    await drive(sched, b.node.publishWriterDirective(supersede));
    await sched.run({ untilMs: sched.now() + 3_000 });
    const head = coreOf(b.node).getStream(T, "wZ");
    expect(head.sealReason).toBe("retired");
    expect(head.contigSeq).toBe(0);
    expect(coreOf(b.node).entries(T, "wZ", 1, 2)).toEqual([]); // quarantined
  });
});
