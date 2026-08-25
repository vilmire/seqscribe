// §7 finality: proposal computation, ingestion, pre-finality rejection,
// post-cut quarantine, propagation + fgen repair, generation discipline.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { chainOf, coreOf, createSeqscribe, seedOf } from "../src/index.js";
import type {
  Anomaly,
  Constants,
  FinalityCert,
  LogEntry,
  SeqscribeNode,
  TopicPolicy,
} from "../src/index.js";

const T = "t.ledger";
const AUTH = "test:authority";
const POLICY: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
  finalityAuthority: AUTH,
};

const TEST_CONSTANTS: Partial<Constants> = {
  FINALITY_WINDOW_MS: 50_000,
  ANTI_ENTROPY_MS: 2_000,
  CONTROL_RETRY_MS: 200,
  CHANNEL_STALL_MS: 5_000,
};

// ingestFinality settles on the commit-queue flush — drive the scheduler while awaiting
async function ingest(sched: Scheduler, node: SeqscribeNode, cert: FinalityCert): Promise<void> {
  const p = node.ingestFinality(cert);
  await sched.run({ untilMs: sched.now() + 1_000 });
  await p;
}

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
    authority: { verifyFinality: (c) => c.sig === "valid-sig" },
  });
  node.onAnomaly((a) => anomalies.push(a));
  node.defineTopic(T, POLICY);
  return { node, anomalies };
}

describe("proposeFinality (§7.2)", () => {
  it("returns null on an empty qualifying window, then a correct proposal", async () => {
    const sched = new Scheduler(1_000_000);
    const { node } = makeNode(sched, "wA");
    for (let i = 0; i < 3; i++) void node.log(T).append("tx", { i });
    await sched.run({ untilMs: 1_000_100 });

    expect(node.proposeFinality(T)).toBeNull(); // entries younger than the window

    await sched.run({ untilMs: 1_100_000 }); // age past FINALITY_WINDOW_MS
    const p = node.proposeFinality(T);
    expect(p).not.toBeNull();
    expect(p!.generation).toBe(1);
    expect(p!.authority).toBe(AUTH);
    expect(p!.cut.wA!.seq).toBe(3);
    expect(p!.order.writer).toBe("wA");
    expect(p!.order.seq).toBe(3);
  });

  it("ingests a signed proposal; finality() then gates consumption", async () => {
    const sched = new Scheduler(1_000_000);
    const { node } = makeNode(sched, "wA");
    for (let i = 0; i < 3; i++) void node.log(T).append("tx", { i });
    await sched.run({ untilMs: 1_100_000 });
    const cert: FinalityCert = { ...node.proposeFinality(T)!, sig: "valid-sig" };
    await ingest(sched, node, cert);
    expect(node.finality(T)).toEqual(cert);
    expect(node.proposeFinality(T)).toBeNull(); // watermark unchanged → nothing new
  });
});

describe("enforcement (§7.5)", () => {
  it("rejects pre-finality arrivals at ingest", async () => {
    const sched = new Scheduler(1_000_000);
    const { node, anomalies } = makeNode(sched, "wA");
    for (let i = 0; i < 3; i++) void node.log(T).append("tx", { i });
    await sched.run({ untilMs: 1_100_000 });
    const cert: FinalityCert = { ...node.proposeFinality(T)!, sig: "valid-sig" };
    await ingest(sched, node, cert);

    // a genuinely-late entry: order below P, writer uncovered
    const late: LogEntry = {
      topic: T,
      writer: "wLate",
      seq: 1,
      hlc: { l: 999_000, c: 0 },
      kind: "tx",
      payload: { late: true },
      chain: "",
    };
    late.chain = chainOf(seedOf(T, "wLate"), late);
    const r = coreOf(node).applyExternal(late, "peer");
    await sched.run({ untilMs: sched.now() + 500 });
    expect(await r).toBe("rejected_finality");
    expect(anomalies.some((a) => a.kind === "pre_finality_rejected")).toBe(true);
  });

  it("quarantines an applied post-cut tail: contig rewinds, stream seals (§7.5a)", async () => {
    const sched = new Scheduler(1_000_000);
    const { node, anomalies } = makeNode(sched, "wB");

    // wA's stream applied locally: seq 1..3, all old enough to be under P
    let prev = seedOf(T, "wA");
    const entries: LogEntry[] = [];
    for (let seq = 1; seq <= 3; seq++) {
      const e: LogEntry = {
        topic: T,
        writer: "wA",
        seq,
        hlc: { l: 900_000 + seq, c: 0 },
        kind: "tx",
        payload: { seq },
        chain: "",
      };
      e.chain = chainOf(prev, e);
      prev = e.chain;
      entries.push(e);
      void coreOf(node).applyExternal(e, "peer");
    }
    await sched.run({ untilMs: 1_000_100 });
    expect(coreOf(node).getStream(T, "wA").contigSeq).toBe(3);

    // authority's cert covers only seq ≤ 2 (P = order of seq 3's hlc, cut at 2:
    // meaning the authority never held seq 3 — it finalized without it)
    const cert: FinalityCert = {
      topic: T,
      order: { l: 900_003, c: 0, writer: "wA", seq: 3 },
      cut: { wA: { seq: 2, chain: entries[1]!.chain } },
      generation: 1,
      authority: AUTH,
      sig: "valid-sig",
    };
    await ingest(sched, node, cert);

    const head = coreOf(node).getStream(T, "wA");
    expect(head.contigSeq).toBe(2); // rewound to the cut
    expect(head.sealReason).toBe("fork"); // 'fork'-equivalent seal — directive required
    expect(anomalies.some((a) => a.kind === "entry_quarantined")).toBe(true);
    expect(anomalies.some((a) => a.kind === "writer_forked")).toBe(true);
    // quarantined seqs stay permanently consumed
    expect(coreOf(node).entries(T, "wA", 3, 3)).toEqual([]);
  });
});

describe("propagation (§7.4)", () => {
  it("repairs a lagging peer via HAVE fgen after connect", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(21);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    for (let i = 0; i < 2; i++) void a.node.log(T).append("tx", { i });
    await sched.run({ untilMs: 1_100_000 });
    const cert: FinalityCert = { ...a.node.proposeFinality(T)!, sig: "valid-sig" };
    await ingest(sched, a.node, cert); // B is offline — push reaches nobody

    const link = new VirtualLink(sched, rng);
    a.node.attach(link.a, { peerId: "wB", peerClass: "content", grants: { [T]: "full" } });
    b.node.attach(link.b, { peerId: "wA", peerClass: "content", grants: { [T]: "full" } });
    await sched.run({ untilMs: 1_110_000 });

    expect(b.node.finality(T)).toEqual(cert); // fgen lag repaired
    expect(coreOf(b.node).getStream(T, "wA").contigSeq).toBe(2); // and entries synced
  });
});

describe("generation discipline (§7.2)", () => {
  it("ignores duplicates, flags generation reuse with different content", async () => {
    const sched = new Scheduler(1_000_000);
    const { node, anomalies } = makeNode(sched, "wA");
    for (let i = 0; i < 2; i++) void node.log(T).append("tx", { i });
    await sched.run({ untilMs: 1_100_000 });
    const cert: FinalityCert = { ...node.proposeFinality(T)!, sig: "valid-sig" };
    await ingest(sched, node, cert);
    await node.ingestFinality(cert); // identical duplicate — resolves without queue work

    const reused: FinalityCert = { ...cert, cut: {} }; // same generation, different content
    await expect(node.ingestFinality(reused)).rejects.toThrow();
    expect(anomalies.some((a) => a.kind === "bad_cert")).toBe(true);
    expect(node.finality(T)).toEqual(cert); // first one kept
  });

  it("rejects certs failing signature verification", async () => {
    const sched = new Scheduler(1_000_000);
    const { node, anomalies } = makeNode(sched, "wA");
    void node.log(T).append("tx", {});
    await sched.run({ untilMs: 1_100_000 });
    const forged: FinalityCert = { ...node.proposeFinality(T)!, sig: "forged" };
    await expect(node.ingestFinality(forged)).rejects.toThrow();
    expect(anomalies.some((a) => a.kind === "bad_cert")).toBe(true);
    expect(node.finality(T)).toBeNull();
  });
});
