// proposals-v3.5 P22/P23 — the ADHDev production-incident regression: a peer
// holding entries the local finality certificate rejects must NOT drive an
// unbounded zero-delay WANT → ENTRIES → reject-all → WANT spin. The finality
// branch now has the recovery branch's absorbing-state discipline, rejects
// are counted into stats(), and the stall surfaces as a sync_stalled anomaly.
// P23 (adapter statement cache) gets a direct prepare-count unit test; every
// other suite exercises it implicitly through memoryHandle. P24 (interval
// sync throughput) pins the complementary "hot and busy" observability:
// served/applied interval counters, stats()-read reset, and the sync_hot
// informational anomaly.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { betterSqlite3Handle, coreOf, createSeqscribe } from "../src/index.js";
import type {
  Anomaly,
  Channel,
  Constants,
  FinalityCert,
  SeqscribeNodeExt,
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
  CHANNEL_STALL_MS: 30_000,
  HELLO_TIMEOUT_MS: 1_000,
};

function makeNode(
  sched: Scheduler,
  writerId: string,
  verify: (c: FinalityCert) => boolean,
  constants?: Partial<Constants>,
): { node: SeqscribeNodeExt; anomalies: Anomaly[] } {
  const anomalies: Anomaly[] = [];
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.5,
    constants: { ...TEST_CONSTANTS, ...constants },
    authority: { verifyFinality: verify },
  });
  node.onAnomaly((a) => anomalies.push(a));
  node.defineTopic(T, POLICY);
  return { node, anomalies };
}

function countingChannel(inner: Channel): { ch: Channel; types: () => Record<string, number> } {
  const sent: string[] = [];
  return {
    ch: {
      send: (m) => {
        sent.push(m);
        inner.send(m);
      },
      onMessage: (cb) => inner.onMessage(cb),
      onClose: (cb) => inner.onClose(cb),
      close: () => inner.close(),
    },
    types: () => {
      const out: Record<string, number> = {};
      for (const s of sent) {
        const t = (JSON.parse(s) as { t: string }).t;
        out[t] = (out[t] ?? 0) + 1;
      }
      return out;
    },
  };
}

describe("P22 — finality-branch WANT non-progress detection (incident regression)", () => {
  it("a peer serving pre-finality-rejected rows stalls instead of spinning", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(97);
    // A verifies its own cert; B rejects it (bad_cert) so B keeps its rows hot
    // and keeps serving them — the incident's poisoned-peer shape
    const a = makeNode(sched, "wA", (c) => c.sig === "valid-sig");
    const b = makeNode(sched, "wB", () => false);

    // B authors rows A will reject: their order will sit under A's cert order
    for (let i = 0; i < 5; i++) void b.node.log(T).append("old", { i });
    await sched.run({ untilMs: sched.now() + 500 });
    void a.node.log(T).append("mine", {}); // later hlc — the cert order lands above B's rows
    await sched.run({ untilMs: sched.now() + TEST_CONSTANTS.FINALITY_WINDOW_MS! + 10_000 });
    const cert: FinalityCert = { ...a.node.proposeFinality(T)!, sig: "valid-sig" };
    const pIngest = a.node.ingestFinality(cert);
    await sched.run({ untilMs: sched.now() + 500 });
    await pIngest;
    expect(cert.cut["wB"]).toBeUndefined(); // wB absent from the cut ⇒ cover 0

    const link = new VirtualLink(sched, rng);
    const counted = countingChannel(link.a);
    a.node.attach(counted.ch, { peerId: "wB", peerClass: "content", grants: { [T]: "full" } });
    b.node.attach(link.b, { peerId: "wA", peerClass: "content", grants: { [T]: "full" } });

    // two anti-entropy periods — the old code spun continuously in this window
    await sched.run({ untilMs: sched.now() + 4_500 });

    // every served row was rejected; nothing applied — and the stall is a
    // per-stream suspension, not a session failure
    expect(coreOf(a.node).getStream(T, "wB").contigSeq).toBe(0);
    expect(a.node.stats().peers.find((p) => p.peerId === "wB")!.state).toBe("ready");

    // bounded WANT traffic: initial round + one per HAVE round, not a spin
    // (the incident profile was hundreds of rounds per second)
    expect(counted.types()["WANT"] ?? 0).toBeLessThanOrEqual(6);

    // the stall is visible: anomaly, per-peer stalled count, reject counters
    expect(a.anomalies.filter((x) => x.kind === "sync_stalled").length).toBe(1);
    const stats = a.node.stats();
    expect(stats.peers.find((p) => p.peerId === "wB")!.stalledStreams).toBe(1);
    expect(stats.topics[T]!.applyRejects["rejected_finality"]).toBeGreaterThanOrEqual(5);

    await Promise.all([a.node.close(), b.node.close()]);
  });

  it("multi-round healthy backlogs still pull to completion without stalling", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(98);
    const a = makeNode(sched, "wA", (c) => c.sig === "valid-sig");
    const b = makeNode(sched, "wB", (c) => c.sig === "valid-sig");
    // > ENTRIES_BATCH_MAX so completion re-queues WANT across several rounds
    for (let i = 0; i < 250; i++) void b.node.log(T).append("e", { i });
    await sched.run({ untilMs: sched.now() + 1_000 });

    const link = new VirtualLink(sched, rng);
    a.node.attach(link.a, { peerId: "wB", peerClass: "content", grants: { [T]: "full" } });
    b.node.attach(link.b, { peerId: "wA", peerClass: "content", grants: { [T]: "full" } });
    await sched.run({ untilMs: sched.now() + 5_000 });

    expect(coreOf(a.node).getStream(T, "wB").contigSeq).toBe(250);
    expect(a.anomalies.filter((x) => x.kind === "sync_stalled")).toEqual([]);
    expect(a.node.stats().peers[0]!.stalledStreams).toBe(0);
    await Promise.all([a.node.close(), b.node.close()]);
  });
});

describe("P24 — interval sync throughput in stats()", () => {
  it("a served backlog shows up in interval counters on both ends, and reading stats() resets them", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(99);
    const a = makeNode(sched, "wA", (c) => c.sig === "valid-sig");
    const b = makeNode(sched, "wB", (c) => c.sig === "valid-sig");
    for (let i = 0; i < 250; i++) void b.node.log(T).append("e", { i });
    await sched.run({ untilMs: sched.now() + 1_000 });

    const link = new VirtualLink(sched, rng);
    a.node.attach(link.a, { peerId: "wB", peerClass: "content", grants: { [T]: "full" } });
    b.node.attach(link.b, { peerId: "wA", peerClass: "content", grants: { [T]: "full" } });
    await sched.run({ untilMs: sched.now() + 5_000 });
    expect(coreOf(a.node).getStream(T, "wB").contigSeq).toBe(250);

    // responder side: WANT service (+ knowledge push) counted as served
    const sb = b.node.stats();
    expect(sb.topics[T]!.sync.servedEntries).toBeGreaterThanOrEqual(250);
    expect(sb.topics[T]!.sync.servedBytes).toBeGreaterThan(0);
    expect(sb.topics[T]!.sync.wantRoundsServed).toBeGreaterThanOrEqual(1);
    expect(sb.syncHotspots[0]).toMatchObject({ topic: T, peerId: "wA" });
    expect(sb.syncHotspots[0]!.bytes).toBeGreaterThan(0);

    // requester side: landed applies counted, WANT rounds issued
    const sa = a.node.stats();
    expect(sa.topics[T]!.sync.appliedEntries).toBeGreaterThanOrEqual(250);
    expect(sa.topics[T]!.sync.appliedBytes).toBeGreaterThan(0);
    expect(sa.topics[T]!.sync.wantRoundsRequested).toBeGreaterThanOrEqual(1);
    expect(sa.syncHotspots[0]).toMatchObject({ topic: T, peerId: "wB" });

    // default SYNC_HOT_BYTES (32 MiB) is far above this traffic — no anomaly
    expect(a.anomalies.filter((x) => x.kind === "sync_hot")).toEqual([]);
    expect(b.anomalies.filter((x) => x.kind === "sync_hot")).toEqual([]);

    // interval semantics: the read above consumed the interval — a second
    // read with no traffic in between is all zeros and hotspot-free
    const sa2 = a.node.stats();
    expect(sa2.topics[T]!.sync).toEqual({
      servedEntries: 0,
      servedBytes: 0,
      appliedEntries: 0,
      appliedBytes: 0,
      wantRoundsRequested: 0,
      wantRoundsServed: 0,
    });
    expect(sa2.syncHotspots).toEqual([]);

    // and a fresh interval accumulates again
    for (let i = 0; i < 5; i++) void b.node.log(T).append("more", { i });
    await sched.run({ untilMs: sched.now() + 3_000 });
    const sa3 = a.node.stats();
    expect(sa3.topics[T]!.sync.appliedEntries).toBeGreaterThanOrEqual(5);

    await Promise.all([a.node.close(), b.node.close()]);
  });

  it("crossing SYNC_HOT_BYTES within one window emits sync_hot once (informational, no throttle)", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(100);
    // window longer than the whole test ⇒ at most one emission per node
    const HOT: Partial<Constants> = { SYNC_HOT_BYTES: 1_000, SYNC_HOT_WINDOW_MS: 600_000 };
    const a = makeNode(sched, "wA", (c) => c.sig === "valid-sig", HOT);
    const b = makeNode(sched, "wB", (c) => c.sig === "valid-sig", HOT);
    for (let i = 0; i < 250; i++) void b.node.log(T).append("e", { i });
    await sched.run({ untilMs: sched.now() + 1_000 });

    const link = new VirtualLink(sched, rng);
    a.node.attach(link.a, { peerId: "wB", peerClass: "content", grants: { [T]: "full" } });
    b.node.attach(link.b, { peerId: "wA", peerClass: "content", grants: { [T]: "full" } });
    await sched.run({ untilMs: sched.now() + 5_000 });

    // sync completed — the anomaly observed, never throttled
    expect(coreOf(a.node).getStream(T, "wB").contigSeq).toBe(250);
    expect(a.anomalies.filter((x) => x.kind === "sync_hot").length).toBe(1);
    expect(b.anomalies.filter((x) => x.kind === "sync_hot").length).toBe(1);

    await Promise.all([a.node.close(), b.node.close()]);
  });
});

describe("P23 — better-sqlite3 statement cache", () => {
  it("prepares each distinct SQL once across repeated run/get/all", () => {
    const prepared: string[] = [];
    const fake = {
      prepare(sql: string) {
        prepared.push(sql);
        return {
          reader: sql.trimStart().toUpperCase().startsWith("SELECT"),
          run: () => ({ changes: 1, lastInsertRowid: 1 }),
          get: () => undefined,
          all: () => [],
        };
      },
      transaction<R>(fn: () => R): () => R {
        return fn;
      },
    };
    const h = betterSqlite3Handle(fake);
    for (let i = 0; i < 10; i++) {
      h.run("INSERT INTO t VALUES (?)", [i]);
      h.get("SELECT * FROM t WHERE id = ?", [i]);
      h.all("SELECT * FROM t", []);
    }
    expect(prepared.sort()).toEqual([
      "INSERT INTO t VALUES (?)",
      "SELECT * FROM t",
      "SELECT * FROM t WHERE id = ?",
    ]);
  });
});
