// Acknowledged retention for full-sync append topics (host-guide §4.8):
// node.pruneAcked deletes, per stream, only rows every included member has
// acknowledged (their recorded HAVE vectors), that are older than the
// retention window and that every onEntry consumer has read; a peer below the
// resulting floor recovers with TRUNCATED (proto ≥ 3) instead of a gap.
//
// Deterministic simulation: virtual-time scheduler + virtual links, real
// SQLite. Day-scale ageing is a per-fleet clock offset (every node's clock is
// sched.now() + skew), so a "40 days later" step costs one assignment instead
// of 1.7M anti-entropy events.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { coreOf, createSeqscribe, SeqscribeError } from "../src/index.js";
import type {
  AckedPruneResult,
  Anomaly,
  Channel,
  Constants,
  LogEntry,
  PeerHandle,
  SeqscribeNodeExt,
  TopicPolicy,
} from "../src/index.js";

const T = "mesh.m.events";
const DAY = 86_400_000;
const WINDOW = 30 * DAY;
const FULL_SYNC: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "metadata",
};
const TEST_CONSTANTS: Partial<Constants> = {
  ANTI_ENTROPY_MS: 2_000,
  CONTROL_RETRY_MS: 200,
  CHANNEL_STALL_MS: 30_000,
  HELLO_TIMEOUT_MS: 1_000,
};

interface Fleet {
  sched: Scheduler;
  rng: SeededRng;
  skew: { v: number };
}

interface TestNode {
  node: SeqscribeNodeExt;
  id: string;
  anomalies: Anomaly[];
  // what a windowed onEntry consumer (the shape of ADHDev's mesh.index: rows
  // older than the window are dropped by its own retention) currently holds
  consumed: Map<string, number>;
  sent: string[]; // wire frame types this node emitted
}

function fleet(seed: number): Fleet {
  return { sched: new Scheduler(1_000_000), rng: new SeededRng(seed), skew: { v: 0 } };
}

function makeNode(f: Fleet, id: string, o: { consumer?: boolean; authority?: boolean } = {}): TestNode {
  const anomalies: Anomaly[] = [];
  const node = createSeqscribe({
    writerId: id,
    storage: memoryHandle(),
    clock: () => f.sched.now() + f.skew.v,
    timers: f.sched.timers(),
    constants: TEST_CONSTANTS,
    ...(o.authority
      ? {
          authority: {
            verifyWriterDirective: (d: { sig: string }) => d.sig === "valid-sig",
            issueWriterDirective: async <U,>(u: U) => ({ ...u, authority: "test:auth", sig: "valid-sig" }),
          },
        }
      : {}),
  } as Parameters<typeof createSeqscribe>[0]);
  node.onAnomaly((a) => anomalies.push(a));
  node.defineTopic(T, FULL_SYNC);
  const consumed = new Map<string, number>();
  if (o.consumer !== false)
    node.onEntry(T, "idx", (e: LogEntry) => {
      consumed.set(`${e.writer}:${e.seq}`, e.hlc.l);
    });
  return { node, id, anomalies, consumed, sent: [] };
}

// Frame-type sniffer + optional HELLO proto rewrite (an "old" peer: both
// directions of the link claim protoMax 2, so both ends negotiate proto 2).
function wrap(ch: Channel, sent: string[], protoMax?: number): Channel {
  const rewrite = (raw: string): string => {
    if (protoMax === undefined) return raw;
    const m = JSON.parse(raw) as { t: string; protoMax?: number };
    if (m.t !== "HELLO") return raw;
    return JSON.stringify({ ...m, protoMax });
  };
  return {
    send: (raw: string) => {
      sent.push((JSON.parse(raw) as { t: string }).t);
      ch.send(rewrite(raw));
    },
    onMessage: (cb) => ch.onMessage((raw) => cb(rewrite(raw))),
    onClose: (cb) => ch.onClose(cb),
    close: () => ch.close(),
  };
}

function connect(
  f: Fleet,
  a: TestNode,
  b: TestNode,
  o: { protoMax?: number } = {},
): { link: VirtualLink; ha: PeerHandle; hb: PeerHandle } {
  const link = new VirtualLink(f.sched, f.rng.substream(`${a.id}-${b.id}`));
  const grants = { [T]: "full" as const };
  const ha = a.node.attach(wrap(link.a, a.sent, o.protoMax), { peerId: b.id, peerClass: "content", grants });
  const hb = b.node.attach(wrap(link.b, b.sent, o.protoMax), { peerId: a.id, peerClass: "content", grants });
  return { link, ha, hb };
}

async function advance(f: Fleet, ms: number): Promise<void> {
  await f.sched.run({ untilMs: f.sched.now() + ms });
}

// queue-backed promises settle on the virtual clock — drive it
async function drive<R>(f: Fleet, p: Promise<R>): Promise<R> {
  p.catch(() => {});
  await advance(f, 100);
  return p;
}

async function appendN(f: Fleet, n: TestNode, count: number, tag: string): Promise<void> {
  for (let i = 0; i < count; i++) void n.node.log(T).append("ev", { tag, i });
  await advance(f, 100);
}

function contig(n: TestNode, writer: string): number {
  return coreOf(n.node).getStream(T, writer).contigSeq;
}

function rows(n: TestNode, writer: string): LogEntry[] {
  return coreOf(n.node).entries(T, writer, 1, Number.MAX_SAFE_INTEGER);
}

function logRows(n: TestNode): number {
  return n.node.stats().topics[T]!.logRows;
}

function pending(n: TestNode): number {
  return n.node.stats().topics[T]!.pending;
}

// the windowed consumer state: entries still inside the retention window
function windowed(n: TestNode, now: number): string[] {
  return [...n.consumed]
    .filter(([, l]) => l >= now - WINDOW)
    .map(([k]) => k)
    .sort();
}

const prune = (f: Fleet, n: TestNode, o: Partial<Parameters<SeqscribeNodeExt["pruneAcked"]>[1]> = {}) =>
  drive(f, n.node.pruneAcked(T, { olderThanMs: WINDOW, maxLagMs: WINDOW, ...o }));

// Three fully meshed nodes, each with `count` entries of its own, converged.
async function meshOf3(seed: number, count = 20) {
  const f = fleet(seed);
  const a = makeNode(f, "wA");
  const b = makeNode(f, "wB");
  const c = makeNode(f, "wC");
  const ab = connect(f, a, b);
  const bc = connect(f, b, c);
  const ac = connect(f, a, c);
  for (const n of [a, b, c]) await appendN(f, n, count, "old");
  await advance(f, 6_000);
  for (const n of [a, b, c]) for (const w of ["wA", "wB", "wC"]) expect(contig(n, w)).toBe(count);
  return { f, a, b, c, ab, bc, ac };
}

describe("pruneAcked — floor bounded by every member's acknowledgment", () => {
  it("prunes rows every member acknowledged and older than the window, keeping each live head", async () => {
    const { f, a, b, c } = await meshOf3(501);
    f.skew.v = 40 * DAY;
    await advance(f, 5_000); // HAVE rounds under the new clock refresh liveness
    const r = await prune(f, a);
    expect(r.prunedRows).toBe(57); // 3 writers × (20 − the head row)
    expect(r.more).toBe(false);
    expect(a.node.retentionFloors(T)).toEqual({ wA: 19, wB: 19, wC: 19 });
    for (const w of ["wA", "wB", "wC"]) {
      expect(r.writers[w]).toMatchObject({ floor: 19, ackFloor: 20 });
      expect(rows(a, w).map((e) => e.seq)).toEqual([20]);
      expect(contig(a, w)).toBe(20); // heads untouched
    }
    expect(r.members.map((m) => [m.node, m.excluded])).toEqual([
      ["wB", null],
      ["wC", null],
    ]);
    expect(logRows(a)).toBe(3);
    const st = a.node.stats().topics[T]!.retention!;
    expect(st).toMatchObject({ prunedRows: 57, floorStreams: 3, ackNodes: 2 });

    // replication continues across the floor
    await appendN(f, b, 3, "new");
    await advance(f, 3_000);
    expect(contig(a, "wB")).toBe(23);
    expect(contig(c, "wB")).toBe(23);
    expect(pending(a)).toBe(0);
  });

  it("keeps rows inside the retention window whatever the acks say", async () => {
    const { f, a } = await meshOf3(502);
    f.skew.v = 10 * DAY; // everything acknowledged, nothing old enough
    await advance(f, 5_000);
    const r = await prune(f, a);
    expect(r.prunedRows).toBe(0);
    expect(logRows(a)).toBe(60);
  });

  it("a lagging member within maxLag pins the floor at its acknowledgment", async () => {
    const f = fleet(503);
    const a = makeNode(f, "wA");
    const b = makeNode(f, "wB");
    const c = makeNode(f, "wC");
    connect(f, a, b);
    const ac = connect(f, a, c);
    for (const n of [a, b, c]) await appendN(f, n, 20, "old");
    await advance(f, 6_000);
    expect(contig(c, "wA")).toBe(20);
    ac.ha.detach(); // c goes offline holding 20 of each
    await advance(f, 1_000);
    await appendN(f, a, 20, "later");
    await appendN(f, b, 20, "later");
    await advance(f, 5_000);
    expect(contig(b, "wA")).toBe(40);

    f.skew.v = 40 * DAY;
    await advance(f, 5_000);
    // c was last seen 40 days ago: inside a 60-day max-lag it still pins
    const r = await prune(f, a, { maxLagMs: 60 * DAY });
    expect(r.writers.wA).toMatchObject({ floor: 20, ackFloor: 20, pinnedBy: "wC" });
    expect(r.writers.wB).toMatchObject({ floor: 20, ackFloor: 20, pinnedBy: "wC" });
    expect(r.writers.wC).toMatchObject({ floor: 19 }); // own head row kept
    expect(r.members.find((m) => m.node === "wC")!.excluded).toBeNull();
    expect(rows(a, "wA")[0]!.seq).toBe(21); // what c still needs is held

    // past max-lag c stops pinning; it will bootstrap with TRUNCATED
    f.skew.v = 100 * DAY;
    await advance(f, 5_000);
    const r2 = await prune(f, a, { maxLagMs: 60 * DAY });
    expect(r2.members.find((m) => m.node === "wC")!.excluded).toBe("lagging");
    // every remaining member holds the whole stream: only the head row bounds it
    expect(r2.writers.wA).toMatchObject({ floor: 39, ackFloor: 40, pinnedBy: null });
    expect(r2.writers.wB!.floor).toBe(39);
  });

  it("a removed member stops pinning at once and its acknowledgments are forgotten", async () => {
    const f = fleet(504);
    const a = makeNode(f, "wA");
    const b = makeNode(f, "wB");
    const c = makeNode(f, "wC");
    connect(f, a, b);
    const ac = connect(f, a, c);
    for (const n of [a, b]) await appendN(f, n, 20, "old");
    await advance(f, 6_000);
    ac.ha.detach();
    await appendN(f, a, 20, "later");
    await advance(f, 5_000);
    f.skew.v = 40 * DAY;
    await advance(f, 5_000);
    expect(a.node.stats().topics[T]!.retention!.ackNodes).toBe(2);
    const pinned = await prune(f, a, { maxLagMs: 60 * DAY, dryRun: true });
    expect(pinned.writers.wA!.pinnedBy).toBe("wC");
    const r = await prune(f, a, { maxLagMs: 60 * DAY, excludeMembers: ["wC"] });
    expect(r.members.find((m) => m.node === "wC")!.excluded).toBe("excluded");
    expect(r.writers.wA).toMatchObject({ floor: 39, pinnedBy: null });
    expect(a.node.stats().topics[T]!.retention!.ackNodes).toBe(1);
  });

  it("a writer that has never acknowledged pins its peers' streams at 0 until max-lag", async () => {
    const f = fleet(505);
    const a = makeNode(f, "wA");
    const b = makeNode(f, "wB");
    // wZ wrote to the topic (via b) but a never exchanged HAVE with it
    const z = makeNode(f, "wZ");
    const bz = connect(f, b, z);
    connect(f, a, b);
    await appendN(f, z, 5, "z");
    await appendN(f, a, 20, "a");
    await advance(f, 6_000);
    bz.ha.detach();
    expect(contig(a, "wZ")).toBe(5);
    f.skew.v = 40 * DAY;
    await advance(f, 5_000);
    // wZ's newest entry is 40 days old: within 60 days it pins wA at 0
    const r = await prune(f, a, { maxLagMs: 60 * DAY });
    expect(r.writers.wA).toMatchObject({ floor: 0, ackFloor: 0, pinnedBy: "wZ" });
    // wZ's own stream is held by its author and acknowledged by b: it may go
    expect(r.writers.wZ).toMatchObject({ floor: 4, ackFloor: 5 });
    expect(r.prunedRows).toBe(4);
  });

  it("never prunes rows an onEntry consumer has not read", async () => {
    const f = fleet(506);
    const a = makeNode(f, "wA", { consumer: false });
    let fail = true;
    a.node.onEntry(T, "stuck", () => {
      if (fail) throw new Error("not yet");
    });
    await appendN(f, a, 10, "x");
    f.skew.v = 40 * DAY;
    const r = await prune(f, a);
    expect(r.prunedRows).toBe(0);
    fail = false;
    await advance(f, 35_000); // consumer backoff retries and catches up
    const r2 = await prune(f, a);
    expect(r2.prunedRows).toBe(9);
  });

  it("is bounded per call and reports `more` until done; dryRun deletes nothing", async () => {
    const { f, a } = await meshOf3(507, 30);
    f.skew.v = 40 * DAY;
    await advance(f, 5_000);
    const dry = await prune(f, a, { dryRun: true, maxRows: 10_000 });
    expect(dry.prunedRows).toBe(87);
    expect(logRows(a)).toBe(90);
    expect(a.node.retentionFloors(T)).toEqual({});
    let total = 0;
    let calls = 0;
    for (;;) {
      const r: AckedPruneResult = await prune(f, a, { maxRows: 25 });
      expect(r.prunedRows).toBeLessThanOrEqual(25);
      total += r.prunedRows;
      calls++;
      if (!r.more) break;
    }
    expect(total).toBe(87);
    expect(calls).toBe(4);
    expect(logRows(a)).toBe(3);
  });

  it("skips a fork-sealed stream (§12 adjudication may need every row)", async () => {
    const { f, a } = await meshOf3(508);
    f.skew.v = 40 * DAY;
    await advance(f, 5_000);
    await drive(f, coreOf(a.node).sealStream(T, "wB"));
    const r = await prune(f, a);
    expect(r.writers.wB).toMatchObject({ floor: 0, skipped: "forked" });
    expect(rows(a, "wB")).toHaveLength(20);
    expect(r.prunedRows).toBe(38);
    // and a fork-sealed stream refuses floor adoption
    expect(await drive(f, coreOf(a.node).adoptFloor(T, "wB", 30, "ab".repeat(32)))).toBe("refused");
  });

  it("prunes a retired writer's stream through its final seq", async () => {
    const f = fleet(509);
    const a = makeNode(f, "wA", { authority: true });
    const b = makeNode(f, "wB", { authority: true });
    connect(f, a, b);
    await appendN(f, a, 5, "a");
    await appendN(f, b, 8, "b");
    await advance(f, 5_000);
    await drive(f, a.node.retire("wB"));
    await advance(f, 5_000);
    expect(coreOf(b.node).getStream(T, "wB").sealReason).toBe("retired");
    f.skew.v = 40 * DAY;
    await advance(f, 5_000);
    const r = await prune(f, a);
    expect(r.writers.wB!.floor).toBe(8);
    expect(rows(a, "wB")).toHaveLength(0);
    expect(r.writers.wA!.floor).toBe(4);
  });

  it("refuses register, keyed, subscribe-only topics and bad arguments", async () => {
    const f = fleet(510);
    const a = makeNode(f, "wA");
    a.node.defineTopic("t.sub", { ...FULL_SYNC, replication: "subscribe-only" });
    a.node.defineTopic("t.keyed", {
      kind: "append",
      keyed: { tombstoneKind: "del" },
      retention: { mode: "full" },
      replication: "subscribe-only",
      access: "metadata",
    });
    for (const [topic, o] of [
      ["t.sub", { olderThanMs: 1, maxLagMs: 1 }],
      ["t.keyed", { olderThanMs: 1, maxLagMs: 1 }],
      [T, { olderThanMs: -1, maxLagMs: 1 }],
      [T, { olderThanMs: 1, maxLagMs: Number.NaN }],
      [T, { olderThanMs: 1, maxLagMs: 1, maxRows: 0 }],
    ] as const) {
      await expect(drive(f, a.node.pruneAcked(topic, o))).rejects.toBeInstanceOf(SeqscribeError);
    }
  });
});

describe("TRUNCATED bootstrap — a peer below the floor recovers without a gap", () => {
  it("a node joining after the prune adopts every floor, converges, and its windowed consumer state matches", async () => {
    const { f, a, b, c } = await meshOf3(520);
    f.skew.v = 40 * DAY;
    for (const n of [a, b, c]) await appendN(f, n, 5, "recent"); // inside the window
    await advance(f, 5_000);
    for (const n of [a, b, c]) {
      const r = await prune(f, n);
      expect(r.prunedRows).toBe(60); // the 20 old rows per writer; the recent 5 stay
    }
    expect(a.node.retentionFloors(T)).toEqual({ wA: 20, wB: 20, wC: 20 });

    const d = makeNode(f, "wD");
    connect(f, d, a);
    await advance(f, 8_000);
    for (const w of ["wA", "wB", "wC"]) {
      expect(contig(d, w)).toBe(25);
      expect(rows(d, w)).toEqual(rows(a, w)); // exactly what a still holds
    }
    expect(pending(d)).toBe(0);
    expect(d.anomalies.filter((x) => x.kind === "floor_adopted").map((x) => x.writer).sort()).toEqual([
      "wA",
      "wB",
      "wC",
    ]);
    expect(d.node.retentionFloors(T)).toEqual({ wA: 20, wB: 20, wC: 20 });
    expect(a.node.stats().topics[T]!.retention!.truncatedServed).toBe(3);
    expect(d.node.stats().topics[T]!.retention!.floorsAdopted).toBe(3);
    expect(a.sent).toContain("TRUNCATED");

    // the consumers a window-bounded host keeps agree exactly
    const now = f.sched.now() + f.skew.v;
    expect(windowed(d, now)).toEqual(windowed(a, now));
    expect(windowed(d, now)).toHaveLength(15);

    // and d is a full participant afterwards: new entries flow both ways
    await appendN(f, d, 2, "d");
    await appendN(f, b, 2, "b2");
    await advance(f, 5_000);
    expect(contig(c, "wD")).toBe(2);
    expect(contig(d, "wB")).toBe(27);
    expect(windowed(d, f.sched.now() + f.skew.v)).toEqual(windowed(c, f.sched.now() + f.skew.v));
  });

  it("a lag-excluded member returning later bootstraps past the floor the others moved on", async () => {
    const f = fleet(521);
    const a = makeNode(f, "wA");
    const b = makeNode(f, "wB");
    const c = makeNode(f, "wC");
    connect(f, a, b);
    const ac = connect(f, a, c);
    for (const n of [a, b]) await appendN(f, n, 10, "old");
    await advance(f, 6_000);
    ac.ha.detach();
    await appendN(f, a, 30, "missed-by-c");
    await advance(f, 5_000);
    f.skew.v = 100 * DAY;
    await appendN(f, a, 3, "recent");
    await advance(f, 5_000);
    const r = await prune(f, a, { maxLagMs: 60 * DAY });
    expect(r.members.find((m) => m.node === "wC")!.excluded).toBe("lagging");
    expect(r.writers.wA!.floor).toBe(40);
    expect(contig(c, "wA")).toBe(10);

    connect(f, c, a); // c comes back
    await advance(f, 8_000);
    expect(contig(c, "wA")).toBe(43);
    expect(pending(c)).toBe(0);
    expect(rows(c, "wA").map((e) => e.seq).filter((s) => s > 40)).toEqual([41, 42, 43]);
    // c's pre-floor local history is untouched until its own sweep drops it
    expect(rows(c, "wA").filter((e) => e.seq <= 10)).toHaveLength(10);
    const rc = await prune(f, c, { maxLagMs: 60 * DAY });
    expect(rc.writers.wA!.floor).toBe(40);
    expect(rows(c, "wA").map((e) => e.seq)).toEqual([41, 42, 43]);
  });

  it("an old (proto < 3) peer below the floor gets the empty completion — no TRUNCATED, no pending growth, no spin", async () => {
    const { f, a } = await meshOf3(522);
    f.skew.v = 40 * DAY;
    await appendN(f, a, 2, "recent");
    await advance(f, 5_000);
    await prune(f, a);
    const old = makeNode(f, "wO");
    const before = a.sent.length;
    connect(f, old, a, { protoMax: 2 });
    await advance(f, 20_000); // ten anti-entropy rounds
    // live appends mark the stream dirty toward the old peer: the eager push
    // must not park rows above the floor in its sq_pending (it can never
    // drain them — it cannot get past the floor)
    await appendN(f, a, 3, "live");
    await advance(f, 4_000);
    const toOld = a.sent.slice(before);
    expect(toOld).not.toContain("TRUNCATED");
    for (const w of ["wA", "wB", "wC"]) expect(contig(old, w)).toBe(0);
    expect(pending(old)).toBe(0);
    const unservable = a.anomalies.filter((x) => x.kind === "floor_unservable");
    expect(unservable.map((x) => x.writer).sort()).toEqual(["wA", "wB", "wC"]); // once per stream
    const st = a.node.stats().topics[T]!;
    // P22 parks each stream after one non-progress round; HAVE rounds re-drive
    // it once per ANTI_ENTROPY_MS: 24 s = 12 rounds, bounded by streams ×
    // (rounds + 3). A spin would be thousands (one per round trip).
    expect(st.retention!.truncatedUnservable).toBeGreaterThanOrEqual(3);
    expect(st.retention!.truncatedUnservable).toBeLessThanOrEqual(3 * 15);
    // the old peer still acknowledges through HAVE, so it pins new pruning
    expect(st.retention!.ackNodes).toBe(3);
  });

  it("export carries the floors and a fresh import adopts them instead of parking rows in pending", async () => {
    const { f, a } = await meshOf3(523);
    f.skew.v = 40 * DAY;
    await advance(f, 5_000);
    await prune(f, a);
    const lines: string[] = [];
    for await (const l of a.node.export(T, "jsonl")) lines.push(l);
    expect(JSON.parse(lines[0]!).floors).toEqual({
      wA: { seq: 19, chain: expect.any(String) },
      wB: { seq: 19, chain: expect.any(String) },
      wC: { seq: 19, chain: expect.any(String) },
    });
    const e = makeNode(f, "wE");
    const imported = drive(
      f,
      e.node.import(
        T,
        (async function* () {
          yield* lines;
        })(),
      ),
    );
    await advance(f, 500);
    expect(await imported).toBe(3);
    for (const w of ["wA", "wB", "wC"]) expect(contig(e, w)).toBe(20);
    expect(pending(e)).toBe(0);
  });
});

// Randomized deterministic simulation: four nodes on lossy/duplicating links,
// ~90 virtual days of appends, nodes dropping offline for up to several weeks
// (some past max-lag, so the others prune past them and they come back via
// TRUNCATED), and pruneAcked run at random on every online node. At the end
// the fleet must be one replica again: equal heads, nothing parked in
// sq_pending, identical rows wherever two nodes both hold a seq, every row
// newer than the window held everywhere, and window-bounded consumers equal.
describe("simulation — churn, loss and periodic pruneAcked converge", () => {
  for (const seed of [601, 602, 603, 604, 605, 606]) {
    it(`seed ${seed}`, async () => {
      const f = fleet(seed);
      const wl = f.rng.substream("workload");
      const ids = ["wA", "wB", "wC", "wD"];
      const nodes = ids.map((id) => makeNode(f, id));
      const online = new Set(ids);
      const links = new Map<string, PeerHandle>();
      const dial = (i: number, j: number): void => {
        const key = `${ids[i]}-${ids[j]}`;
        if (links.has(key) || !online.has(ids[i]!) || !online.has(ids[j]!)) return;
        const link = new VirtualLink(f.sched, f.rng.substream(`${key}@${f.sched.now()}`), {
          lossP: 0.02,
          dupP: 0.02,
        });
        const grants = { [T]: "full" as const };
        const ha = nodes[i]!.node.attach(link.a, { peerId: ids[j]!, peerClass: "content", grants });
        nodes[j]!.node.attach(link.b, { peerId: ids[i]!, peerClass: "content", grants });
        links.set(key, ha);
        ha.onStateChange((s) => {
          if (s !== "closed") return;
          links.delete(key);
          f.sched.schedule(f.sched.now() + 100, () => dial(i, j)); // host redial
        });
      };
      const dialAll = () => {
        for (let i = 0; i < ids.length; i++) for (let j = i + 1; j < ids.length; j++) dial(i, j);
      };
      const takeOffline = (id: string) => {
        online.delete(id);
        for (const [key, h] of [...links]) if (key.split("-").includes(id)) h.detach();
      };
      dialAll();
      const offlineUntil = new Map<string, number>();
      for (let day = 0; day < 90; day += 3) {
        f.skew.v = day * DAY;
        for (const [id, until] of [...offlineUntil])
          if (day >= until) {
            offlineUntil.delete(id);
            online.add(id);
          }
        dialAll();
        if (wl.next() < 0.3 && offlineUntil.size < 2) {
          const id = ids[Math.floor(wl.next() * ids.length)]!;
          if (online.has(id)) {
            takeOffline(id);
            offlineUntil.set(id, day + 3 * (1 + Math.floor(wl.next() * 15))); // 3..45 days
          }
        }
        for (const n of nodes) {
          const k = Math.floor(wl.next() * 5);
          for (let i = 0; i < k; i++) void n.node.log(T).append("ev", { day, i });
        }
        await advance(f, 8_000);
        for (const n of nodes) {
          if (!online.has(n.id) || wl.next() < 0.5) continue;
          await prune(f, n);
        }
      }
      for (const id of ids) online.add(id);
      offlineUntil.clear();
      dialAll();
      await advance(f, 60_000);

      const now = f.sched.now() + f.skew.v;
      for (const w of ids) {
        const heads = nodes.map((n) => contig(n, w));
        expect(new Set(heads).size, `${w} heads ${heads.join(",")}`).toBe(1);
        const bySeq = new Map<number, string>();
        for (const n of nodes)
          for (const e of rows(n, w)) {
            const prev = bySeq.get(e.seq);
            if (prev !== undefined) expect(e.chain).toBe(prev);
            bySeq.set(e.seq, e.chain);
          }
        // every in-window row of the stream is held by every node
        for (const n of nodes) {
          const held = new Set(rows(n, w).map((e) => e.seq));
          for (const other of nodes)
            for (const e of rows(other, w)) if (e.hlc.l >= now - WINDOW) expect(held.has(e.seq)).toBe(true);
        }
      }
      for (const n of nodes) expect(pending(n), n.id).toBe(0);
      const ref = windowed(nodes[0]!, now);
      expect(ref.length).toBeGreaterThan(0);
      for (const n of nodes.slice(1)) expect(windowed(n, now), n.id).toEqual(ref);
      // pruning happened, and at least one bootstrap did
      expect(nodes.reduce((s, n) => s + n.node.stats().topics[T]!.retention!.prunedRows, 0)).toBeGreaterThan(0);
    });
  }
});
