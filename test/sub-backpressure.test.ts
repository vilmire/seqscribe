// §10 subscriptions under a backpressured data lane (§5.2 SEND_QUEUE_CAP).
//
// Incident this pins (2026-09-27, embedder daemon): a `full`+subscribe-only
// tail topic receiving a burst of large rows, served to a subscriber whose
// ACKs lag. Once the peer's data-lane queue filled, every DELTA was
// tail-dropped and `sendDelta` downgraded to `sendSnap(reset)` — which re-read
// the whole tail (FULL_TAIL_DEFAULT rows) and re-serialized it on EVERY
// applied write, while its own chunks were tail-dropped by the same full
// queue. Cost was O(writes × tail bytes) on the event loop (seconds per 10 s
// window), and the subscriber silently missed rows because no SNAP ever
// landed whole.
//
// The contract pinned here:
//   - N writes against a backpressured subscriber produce at most ONE resync
//     SNAP, generated once the queue drains — never one per write;
//   - a SNAP larger than SEND_QUEUE_CAP chunks is paced by capacity, so it
//     arrives whole instead of being tail-dropped;
//   - after the resync the subscriber holds exactly the served tail and
//     resumes on DELTAs;
//   - an oversized DELTA takes the same coalesced path;
//   - per-write event-loop cost stays bounded regardless of tail size.

import { afterEach, describe, expect, it, vi } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe } from "../src/index.js";
import type {
  Anomaly,
  Channel,
  Constants,
  Row,
  SeqscribeNode,
  SeqscribeNodeExt,
  TopicPolicy,
} from "../src/index.js";
import { LogCore } from "../src/log.js";
import { SubHub } from "../src/subs.js";

const FULL_SUBSCRIBE_ONLY: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "subscribe-only",
  access: "metadata",
};

const BASE: Partial<Constants> = {
  ANTI_ENTROPY_MS: 600_000, // keep HAVE noise out
  CONTROL_RETRY_MS: 2_000,
  CHANNEL_STALL_MS: 120_000,
};

function makeNode(sched: Scheduler, writerId: string, constants: Partial<Constants> = BASE): SeqscribeNode {
  return createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.37,
    constants: { ...BASE, ...constants },
  });
}

interface WireSnap {
  subId: number;
  chunk: number;
  of: number;
  cursor: string;
  reset: boolean;
}

// Wraps the server-side channel so the test sees exactly what went on the wire.
function tap(ch: Channel, snaps: WireSnap[], deltas: { cursor: string }[]): Channel {
  return {
    send: (m: string) => {
      const p = JSON.parse(m) as { t: string } & WireSnap;
      if (p.t === "SNAP") snaps.push({ subId: p.subId, chunk: p.chunk, of: p.of, cursor: p.cursor, reset: p.reset });
      if (p.t === "DELTA") deltas.push({ cursor: p.cursor });
      ch.send(m);
    },
    onMessage: (cb) => ch.onMessage(cb),
    onClose: (cb) => ch.onClose(cb),
    close: () => ch.close(),
  };
}

// Client-side materialization of the tail: SNAP(reset) replaces, DELTA upserts.
function mirror(sub: ReturnType<SeqscribeNode["subscribe"]>) {
  const rows = new Map<string, Row>();
  const log = { snaps: 0, deltas: 0 };
  sub.onSnapshot((snap, reset) => {
    log.snaps++;
    if (reset) rows.clear();
    for (const r of snap) rows.set(String(r.key), r);
  });
  sub.onDelta((d) => {
    log.deltas++;
    for (const r of d.upserts) rows.set(String(r.key), r);
    for (const k of d.deletes) rows.delete(k);
  });
  return { rows, log };
}

async function setup(opts: {
  seed: number;
  topic: string;
  latencyMs: number;
  constants?: Partial<Constants>;
  preload?: number;
  preloadBytes?: number;
}) {
  const sched = new Scheduler(0);
  const rng = new SeededRng(opts.seed);
  const server = makeNode(sched, "server", opts.constants);
  const client = makeNode(sched, "client", opts.constants);
  const anomalies: Anomaly[] = [];
  server.onAnomaly((a) => anomalies.push(a));
  server.defineTopic(opts.topic, FULL_SUBSCRIBE_ONLY);
  client.defineTopic(opts.topic, FULL_SUBSCRIBE_ONLY);
  const fill = "x".repeat(opts.preloadBytes ?? 16);
  for (let i = 0; i < (opts.preload ?? 0); i++) void server.log(opts.topic).append("row", { i, fill });
  await sched.run({ untilMs: 200 });

  const link = new VirtualLink(sched, rng, { latency: () => opts.latencyMs });
  const snaps: WireSnap[] = [];
  const deltas: { cursor: string }[] = [];
  server.attach(tap(link.a, snaps, deltas), {
    peerId: "client",
    peerClass: "metadata",
    grants: { [opts.topic]: "serve" },
  });
  const handle = client.attach(link.b, {
    peerId: "server",
    peerClass: "metadata",
    grants: { [opts.topic]: "none" },
  });
  await sched.run({ untilMs: sched.now() + opts.latencyMs * 4 });
  const sub = client.subscribe(handle, { view: "tail", params: { topic: opts.topic } });
  const m = mirror(sub);
  return { sched, server, client, sub, m, snaps, deltas, anomalies };
}

// Distinct SNAP generations on the wire (one per (subId, cursor)).
function snapGenerations(snaps: WireSnap[]): number {
  return new Set(snaps.map((s) => `${s.subId} ${s.cursor}`)).size;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("tail SUB under data-lane backpressure", () => {
  it("N writes against a backpressured subscriber produce at most one resync SNAP, then DELTAs resume", async () => {
    const topic = "session.bp1.transcript";
    const s = await setup({ seed: 901, topic, latencyMs: 300, preload: 5 });
    await s.sched.run({ untilMs: s.sched.now() + 2_000 });
    expect(s.m.log.snaps).toBe(1);
    expect(snapGenerations(s.snaps)).toBe(1);

    const tailReads = vi.spyOn(LogCore.prototype, "fullTail");
    const N = 300; // far beyond INFLIGHT_CREDITS + SEND_QUEUE_CAP (68)
    const fill = "y".repeat(2_048);
    for (let i = 0; i < N; i++) void s.server.log(topic).append("row", { i, fill });
    await s.sched.run({ untilMs: s.sched.now() + 60_000 });

    // The storm signature was one full-tail read per applied write (~N).
    expect(tailReads.mock.calls.length).toBeLessThanOrEqual(1);
    // exactly one extra SNAP generation for the whole burst
    expect(snapGenerations(s.snaps)).toBeLessThanOrEqual(2);

    // converged: the subscriber holds every served row, no silent gap
    expect(s.m.rows.size).toBe(5 + N);
    const seqs = [...s.m.rows.values()].map((r) => Number(r.seq)).sort((a, b) => a - b);
    expect(seqs[0]).toBe(1);
    expect(seqs[seqs.length - 1]).toBe(5 + N);

    // back on the steady-state path: one write → one DELTA, no SNAP
    const snapsBefore = s.snaps.length;
    const deltasBefore = s.m.log.deltas;
    void s.server.log(topic).append("row", { i: N, fill: "z" });
    await s.sched.run({ untilMs: s.sched.now() + 2_000 });
    expect(s.snaps.length).toBe(snapsBefore);
    expect(s.m.log.deltas).toBe(deltasBefore + 1);
    expect(s.m.rows.size).toBe(5 + N + 1);
    expect(tailReads.mock.calls.length).toBeLessThanOrEqual(1);

    // observability: one resync entered, the burst's withheld writes counted,
    // and exactly one anomaly for it (the host rate-limits its WARN on this)
    const st = (s.server as SeqscribeNodeExt).stats().subs!;
    expect(st.resyncs).toBe(1);
    expect(st.resyncsBackpressure).toBe(1);
    expect(st.resyncWritesCoalesced).toBeGreaterThan(N / 2);
    expect(st.snapsStarted).toBe(2); // initial + the one resync
    expect(st.snapsCompleted).toBe(2);
    expect(st.resyncPending).toBe(0);
    expect(st.snapsInFlight).toBe(0);
    expect(st.subscribers).toBe(1);
    expect(s.anomalies.filter((a) => a.kind === "sub_resync")).toEqual([
      { kind: "sub_resync", topic, peerId: "client", view: "tail", reason: "backpressure" },
    ]);
  });

  it("a SNAP larger than SEND_QUEUE_CAP chunks is paced by capacity and arrives whole", async () => {
    // Small frames force a many-chunk SNAP: 400 rows × ~300 B ≈ 120 KB body
    // over a ~1.5 KB raw chunk budget → ~80 chunks > SEND_QUEUE_CAP (64).
    const topic = "session.bp2.transcript";
    const s = await setup({
      seed: 902,
      topic,
      latencyMs: 50,
      constants: { MAX_FRAME_BYTES: 4_096 },
      preload: 400,
      preloadBytes: 200,
    });
    await s.sched.run({ untilMs: s.sched.now() + 30_000 });
    const of = s.snaps[0]?.of ?? 0;
    expect(of).toBeGreaterThan(64);
    expect(s.m.log.snaps).toBe(1);
    expect(s.m.rows.size).toBe(400);
  });

  it("oversized DELTAs take the same coalesced resync path (one SNAP, not one per write)", async () => {
    const topic = "session.bp3.transcript";
    const s = await setup({
      seed: 903,
      topic,
      latencyMs: 20,
      constants: { MAX_FRAME_BYTES: 8_192 },
      preload: 3,
    });
    await s.sched.run({ untilMs: s.sched.now() + 1_000 });
    expect(s.m.log.snaps).toBe(1);

    const tailReads = vi.spyOn(LogCore.prototype, "fullTail");
    // each row's DELTA frame exceeds MAX_FRAME_BYTES (8 KiB); one group commit
    const big = "q".repeat(9_000);
    const N = 40;
    for (let i = 0; i < N; i++) void s.server.log(topic).append("row", { i, big });
    await s.sched.run({ untilMs: s.sched.now() + 20_000 });

    expect(tailReads.mock.calls.length).toBeLessThanOrEqual(1);
    expect(snapGenerations(s.snaps)).toBeLessThanOrEqual(2);
    expect(s.m.rows.size).toBe(3 + N);
    const st = (s.server as SeqscribeNodeExt).stats().subs!;
    expect(st.resyncsOversized).toBe(1);
    expect(s.anomalies.filter((a) => a.kind === "sub_resync").map((a) => a.reason)).toEqual(["oversized"]);
  });

  it("per-write event-loop cost stays bounded with a 500-row × 36 KiB tail under backpressure", async () => {
    const topic = "session.bp4.transcript";
    const s = await setup({ seed: 904, topic, latencyMs: 400, preload: 500, preloadBytes: 36 * 1024 });
    // let the initial SNAP go out (its delivery is pinned by the pacing test
    // above; this one measures only per-write cost)
    await s.sched.run({ untilMs: s.sched.now() + 120_000 });

    const durations: number[] = [];
    const orig = SubHub.prototype.handleTailApplied;
    vi.spyOn(SubHub.prototype, "handleTailApplied").mockImplementation(function (this: SubHub, e) {
      const t0 = performance.now();
      orig.call(this, e);
      durations.push(performance.now() - t0);
    });
    const tailReads = vi.spyOn(LogCore.prototype, "fullTail");
    const fill = "w".repeat(36 * 1024);
    for (let i = 0; i < 120; i++) void s.server.log(topic).append("row", { i, fill });
    // only run far enough for the burst to apply and the queue to be full —
    // the per-write cost is what is measured here, not the recovery
    await s.sched.run({ untilMs: s.sched.now() + 200 });

    expect(durations.length).toBe(120);
    // old behavior: each write past the queue cap re-read and re-encoded the
    // whole ~18 MB tail (hundreds of ms per write)
    expect(tailReads.mock.calls.length).toBe(0);
    expect(Math.max(...durations)).toBeLessThan(25);
  }, 120_000);
});
