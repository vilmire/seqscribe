// §10 subscriptions: SNAP/DELTA lifecycle, cursor resume from the delta
// journal, epoch reset on recompute, future-cursor recovery, ring tail, ACL.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { chainOf, coreOf, createSeqscribe, seedOf } from "../src/index.js";
import type {
  Constants,
  LogEntry,
  Row,
  SeqscribeNode,
  Subscription,
  TopicPolicy,
  ViewDef,
} from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
const TEST_CONSTANTS: Partial<Constants> = {
  ANTI_ENTROPY_MS: 60_000, // keep HAVE noise out of these tests
  CONTROL_RETRY_MS: 200,
  CHANNEL_STALL_MS: 30_000,
};

type CountState = { [kind: string]: number };
const countView: ViewDef<CountState, { kind: string; n: number }> = {
  version: "1",
  init: {},
  reduce: (s, e) => ({ ...s, [e.kind]: (s[e.kind] ?? 0) + 1 }),
  rows: (s) => Object.entries(s).map(([kind, n]) => ({ kind, n })),
  rowKey: "kind",
  schema: { kind: "TEXT", n: "INTEGER" },
  delta: (s, e) => ({ upserts: [{ kind: e.kind, n: (s[e.kind] ?? 0) + 1 }], deletes: [] }),
};

function makeServer(sched: Scheduler): SeqscribeNode {
  const node = createSeqscribe({
    writerId: "wA",
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.42,
    constants: TEST_CONSTANTS,
  });
  node.defineTopic(T, FULL);
  node.view("counts", T, countView);
  return node;
}

function makeClient(sched: Scheduler): SeqscribeNode {
  const node = createSeqscribe({
    writerId: "wB",
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.43,
    constants: TEST_CONSTANTS,
  });
  node.defineTopic(T, FULL);
  return node;
}

function connectServeOnly(
  sched: Scheduler,
  rng: SeededRng,
  server: SeqscribeNode,
  client: SeqscribeNode,
  serverMode: "serve" | "none" = "serve",
) {
  const link = new VirtualLink(sched, rng);
  server.attach(link.a, { peerId: "wB", peerClass: "content", grants: { [T]: serverMode } });
  const handle = client.attach(link.b, {
    peerId: "wA",
    peerClass: "content",
    grants: { [T]: "none" },
  });
  return handle;
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

describe("subscriptions", () => {
  it("SNAP on subscribe, DELTA on subsequent changes", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(31);
    const server = makeServer(sched);
    const client = makeClient(sched);
    void server.log(T).append("a", {});
    await sched.run({ untilMs: 100 });

    const handle = connectServeOnly(sched, rng, server, client);
    await sched.run({ untilMs: 300 });
    const sub = client.subscribe(handle, { view: "counts", params: null });
    const got = collect(sub);
    await sched.run({ untilMs: 600 });

    expect(got.snapshots).toHaveLength(1);
    expect(got.snapshots[0]!.reset).toBe(true);
    expect(got.snapshots[0]!.rows).toEqual([{ kind: "a", n: 1 }]);
    expect(sub.cursor).toBeDefined();

    void server.log(T).append("b", {});
    await sched.run({ untilMs: 900 });
    expect(got.deltas).toHaveLength(1);
    expect(got.deltas[0]!.upserts).toEqual([{ kind: "b", n: 1 }]);
  });

  it("resumes from a held cursor via the delta journal — no SNAP", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(32);
    const server = makeServer(sched);
    const client = makeClient(sched);
    const handle = connectServeOnly(sched, rng, server, client);
    await sched.run({ untilMs: 300 });

    const sub1 = client.subscribe(handle, { view: "counts", params: null });
    const got1 = collect(sub1);
    await sched.run({ untilMs: 600 });
    void server.log(T).append("a", {});
    await sched.run({ untilMs: 900 });
    expect(got1.deltas).toHaveLength(1);
    const held = sub1.cursor!;
    sub1.close();
    await sched.run({ untilMs: 1_000 });

    // changes while unsubscribed
    void server.log(T).append("b", {});
    void server.log(T).append("c", {});
    await sched.run({ untilMs: 1_400 });

    const sub2 = client.subscribe(handle, { view: "counts", params: null, fromCursor: held });
    const got2 = collect(sub2);
    await sched.run({ untilMs: 1_800 });
    expect(got2.snapshots).toHaveLength(0); // within retention → DELTA resume
    expect(got2.deltas.length).toBeGreaterThanOrEqual(1); // batched commits may merge deltas
    const kinds = got2.deltas.flatMap((d) => d.upserts.map((u) => u.kind));
    expect(kinds).toContain("b");
    expect(kinds).toContain("c");
  });

  it("recovers from a future cursor by re-subscribing fresh", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(33);
    const server = makeServer(sched);
    const client = makeClient(sched);
    const handle = connectServeOnly(sched, rng, server, client);
    await sched.run({ untilMs: 300 });

    const sub0 = client.subscribe(handle, { view: "counts", params: null });
    await sched.run({ untilMs: 600 });
    const cur = JSON.parse(sub0.cursor!) as { e: string; d: number };
    sub0.close();

    const future = JSON.stringify({ e: cur.e, d: cur.d + 100 });
    const sub = client.subscribe(handle, { view: "counts", params: null, fromCursor: future });
    const got = collect(sub);
    await sched.run({ untilMs: 1_200 });
    expect(got.snapshots).toHaveLength(1); // ERR_FUTURE_CURSOR → fresh SUB → SNAP reset
    expect(got.snapshots[0]!.reset).toBe(true);
  });

  it("pushes SNAP(reset) when a late arrival forces a recompute (epoch bump)", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(34);
    const server = makeServer(sched);
    const client = makeClient(sched);
    void server.log(T).append("a", {});
    await sched.run({ untilMs: 1_000_200 });
    const handle = connectServeOnly(sched, rng, server, client);
    await sched.run({ untilMs: 1_000_500 });
    const sub = client.subscribe(handle, { view: "counts", params: null });
    const got = collect(sub);
    await sched.run({ untilMs: 1_000_800 });
    expect(got.snapshots).toHaveLength(1);

    // late arrival below the server's watermark → recompute → epoch bump → SNAP
    const late: LogEntry = {
      topic: T,
      writer: "w0",
      seq: 1,
      hlc: { l: 900_000, c: 0 },
      kind: "z",
      payload: {},
      chain: "",
    };
    late.chain = chainOf(seedOf(T, "w0"), late);
    void coreOf(server).applyExternal(late, "peer");
    await sched.run({ untilMs: 1_001_500 });

    expect(got.snapshots.length).toBeGreaterThanOrEqual(2);
    const lastSnap = got.snapshots[got.snapshots.length - 1]!;
    expect(lastSnap.rows).toEqual([
      { kind: "a", n: 1 },
      { kind: "z", n: 1 },
    ]);
  });

  it("serves ring topics through the built-in tail view", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(35);
    const RING = "t.ring";
    const server = makeServer(sched);
    const client = makeClient(sched);
    const ringPolicy: TopicPolicy = {
      kind: "append",
      retention: { mode: "ring", size: 3 },
      replication: "subscribe-only",
      access: "content",
    };
    server.defineTopic(RING, ringPolicy);
    client.defineTopic(RING, ringPolicy);
    for (let i = 0; i < 5; i++) void server.log(RING).append("tick", { i });
    await sched.run({ untilMs: 100 });

    const link = new VirtualLink(sched, rng);
    server.attach(link.a, {
      peerId: "wB",
      peerClass: "content",
      grants: { [T]: "serve", [RING]: "serve" },
    });
    const handle = client.attach(link.b, {
      peerId: "wA",
      peerClass: "content",
      grants: { [T]: "none", [RING]: "none" },
    });
    await sched.run({ untilMs: 400 });

    const sub = client.subscribe(handle, { view: "tail", params: { topic: RING } });
    const got = collect(sub);
    await sched.run({ untilMs: 800 });
    expect(got.snapshots).toHaveLength(1);
    expect(got.snapshots[0]!.rows.map((r) => r.seq)).toEqual([3, 4, 5]); // ring size 3

    void server.log(RING).append("tick", { i: 5 });
    await sched.run({ untilMs: 1_200 });
    expect(got.deltas).toHaveLength(1);
    expect(got.deltas[0]!.upserts[0]!.seq).toBe(6);
  });

  it("denies subscriptions on ungranted topics", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(36);
    const server = makeServer(sched);
    const client = makeClient(sched);
    const handle = connectServeOnly(sched, rng, server, client, "none");
    await sched.run({ untilMs: 300 });
    const sub = client.subscribe(handle, { view: "counts", params: null });
    const got = collect(sub);
    await sched.run({ untilMs: 800 });
    expect(got.snapshots).toHaveLength(0);
    expect(got.deltas).toHaveLength(0);
  });
});
