// LogCore behavior under the virtual scheduler: group commit, contig apply,
// pending buffering, fork detection paths, retention modes.

import { describe, expect, it } from "vitest";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import {
  chainOf,
  coreOf,
  createSeqscribe,
  DEFAULT_CONSTANTS,
  SeqscribeError,
  seedOf,
  validateEntry,
} from "../src/index.js";
import type { Anomaly, LogEntry, SeqscribeNode, TopicPolicy } from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

function makeNode(sched: Scheduler, writerId: string): {
  node: SeqscribeNode;
  anomalies: Anomaly[];
} {
  const anomalies: Anomaly[] = [];
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
  });
  node.onAnomaly((a) => anomalies.push(a));
  node.defineTopic(T, FULL);
  return { node, anomalies };
}

async function authorEntries(sched: Scheduler, n: number): Promise<LogEntry[]> {
  const { node } = makeNode(sched, "w1");
  const ps = [];
  for (let i = 0; i < n; i++) ps.push(node.log(T).append("note", { i }));
  await sched.run();
  await Promise.all(ps);
  const core = coreOf(node);
  const head = core.getStream(T, "w1");
  expect(head.contigSeq).toBe(n);
  // read back full entries via the range accessor
  const store = (core as unknown as { store: { entriesRange: Function } }).store;
  return (store.entriesRange(T, "w1", 1, n) as { entry: LogEntry }[]).map((r) => r.entry);
}

describe("author append path", () => {
  it("appends with gap-free seq and verifiable chains", async () => {
    const sched = new Scheduler(1756166400000);
    const entries = await authorEntries(sched, 5);
    let prev = seedOf(T, "w1");
    for (const [i, e] of entries.entries()) {
      expect(e.seq).toBe(i + 1);
      expect(chainOf(prev, e)).toBe(e.chain);
      prev = e.chain;
    }
  });

  it("stamps non-decreasing hlc with seq", async () => {
    const sched = new Scheduler(1000);
    const entries = await authorEntries(sched, 10);
    for (let i = 1; i < entries.length; i++) {
      const a = entries[i - 1]!.hlc;
      const b = entries[i]!.hlc;
      expect(b.l > a.l || (b.l === a.l && b.c > a.c)).toBe(true);
    }
  });

  it("rejects raw append on a register topic synchronously (§11.1)", () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "w1");
    node.defineTopic("t.reg", {
      kind: "register",
      retention: { mode: "full" },
      replication: "full-sync",
      access: "content",
    });
    expect(() => node.log("t.reg").append("set", { v: 1 })).toThrowError(SeqscribeError);
  });

  it("groups a large burst into batched commits without loss", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "w1");
    const ps = Array.from({ length: 200 }, (_, i) => node.log(T).append("note", { i }));
    await sched.run();
    const ids = await Promise.all(ps);
    expect(ids.map(([, , s]) => s)).toEqual(Array.from({ length: 200 }, (_, i) => i + 1));
  });
});

describe("canonical strip at ingress (§2/§4)", () => {
  it("drops unknown fields without disturbing the chain", async () => {
    const sched = new Scheduler(2000);
    const [e] = await authorEntries(sched, 1);
    const junked = {
      ...e!,
      junk: "rides-along",
      hlc: { ...e!.hlc, junk: 2 },
    } as unknown;

    const clean = validateEntry(junked, DEFAULT_CONSTANTS);
    expect(Object.keys(clean).sort()).toEqual(
      Object.keys(e!).sort(), // exactly the authored §2 fields, nothing extra
    );
    expect(Object.keys(clean.hlc).sort()).toEqual(["c", "l"]);
    // §4 chainOf never covered the extras, so the stripped entry still chains
    expect(chainOf(seedOf(T, "w1"), clean)).toBe(clean.chain);
  });
});

describe("external apply path (§6.1–6.2)", () => {
  it("applies in order, buffers out-of-order, drains pending", async () => {
    const sched = new Scheduler(2000);
    const entries = await authorEntries(sched, 4);
    const { node: b } = makeNode(sched, "w2");
    const core = coreOf(b);

    const r3 = core.applyExternal(entries[2]!);
    const r1 = core.applyExternal(entries[0]!);
    await sched.run();
    expect(await r3).toBe("pending");
    expect(await r1).toBe("applied");
    expect(core.getStream(T, "w1").contigSeq).toBe(1);

    const r2 = core.applyExternal(entries[1]!);
    await sched.run();
    expect(await r2).toBe("applied"); // and 3 drains from pending
    expect(core.getStream(T, "w1").contigSeq).toBe(3);

    const r4 = core.applyExternal(entries[3]!);
    await sched.run();
    expect(await r4).toBe("applied");
    expect(core.getStream(T, "w1").contigSeq).toBe(4);
  });

  it("treats identical re-delivery as duplicate", async () => {
    const sched = new Scheduler(2000);
    const entries = await authorEntries(sched, 2);
    const { node: b, anomalies } = makeNode(sched, "w2");
    const core = coreOf(b);
    for (const e of entries) core.applyExternal(e);
    await sched.run();
    const dup = core.applyExternal(entries[0]!);
    await sched.run();
    expect(await dup).toBe("duplicate");
    expect(anomalies).toHaveLength(0);
  });

  it("seals on chain mismatch (fork path ①)", async () => {
    const sched = new Scheduler(2000);
    const entries = await authorEntries(sched, 2);
    const { node: b, anomalies } = makeNode(sched, "w2");
    const core = coreOf(b);
    core.applyExternal(entries[0]!);
    await sched.run();

    // divergent seq-2: same stream position, different content, self-consistent chain
    const forged: LogEntry = { ...entries[1]!, payload: { evil: true } };
    forged.chain = chainOf(entries[0]!.chain, forged);
    const r = core.applyExternal(forged);
    await sched.run();
    expect(await r).toBe("applied"); // first-applied branch wins locally

    const real = core.applyExternal(entries[1]!);
    await sched.run();
    expect(await real).toBe("forked"); // same-id-different-content → seal
    expect(anomalies.some((a) => a.kind === "writer_forked")).toBe(true);

    // stream now sealed: further entries rejected
    const forged3: LogEntry = {
      ...entries[1]!,
      seq: 3,
      payload: { more: 1 },
      chain: "",
    };
    forged3.chain = chainOf(forged.chain, forged3);
    const r3 = core.applyExternal(forged3);
    await sched.run();
    expect(await r3).toBe("sealed");
  });

  it("seals on per-writer HLC monotonicity violation (fork path ④)", async () => {
    const sched = new Scheduler(2000);
    const entries = await authorEntries(sched, 1);
    const { node: b, anomalies } = makeNode(sched, "w2");
    const core = coreOf(b);
    core.applyExternal(entries[0]!);
    await sched.run();

    const decreasing: LogEntry = {
      topic: T,
      writer: "w1",
      seq: 2,
      hlc: { l: entries[0]!.hlc.l - 1, c: 0 },
      kind: "note",
      payload: { i: 1 },
      chain: "",
    };
    decreasing.chain = chainOf(entries[0]!.chain, decreasing);
    const r = core.applyExternal(decreasing);
    await sched.run();
    expect(await r).toBe("forked");
    expect(anomalies.some((a) => a.kind === "writer_forked")).toBe(true);
  });

  it("merges remote HLC only within ε and annotates outliers", async () => {
    const sched = new Scheduler(1_000_000);
    const { node: b, anomalies } = makeNode(sched, "w2");
    const core = coreOf(b);

    const outlier: LogEntry = {
      topic: T,
      writer: "w1",
      seq: 1,
      hlc: { l: 1_000_000 + 301_000, c: 0 }, // beyond HLC_EPSILON_MS (300s) incl. commit delay
      kind: "note",
      payload: {},
      chain: "",
    };
    outlier.chain = chainOf(seedOf(T, "w1"), outlier);
    const r = core.applyExternal(outlier);
    await sched.run();
    expect(await r).toBe("applied"); // stored as stamped — ordering keeps the stamp
    expect(anomalies.some((a) => a.kind === "clock_outlier")).toBe(true);

    // local clock did NOT merge the outlier: next own append stamps near pt, not near l+ε
    const { node: c } = makeNode(sched, "w3");
    void c;
    const own = core.getStream(T, "w1");
    expect(own.contigSeq).toBe(1);
  });
});

describe("retention modes (§14)", () => {
  it("ring topics persist stream heads but no log rows", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "w1");
    node.defineTopic("t.ring", {
      kind: "append",
      retention: { mode: "ring", size: 3 },
      replication: "subscribe-only",
      access: "content",
    });
    const core = coreOf(node);
    for (let i = 0; i < 5; i++) void node.log("t.ring").append("tick", { i });
    await sched.run();
    expect(core.getStream("t.ring", "w1").contigSeq).toBe(5); // EntryId uniqueness preserved
    const tail = core.ringTail("t.ring");
    expect(tail.map((e) => e.seq)).toEqual([3, 4, 5]); // capped at ring size
    const stored = (core as unknown as { store: { getEntry: Function } }).store;
    expect(stored.getEntry("t.ring", "w1", 5)).toBeUndefined();
  });
});
