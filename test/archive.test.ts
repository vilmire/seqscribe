// §7.6 cold archiving: consumer-gated movement out of the hot log,
// consumer_abandoned drops, post-cut cursor resume, base-checkpoint rebuilds.

import { describe, expect, it } from "vitest";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { coreOf, createSeqscribe } from "../src/index.js";
import type {
  Anomaly,
  Constants,
  FinalityCert,
  SeqscribeNode,
  TopicPolicy,
  ViewDef,
} from "../src/index.js";
import type { Store } from "../src/store.js";
import type { ViewHub } from "../src/views.js";

const T = "t.ledger";
const AUTH = "test:authority";
const POLICY: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
  finalityAuthority: AUTH,
};
const TEST_CONSTANTS: Partial<Constants> = { FINALITY_WINDOW_MS: 50_000 };

type CountState = { [kind: string]: number };
const countView: ViewDef<CountState, { kind: string; n: number }> = {
  version: "1",
  init: {},
  reduce: (s, e) => ({ ...s, [e.kind]: (s[e.kind] ?? 0) + 1 }),
  rows: (s) => Object.entries(s).map(([kind, n]) => ({ kind, n })),
  rowKey: "kind",
  schema: { kind: "TEXT", n: "INTEGER" },
};

function storeOf(n: SeqscribeNode): Store {
  return (n as unknown as { _core: { store: Store } })._core.store;
}

function makeNode(sched: Scheduler): { node: SeqscribeNode; anomalies: Anomaly[] } {
  const anomalies: Anomaly[] = [];
  const node = createSeqscribe({
    writerId: "wA",
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.5,
    constants: TEST_CONSTANTS,
    authority: { verifyFinality: (c) => c.sig === "valid-sig" },
  });
  node.onAnomaly((a) => anomalies.push(a));
  node.defineTopic(T, POLICY);
  return { node, anomalies };
}

async function certify(sched: Scheduler, node: SeqscribeNode): Promise<FinalityCert> {
  const cert: FinalityCert = { ...node.proposeFinality(T)!, sig: "valid-sig" };
  const p = node.ingestFinality(cert);
  await sched.run({ untilMs: sched.now() + 1_000 });
  await p;
  return cert;
}

describe("cold archiving (§7.6)", () => {
  it("archives the covered region once no consumer is behind it", async () => {
    const sched = new Scheduler(1_000_000);
    const { node } = makeNode(sched);
    for (let i = 0; i < 5; i++) void node.log(T).append("old", { i });
    await sched.run({ untilMs: 1_100_000 });
    await certify(sched, node);
    void node.log(T).append("new", { post: true });
    await sched.run({ untilMs: sched.now() + 500 });

    const store = storeOf(node);
    expect(store.archivedCount(T)).toBe(5); // covered region moved cold
    expect(coreOf(node).entries(T, "wA", 1, 5)).toEqual([]); // out of the hot log
    expect(store.archivedEntries(T, "wA", 1, 5)).toHaveLength(5); // still readable locally
    expect(coreOf(node).getStream(T, "wA").contigSeq).toBe(6); // stream unaffected
  });

  it("holds archiving while a live consumer lags, then proceeds after it drains", async () => {
    const sched = new Scheduler(1_000_000);
    const { node } = makeNode(sched);
    let block = true;
    const seen: number[] = [];
    node.onEntry(T, "slow", async (e) => {
      if (block) throw new Error("not yet");
      seen.push(e.seq);
    });
    for (let i = 0; i < 4; i++) void node.log(T).append("old", { i });
    await sched.run({ untilMs: 1_100_000 });
    await certify(sched, node);

    const store = storeOf(node);
    expect(store.archivedCount(T)).toBe(0); // the lagging consumer gates it

    block = false;
    await sched.run({ untilMs: sched.now() + 60_000 }); // let the backoff retry drain
    expect(seen).toEqual([1, 2, 3, 4]); // at-least-once delivery completed first
    expect(store.archivedCount(T)).toBe(4); // then the region went cold
  });

  it("drops a consumer idle past the window and resumes it post-cut", async () => {
    const sched = new Scheduler(1_000_000);
    const { node, anomalies } = makeNode(sched);
    // a consumer that saw entry 1 and then went away
    const unsub = node.onEntry(T, "sleeper", async (e) => {
      if (e.seq >= 2) throw new Error("gone to sleep");
    });
    for (let i = 0; i < 3; i++) void node.log(T).append("old", { i });
    await sched.run({ untilMs: 1_000_500 });
    unsub(); // process went away; its cursor row stays at entry 1

    await sched.run({ untilMs: 1_100_000 }); // idle past FINALITY_WINDOW_MS
    await certify(sched, node);

    expect(anomalies.some((a) => a.kind === "consumer_abandoned")).toBe(true);
    expect(storeOf(node).archivedCount(T)).toBe(3); // no longer gated

    // resume: cursor row is gone; the hot log starts post-cut
    void node.log(T).append("new", { post: true });
    const resumed: number[] = [];
    node.onEntry(T, "sleeper", (e) => {
      resumed.push(e.seq);
    });
    await sched.run({ untilMs: sched.now() + 1_000 });
    expect(resumed).toEqual([4]); // first post-cut entry; archived middle skipped
  });

  it("rebuilds views from the permanent base checkpoint after archiving", async () => {
    const sched = new Scheduler(1_000_000);
    const { node } = makeNode(sched);
    node.view("counts", T, countView);
    for (let i = 0; i < 5; i++) void node.log(T).append("old", { i });
    await sched.run({ untilMs: 1_100_000 });
    await certify(sched, node);
    void node.log(T).append("new", { post: true });
    await sched.run({ untilMs: sched.now() + 500 });

    const hub = (node as unknown as { _views: ViewHub })._views;
    expect(hub.tableRowsSorted("counts")).toEqual([
      { kind: "new", n: 1 },
      { kind: "old", n: 5 },
    ]);
    // rebuild with the pre-cut rows archived away — the base checkpoint carries them
    await node.rebuildView("counts");
    await sched.run({ untilMs: sched.now() + 500 });
    expect(hub.tableRowsSorted("counts")).toEqual([
      { kind: "new", n: 1 },
      { kind: "old", n: 5 },
    ]);
  });

  it("exports declare a cut base once pre-cut rows are archived", async () => {
    const sched = new Scheduler(1_000_000);
    const { node } = makeNode(sched);
    for (let i = 0; i < 3; i++) void node.log(T).append("old", { i });
    await sched.run({ untilMs: 1_100_000 });
    const cert = await certify(sched, node);
    void node.log(T).append("new", {});
    await sched.run({ untilMs: sched.now() + 500 });

    const lines: string[] = [];
    for await (const line of node.export(T, "jsonl")) lines.push(line);
    const header = JSON.parse(lines[0]!) as { base: unknown };
    expect(header.base).toEqual({ order: cert.order, cut: cert.cut }); // partial export
    expect(lines).toHaveLength(2); // header + the one post-cut entry
  });
});
