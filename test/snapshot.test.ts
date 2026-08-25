// §15 export/import and §7.7–7.8 snapshot bootstrap.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { coreOf, createSeqscribe } from "../src/index.js";
import type {
  Constants,
  FinalityCert,
  SeqscribeNode,
  TopicPolicy,
  ViewDef,
} from "../src/index.js";
import type { RegisterHub } from "../src/register.js";
import type { ViewHub } from "../src/views.js";

const T = "t.ledger";
const AUTH = "test:authority";
const TEST_CONSTANTS: Partial<Constants> = {
  FINALITY_WINDOW_MS: 50_000,
  ANTI_ENTROPY_MS: 1_000,
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
};

function makeNode(sched: Scheduler, writerId: string, policy: TopicPolicy): SeqscribeNode {
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.9,
    constants: TEST_CONSTANTS,
    authority: {
      verifyFinality: (c) => c.sig === "valid-sig",
      verifyWriterDirective: (d) => d.sig === "valid-sig",
    },
  });
  node.defineTopic(T, policy);
  return node;
}

function connect(sched: Scheduler, rng: SeededRng, a: SeqscribeNode, b: SeqscribeNode) {
  const link = new VirtualLink(sched, rng);
  a.attach(link.a, { peerId: "pB", peerClass: "content", grants: { [T]: "full" } });
  b.attach(link.b, { peerId: "pA", peerClass: "content", grants: { [T]: "full" } });
}

async function certify(sched: Scheduler, node: SeqscribeNode): Promise<FinalityCert> {
  const cert: FinalityCert = { ...node.proposeFinality(T)!, sig: "valid-sig" };
  const p = node.ingestFinality(cert);
  await sched.run({ untilMs: sched.now() + 1_000 });
  await p;
  return cert;
}

describe("export / import (§15)", () => {
  const PLAIN: TopicPolicy = {
    kind: "append",
    retention: { mode: "full" },
    replication: "full-sync",
    access: "content",
  };

  it("round-trips a topic with verified chains", async () => {
    const sched = new Scheduler(0);
    const a = makeNode(sched, "wA", PLAIN);
    for (let i = 0; i < 10; i++) void a.log(T).append("note", { i });
    await sched.run({ untilMs: 500 });

    const lines: string[] = [];
    for await (const line of a.export(T, "jsonl")) lines.push(line);
    expect(lines).toHaveLength(11); // header + 10 entries
    expect(JSON.parse(lines[0]!)).toMatchObject({ seqscribe: "export/v1", topic: T });

    const c = makeNode(sched, "wC", PLAIN);
    const importP = c.import(
      T,
      (async function* () {
        yield* lines;
      })(),
    );
    await sched.run({ untilMs: 1_500 });
    expect(await importP).toBe(10);
    expect(coreOf(c).getStream(T, "wA").contigSeq).toBe(10);
    expect(coreOf(c).entries(T, "wA", 1, 10)).toEqual(coreOf(a).entries(T, "wA", 1, 10));
  });

  it("routes a tampered import line to the fork path", async () => {
    const sched = new Scheduler(0);
    const a = makeNode(sched, "wA", PLAIN);
    for (let i = 0; i < 3; i++) void a.log(T).append("note", { i });
    await sched.run({ untilMs: 500 });
    const lines: string[] = [];
    for await (const line of a.export(T, "jsonl")) lines.push(line);
    // tamper entry 2's payload without recomputing the chain
    const tampered = JSON.parse(lines[2]!) as { payload: unknown };
    tampered.payload = { i: 999 };
    lines[2] = JSON.stringify(tampered);

    const c = makeNode(sched, "wC", PLAIN);
    const importP = c.import(
      T,
      (async function* () {
        yield* lines;
      })(),
    );
    await sched.run({ untilMs: 1_500 });
    expect(await importP).toBe(1); // entry 1 applied; 2 forked the stream
    expect(coreOf(c).getStream(T, "wA").sealReason).toBe("fork");
  });
});

describe("snapshot bootstrap (§7.8)", () => {
  const WITH_AUTH: TopicPolicy = {
    kind: "append",
    retention: { mode: "full" },
    replication: "full-sync",
    access: "content",
    finalityAuthority: AUTH,
  };

  it("bootstraps a fresh replica: cert → snapshot → post-cut replay", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(71);
    const a = makeNode(sched, "wA", WITH_AUTH);
    a.view("counts", T, countView);

    // 5 old entries (will be under P), then age past the window, then 2 recent
    for (let i = 0; i < 5; i++) void a.log(T).append("old", { i });
    await sched.run({ untilMs: 1_100_000 });
    const cert = await certify(sched, a);
    expect(cert.cut.wA!.seq).toBe(5);
    for (let i = 0; i < 2; i++) void a.log(T).append("new", { i });
    await sched.run({ untilMs: sched.now() + 500 });

    const b = makeNode(sched, "wB", WITH_AUTH);
    b.view("counts", T, countView);
    connect(sched, rng, a, b);
    await sched.run({ untilMs: sched.now() + 15_000 });

    // basis adopted at the cut; post-cut entries replayed on top
    const headB = coreOf(b).getStream(T, "wA");
    expect(headB.contigSeq).toBe(7);
    expect(headB.contigChain).toBe(coreOf(a).getStream(T, "wA").contigChain);
    expect(b.finality(T)).toEqual(cert);
    // pre-cut entries came via the snapshot, not the log
    expect(coreOf(b).entries(T, "wA", 1, 5)).toEqual([]);
    expect(coreOf(b).entries(T, "wA", 6, 7)).toEqual(coreOf(a).entries(T, "wA", 6, 7));

    // view state: snapshot base + post-cut fold == authority's full fold
    const hubA = (a as unknown as { _views: ViewHub })._views;
    const hubB = (b as unknown as { _views: ViewHub })._views;
    await sched.run({ untilMs: sched.now() + 1_000 });
    expect(hubB.tableRowsSorted("counts")).toEqual(hubA.tableRowsSorted("counts"));
    expect(hubB.isBootstrapPartial("counts")).toBe(false);
  });

  it("bootstraps register state (winner/owner survive the cut)", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(72);
    const REG: TopicPolicy = {
      kind: "register",
      retention: { mode: "full" },
      replication: "full-sync",
      access: "content",
      conflict: { default: "lww" },
      finalityAuthority: AUTH,
    };
    const a = makeNode(sched, "wA", REG);
    void a.register(T).set("theme", "dark");
    void a.register(T).add("tags", "x");
    await sched.run({ untilMs: 1_100_000 });
    await certify(sched, a);
    void a.register(T).set("late.key", "post-cut");
    await sched.run({ untilMs: sched.now() + 500 });

    const b = makeNode(sched, "wB", REG);
    connect(sched, rng, a, b);
    await sched.run({ untilMs: sched.now() + 15_000 });

    const rb = (b as unknown as { _registers: RegisterHub })._registers;
    await rb.settle(T);
    const snap = rb.snapshotState(T);
    expect(snap.keys.theme?.value).toBe("dark"); // from the snapshot base
    expect(snap.keys.tags?.members?.x?.present).toBe(true);
    expect(snap.keys["late.key"]?.value).toBe("post-cut"); // from post-cut replay
  });
});
