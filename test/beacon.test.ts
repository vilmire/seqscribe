// §5.7 beacon: debounced content-free vector reports + staleness prediction.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe, sha256HexUtf8 } from "../src/index.js";
import type {
  BeaconReport,
  BeaconTransport,
  SeqscribeNode,
  SeqscribeNodeExt,
  TopicPolicy,
} from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

function makeNode(sched: Scheduler, writerId: string): SeqscribeNode {
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
  });
  node.defineTopic(T, FULL);
  return node;
}

function memoryBeacon(): { transport: BeaconTransport; board: Map<string, BeaconReport> } {
  const board = new Map<string, BeaconReport>();
  return {
    board,
    transport: {
      put: async (r) => {
        board.set(r.node, r); // one report per node, overwrite (§14)
      },
      get: async () => [...board.values()],
    },
  };
}

describe("beacon (§5.7)", () => {
  it("debounces reports and computes staleness from known vectors", async () => {
    const sched = new Scheduler(0);
    const { transport, board } = memoryBeacon();
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    a.beacon(transport);
    b.beacon(transport);
    await sched.run({ untilMs: 100 });
    expect(board.size).toBe(2);

    // A appends while B is "offline" from A's perspective (no link at all)
    for (let i = 0; i < 7; i++) void a.log(T).append("note", { i });
    await sched.run({ untilMs: 10_000 }); // past BEACON_DEBOUNCE_MS
    expect(board.get("wA")!.vectors[T]!.writers.wA).toMatchObject({ contig: 7 });

    // B learns the fleet vectors from the beacon and predicts its lag
    b.setKnownVectors([...board.values()]);
    const s = b.staleness(T);
    expect(s.behind.wA).toBe(7);

    // sole-copy awareness inverse: A is behind nobody
    a.setKnownVectors([...board.values()]);
    expect(a.staleness(T).behind.wA ?? 0).toBe(0);
  });

  it("stops cleanly and refuses double start", async () => {
    const sched = new Scheduler(0);
    const { transport } = memoryBeacon();
    const a = makeNode(sched, "wA");
    const h = a.beacon(transport);
    expect(() => a.beacon(transport)).toThrowError(/beacon already started/);
    h.stop();
    await sched.run();
  });
});

// proposals-v3.5 P27 — the reader (staleness(topic, key) → keyStale) has always
// existed; nothing ever produced BeaconReport.hints, so it could never fire.
describe("beacon hints (§5.7 pre-write warning, P27)", () => {
  const REG = "t.reg";
  const regPolicy = (hintKeys?: "plain" | "hash"): TopicPolicy => ({
    kind: "register",
    retention: { mode: "full" },
    replication: "full-sync",
    access: "content",
    ...(hintKeys ? { hintKeys } : {}),
  });

  // SeqscribeNodeExt: the hints callback rides the extension overload of
  // beacon(), since ratified §14 beacon(t) stays single-argument (P27)
  function regNode(
    sched: Scheduler,
    writerId: string,
    hintKeys?: "plain" | "hash",
  ): SeqscribeNodeExt {
    const node = createSeqscribe({
      writerId,
      storage: memoryHandle(),
      clock: sched.clock(),
      timers: sched.timers(),
    });
    node.defineTopic(T, FULL);
    node.defineTopic(REG, regPolicy(hintKeys));
    return node;
  }

  it("advertises the register winner and drives a peer's keyStale", async () => {
    const sched = new Scheduler(0);
    const { transport, board } = memoryBeacon();
    const a = regNode(sched, "wA", "plain");
    a.beacon(transport);
    const id = await (async () => {
      const p = a.register(REG).set("doc-1", { v: 1 });
      await sched.run({ untilMs: 200 });
      return p;
    })();
    await sched.run({ untilMs: 10_000 }); // past BEACON_DEBOUNCE_MS

    // the hint is the commit's [writer, seq] — sourced from the MATERIALIZED
    // winner, so it is still there long after the write folded (scopeTails,
    // the §11.2 own-uncommitted tail, is empty by now)
    const hint = board.get("wA")!.hints![REG]!["doc-1"];
    expect(hint).toEqual([id[1], id[2]]);

    // a second node fed that report predicts the overwrite it would cause
    const b = regNode(sched, "wB", "plain");
    b.setKnownVectors([...board.values()]);
    const s = b.staleness(REG, "doc-1");
    expect(s.keyStale).toEqual({ latestKnown: [REG, "wA", id[2]], haveLocally: false });

    // and a key nobody has touched still reports no keyStale
    expect(b.staleness(REG, "untouched").keyStale).toBeUndefined();
    await a.close();
    await b.close();
  });

  it("emits no hints field at all without opt-in (byte-identical to pre-P27)", async () => {
    const sched = new Scheduler(0);
    const { transport, board } = memoryBeacon();
    const a = regNode(sched, "wA"); // no hintKeys
    a.beacon(transport);
    void a.register(REG).set("doc-1", { v: 1 });
    await sched.run({ untilMs: 10_000 });
    const report = board.get("wA")!;
    expect(report.hints).toBeUndefined();
    expect(Object.keys(report).sort()).toEqual(["at", "node", "vectors"]);
    await a.close();
  });

  it("hashes keys under hintKeys: \"hash\" so names never leak", async () => {
    const sched = new Scheduler(0);
    const { transport, board } = memoryBeacon();
    const a = regNode(sched, "wA", "hash");
    a.beacon(transport);
    void a.register(REG).set("secret-key", { v: 1 });
    await sched.run({ untilMs: 10_000 });
    const hints = board.get("wA")!.hints![REG]!;
    expect(hints["secret-key"]).toBeUndefined(); // no plaintext by omission
    expect(Object.keys(hints)).toEqual([sha256HexUtf8("secret-key")]);
    await a.close();
  });

  it("carries a host-supplied callback's hints verbatim, and gates on hintKeys", async () => {
    const sched = new Scheduler(0);
    const { transport, board } = memoryBeacon();
    const a = regNode(sched, "wA", "plain");
    // T has no hintKeys → dropped; REG does → carried
    a.beacon(transport, {
      hints: () => ({ [REG]: { "host-key": ["wZ", 42] }, [T]: { nope: ["wZ", 9] } }),
    });
    await sched.run({ untilMs: 200 });
    const hints = board.get("wA")!.hints!;
    expect(hints[REG]!["host-key"]).toEqual(["wZ", 42]);
    expect(hints[T]).toBeUndefined();
    await a.close();
  });

  it("ranks a key's latest change by total order, not by raw per-writer seq", async () => {
    // seq is per (topic, writer): a HIGH-seq entry from one writer is not newer
    // than a LOW-seq entry from another. This bites on the member path, where a
    // key's candidates come from several writers — ranking by seq picks the
    // stale one and then makes staleness() test the wrong writer's stream.
    const sched = new Scheduler(0);
    const rng = new SeededRng(1);
    const { transport, board } = memoryBeacon();
    const a = regNode(sched, "wA", "plain");
    const b = regNode(sched, "wB", "plain");
    const link = new VirtualLink(sched, rng);
    a.attach(link.a, { peerId: "peerB", peerClass: "content", grants: { [REG]: "full" } });
    b.attach(link.b, { peerId: "peerA", peerClass: "content", grants: { [REG]: "full" } });

    // drive wA's own seq counter far ahead on unrelated keys
    for (let i = 0; i < 12; i++) void a.register(REG).set(`filler-${i}`, { i });
    await sched.run({ untilMs: 3_000 });

    // wA adds a member EARLY (high seq); wB adds one LATER (low seq). The total
    // order says wB is the latest change; the raw seqs say the opposite.
    void a.register(REG).add("team", "alice");
    await sched.run({ untilMs: 6_000 });
    void b.register(REG).add("team", "bob");
    await sched.run({ untilMs: 12_000 });

    a.beacon(transport);
    await sched.run({ untilMs: 30_000 });

    const hint = board.get("wA")!.hints![REG]!["team"]!;
    const aSeq = a.scanEntries(REG, { writer: "wA", limit: 500 }).entries.length;
    const bSeq = b.scanEntries(REG, { writer: "wB", limit: 500 }).entries.length;
    expect(aSeq).toBeGreaterThan(bSeq); // the premise: wA's seqs dwarf wB's
    expect(hint[0]).toBe("wB"); // total order wins over the bigger seq number
    void link;
    await a.close();
    await b.close();
  });

  it("survives a throwing host callback without losing the report", async () => {
    const sched = new Scheduler(0);
    const { transport, board } = memoryBeacon();
    const a = regNode(sched, "wA", "plain");
    a.beacon(transport, {
      hints: () => {
        throw new Error("host bug");
      },
    });
    await sched.run({ untilMs: 200 });
    expect(board.get("wA")!.vectors).toBeDefined(); // the report still went out
    await a.close();
  });
});
