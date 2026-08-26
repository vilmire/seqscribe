// P6 provisional availability (§19): queries during a view recompute window
// serve the previous committed rows (§9 "queryable during; atomic swap"), and
// a register key with an unresolved conflict still serves its provisional
// winner per §11's conflict surfacing rules — defined results, never an error
// or a block, on both paths.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { chainOf, coreOf, createSeqscribe, seedOf } from "../src/index.js";
import type {
  Conflict,
  Constants,
  LogEntry,
  SeqscribeNode,
  TopicPolicy,
  ViewDef,
} from "../src/index.js";
import type { RegisterHub } from "../src/register.js";

const T = "t.notes";
const REG = "cfg.settings";
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

function regPolicy(conflict: "fww" | "resolver" | "lww"): TopicPolicy {
  return {
    kind: "register",
    retention: { mode: "full" },
    replication: "full-sync",
    access: "content",
    conflict: { default: conflict },
  };
}

function makeNode(
  sched: Scheduler,
  writerId: string,
  rng: () => number = () => 0.7,
): SeqscribeNode {
  return createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng,
    constants: TEST_CONSTANTS,
  });
}

function regOf(node: SeqscribeNode): RegisterHub {
  return (node as unknown as { _registers: RegisterHub })._registers;
}

function viewsOf(node: SeqscribeNode): { get: (n: string) => { table: string; epoch: string } } {
  return (node as unknown as { _views: { get: (n: string) => { table: string; epoch: string } } })
    ._views;
}

function connectReg(sched: Scheduler, rng: SeededRng, a: SeqscribeNode, b: SeqscribeNode) {
  const link = new VirtualLink(sched, rng);
  a.attach(link.a, { peerId: "peerB", peerClass: "content", grants: { [REG]: "full" } });
  b.attach(link.b, { peerId: "peerA", peerClass: "content", grants: { [REG]: "full" } });
}

describe("P6: views stay queryable through a late-arrival recompute (§9, §19)", () => {
  it("queries issued mid-recompute serve the pre-recompute rows, then the swap lands atomically", async () => {
    const sched = new Scheduler(1_000_000);
    // an advancing rng so each minted view epoch is distinct and observable
    const node = makeNode(sched, "w1", new SeededRng(70).fn());
    node.defineTopic(T, FULL);

    // order-sensitive view: remembers the LAST payload.v in fold order
    type LastState = { v: string };
    let probe: (() => void) | undefined;
    const lastView: ViewDef<LastState, { id: string; v: string }> = {
      version: "1",
      init: { v: "" },
      reduce: (_s, e) => {
        probe?.(); // observation hook — reads only, state untouched
        return { v: String((e.payload as { v?: string }).v ?? "") };
      },
      rows: (s) => (s.v === "" ? [] : [{ id: "last", v: s.v }]),
      rowKey: "id",
      schema: { id: "TEXT", v: "TEXT" },
    };
    const h = node.view("last", T, lastView);
    const table = viewsOf(node).get("last").table;

    void node.log(T).append("note", { v: "own-1" });
    void node.log(T).append("note", { v: "own-final" });
    await sched.run({ untilMs: 1_000_200 });
    const settled = h.query<{ id: string; v: string }>(`SELECT * FROM "${table}"`);
    expect(settled).toEqual([{ id: "last", v: "own-final" }]);
    const epochBefore = viewsOf(node).get("last").epoch;

    // a late arrival with an earlier hlc forces a suffix recompute (§9); every
    // query issued WHILE the recompute refolds must serve the previous
    // committed rows — provisional but defined, never an error (P6)
    const midRecompute: { id: string; v: string }[][] = [];
    probe = () => midRecompute.push(h.query(`SELECT * FROM "${table}"`));
    const late: LogEntry = {
      topic: T,
      writer: "w0",
      seq: 1,
      hlc: { l: 999_000, c: 0 },
      kind: "note",
      payload: { v: "late" },
      chain: "",
    };
    late.chain = chainOf(seedOf(T, "w0"), late);
    void coreOf(node).applyExternal(late, "peer");
    await sched.run({ untilMs: 1_001_000 });
    probe = undefined;

    expect(midRecompute.length).toBeGreaterThan(0); // the recompute really ran
    for (const rows of midRecompute) {
      expect(rows).toEqual([{ id: "last", v: "own-final" }]); // old rows until the swap
    }
    // after the atomic swap: total order beats arrival order, epoch bumped
    expect(h.query(`SELECT * FROM "${table}"`)).toEqual([{ id: "last", v: "own-final" }]);
    expect(viewsOf(node).get("last").epoch).not.toBe(epochBefore);
  });
});

describe("P6: registers serve provisional winners under unresolved conflicts (§11, §19)", () => {
  async function conflictedPair(conflict: "fww" | "resolver", seed: number) {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(seed);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    a.defineTopic(REG, regPolicy(conflict));
    b.defineTopic(REG, regPolicy(conflict));
    const conflicts: Conflict[] = [];
    a.onConflict(REG, (c) => {
      conflicts.push(c);
    });
    b.onConflict(REG, (c) => {
      conflicts.push(c);
    });

    // partitioned concurrent writes → an unresolved conflict after merge
    void a.register(REG).set("pin", "first");
    await sched.run({ untilMs: 1_100 });
    void b.register(REG).set("pin", "second"); // later hlc, concurrent
    await sched.run({ untilMs: 1_300 });
    connectReg(sched, rng, a, b);
    await sched.run({ untilMs: 4_000 });
    await regOf(a).settle(REG);
    await regOf(b).settle(REG);
    return { sched, a, b, conflicts };
  }

  it("fww: the earlier concurrent write is served while both heads stay unresolved", async () => {
    const { a, b, conflicts } = await conflictedPair("fww", 71);

    for (const n of [a, b]) {
      const snap = regOf(n).snapshotState(REG);
      const key = snap.keys.pin;
      expect(key?.value).toBe("first"); // fww keeps the earlier of concurrent writes
      expect(key?.frontier).toHaveLength(2); // conflict is NOT resolved…

      // …and the built-in table still serves the provisional value, flagged
      const rows = regOf(n).tableRowsSorted(REG);
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ key: "pin", value: JSON.stringify("first"), conflicted: 1 });
    }
    // §11 conflict surfacing: the callback carries the same provisional winner
    expect(conflicts.length).toBeGreaterThan(0);
    const c = conflicts[0]!;
    expect(c.concurrent).toBe(true);
    expect((c.provisionalWinner.payload as { value: string }).value).toBe("first");
    expect(c.entries).toHaveLength(2);
  });

  it("resolver: with no resolver registered, the lww provisional winner is served indefinitely", async () => {
    const { sched, a, b, conflicts } = await conflictedPair("resolver", 72);

    for (const n of [a, b]) {
      const snap = regOf(n).snapshotState(REG);
      const key = snap.keys.pin;
      expect(key?.value).toBe("second"); // resolver policy: provisional winner by lww (§11.4)
      expect(key?.frontier).toHaveLength(2);
      const rows = regOf(n).tableRowsSorted(REG);
      expect(rows[0]).toMatchObject({ key: "pin", value: JSON.stringify("second"), conflicted: 1 });
    }
    expect(conflicts.some((c) => c.concurrent)).toBe(true);

    // the conflict never blocks later reads or writes on the same topic
    void a.register(REG).set("other", "fine");
    await sched.run({ untilMs: sched.now() + 2_000 });
    await regOf(a).settle(REG);
    expect(regOf(a).snapshotState(REG).keys.other?.value).toBe("fine");
    expect(regOf(a).snapshotState(REG).keys.pin?.frontier).toHaveLength(2); // still provisional
  });
});
