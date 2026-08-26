// Rollback and shutdown hygiene: a failed commit transaction must leave no
// in-memory side state behind (§8 — scope tails §11.2, ring tails §14,
// recovery targets §12/§13), historical re-folds must not perturb live state
// (§7.7, §11.6), and close() must quiesce every hub timer before the owner
// lock releases (§14).

import { describe, expect, it } from "vitest";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { coreOf, createSeqscribe, SeqscribeError } from "../src/index.js";
import type {
  Anomaly,
  LogEntry,
  Order,
  SeqscribeNode,
  SqliteHandle,
  TopicPolicy,
  ViewDef,
  WriterDirective,
} from "../src/index.js";
import type { RegisterHub } from "../src/register.js";

const T = "t.notes";
const REG = "cfg.settings";
const RING = "t.ring";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
const REG_LWW: TopicPolicy = {
  kind: "register",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
  conflict: { default: "lww" },
};
const REG_OWNED: TopicPolicy = { ...REG_LWW, conflict: { default: "owned" } };
const RING_POLICY: TopicPolicy = {
  kind: "append",
  retention: { mode: "ring", size: 3 },
  replication: "subscribe-only",
  access: "content",
};

// covers every entry — stateAt with this ord folds the full log
const TOP: Order = { l: Number.MAX_SAFE_INTEGER, c: 0, writer: "", seq: 0 };

function regOf(node: SeqscribeNode): RegisterHub {
  return (node as unknown as { _registers: RegisterHub })._registers;
}

function storeOf(node: SeqscribeNode): {
  getEntry(t: string, w: string, s: number): LogEntry | undefined;
} {
  return (coreOf(node) as unknown as { store: ReturnType<typeof storeOf> }).store;
}

function scopeTailsOf(hub: RegisterHub): Map<string, [string, number]> {
  return (hub as unknown as { scopeTails: Map<string, [string, number]> }).scopeTails;
}

// wraps the harness handle to count store traffic and inject write failures —
// the injected error aborts LogCore's commit transaction like a real disk fault
function instrumentedHandle(): {
  handle: SqliteHandle;
  state: { ops: number; failOn: string | null };
} {
  const inner = memoryHandle();
  const state = { ops: 0, failOn: null as string | null };
  const handle: SqliteHandle = {
    ...inner,
    run(sql, params) {
      state.ops++;
      if (state.failOn !== null && sql.includes(state.failOn))
        throw new Error("injected storage failure");
      return inner.run(sql, params);
    },
    get: <T2>(sql: string, params?: unknown[]) => {
      state.ops++;
      return inner.get<T2>(sql, params);
    },
    all: <T2>(sql: string, params?: unknown[]) => {
      state.ops++;
      return inner.all<T2>(sql, params);
    },
  };
  return { handle, state };
}

function makeNode(
  sched: Scheduler,
  writerId: string,
  handle?: SqliteHandle,
): { node: SeqscribeNode; anomalies: Anomaly[] } {
  const anomalies: Anomaly[] = [];
  const node = createSeqscribe({
    writerId,
    storage: handle ?? memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.5,
    authority: {
      verifyTakeover: () => true,
      verifyWriterDirective: () => true,
    },
  });
  node.onAnomaly((a) => anomalies.push(a));
  return { node, anomalies };
}

describe("commit rollback restores side state (§8)", () => {
  it("rejects the batch with ERR_STORAGE and leaves causal tails, rings and recoveries untouched", async () => {
    const sched = new Scheduler(1_000);
    const { handle, state } = instrumentedHandle();
    const { node } = makeNode(sched, "wA", handle);
    node.defineTopic(REG, REG_LWW);
    node.defineTopic(RING, RING_POLICY);
    const core = coreOf(node);

    // one batch: the ring append lands its in-memory push first, then the
    // register append (which advanced scopeTails via causalProvider) hits the
    // injected sq_log failure and aborts the whole transaction
    state.failOn = "sq_log";
    // attach the rejection handlers before running so the (expected)
    // rejections are never momentarily unhandled
    const pRing = expect(node.log(RING).append("tick", { i: 0 })).rejects.toMatchObject({
      code: "ERR_STORAGE",
    });
    const pReg = expect(node.register(REG).set("k", "v1")).rejects.toMatchObject({
      code: "ERR_STORAGE",
    });
    await sched.run({ untilMs: 1_100 });
    await pRing;
    await pReg;

    // (iii) ring state: the aborted push did not survive
    expect(core.ringTail(RING)).toEqual([]);

    // (ii) the next register write reuses the seq the aborted batch consumed —
    // its causal stamp must be absent (no committed predecessor), never a
    // self-referential [wA, ownSeq] edge
    state.failOn = null;
    const pReg2 = node.register(REG).set("k", "v2");
    await sched.run({ untilMs: 1_200 });
    const id = await pReg2;
    expect(id).toEqual([REG, "wA", 1]);
    const entry = storeOf(node).getEntry(REG, "wA", 1)!;
    expect(entry.causal).toBeUndefined();

    // and the ring topic recovers cleanly at seq 1
    const pRing2 = node.log(RING).append("tick", { i: 1 });
    await sched.run({ untilMs: 1_300 });
    await pRing2;
    expect(core.ringTail(RING).map((e) => [e.seq, e.payload])).toEqual([[1, { i: 1 }]]);
  });

  it("a directive aborted mid-transaction does not leave a live recovery target (§12)", async () => {
    const sched = new Scheduler(1_000);
    const { handle, state } = instrumentedHandle();
    const { node } = makeNode(sched, "wA", handle);
    node.defineTopic(T, FULL);
    const core = coreOf(node);

    // retire directive for a stream we have no prefix of → recovery branch;
    // fail on the txn-final HLC metaSet so the directive's effects roll back
    const d: WriterDirective = {
      topic: T,
      writer: "wB",
      state: "retired",
      rgen: 1,
      finalSeq: 5,
      finalChain: "ff".repeat(32),
      authority: "auth",
      sig: "sig",
    };
    state.failOn = "sq_meta";
    const pd = expect(core.applyDirective(d)).rejects.toMatchObject({ code: "ERR_STORAGE" });
    await sched.run({ untilMs: 1_100 });
    await pd;
    expect(core.recoveryTarget(T, "wB")).toBeUndefined();
    expect(core.getStream(T, "wB").sealReason).toBeNull();

    // retried without the fault, the same directive enters recovery normally
    state.failOn = null;
    const pd2 = core.applyDirective(d);
    await sched.run({ untilMs: 1_200 });
    expect(await pd2).toBe("recovery");
    expect(core.recoveryTarget(T, "wB")).toMatchObject({ finalSeq: 5, rgen: 1 });
  });
});

describe("stateAt is side-effect free (§7.7, §11.6)", () => {
  it("re-folding history emits no anomalies and preserves live causal tails", async () => {
    const sched = new Scheduler(1_000);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    a.node.defineTopic(REG, REG_OWNED);
    b.node.defineTopic(REG, REG_OWNED);

    // wA owns the key; wB's later write is a historical owned violation
    void a.node.register(REG).set("machine", "alpha");
    await sched.run({ untilMs: 1_200 });
    void b.node.register(REG).set("machine", "intruder");
    await sched.run({ untilMs: 1_400 });
    const intruder = storeOf(b.node).getEntry(REG, "wB", 1)!;
    const r = coreOf(a.node).applyExternal(intruder);
    await sched.run({ untilMs: 1_600 });
    expect(await r).toBe("applied");
    const hub = regOf(a.node);
    await hub.settle(REG);
    const violations = () => a.anomalies.filter((x) => x.kind === "owned_violation").length;
    expect(violations()).toBe(1);

    // computing the snapshot twice is pure: identical output, zero new anomalies
    const s1 = await hub.stateAt(REG, TOP);
    const s2 = await hub.stateAt(REG, TOP);
    expect(s2).toEqual(s1);
    expect(s1.keys["machine"]?.value).toBe("alpha");
    expect(violations()).toBe(1);

    // a committed-but-unmaterialized own write keeps its scope tail across a
    // snapshot fold — replay must not shrink the live tail (§11.2)
    const tails = scopeTailsOf(hub);
    expect(tails.size).toBe(0);
    const p1 = a.node.register(REG).set("machine", "beta");
    await coreOf(a.node).flushNow();
    const id1 = await p1;
    expect(tails.size).toBe(1);
    await hub.stateAt(REG, TOP);
    expect(tails.size).toBe(1);

    // and the next write still stamps causal from that tail, not the stale winner
    const p2 = a.node.register(REG).set("machine", "gamma");
    await coreOf(a.node).flushNow();
    const id2 = await p2;
    const e2 = storeOf(a.node).getEntry(REG, "wA", id2[2])!;
    expect(e2.causal).toEqual(["wA", id1[2]]);
    await sched.run(); // drain the deferred materialize passes cleanly
    expect(violations()).toBe(1);
  });
});

describe("close() quiesces (§14)", () => {
  const countView: ViewDef<{ n: number }, { id: string; n: number }> = {
    version: "1",
    init: { n: 0 },
    reduce: (s) => ({ n: s.n + 1 }),
    rows: (s) => (s.n === 0 ? [] : [{ id: "count", n: s.n }]),
    rowKey: "id",
    schema: { id: "TEXT", n: "INTEGER" },
  };

  it("cancels hub timers — nothing touches the store after close resolves", async () => {
    const sched = new Scheduler(1_000);
    const { handle, state } = instrumentedHandle();
    const { node } = makeNode(sched, "wA", handle);
    node.defineTopic(T, FULL);
    node.defineTopic(REG, REG_LWW);
    node.view("counts", T, countView);
    let attempts = 0;
    node.onEntry(T, "c1", () => {
      attempts++;
      throw new Error("consumer down");
    });

    void node.log(T).append("note", { i: 0 });
    void node.register(REG).set("k", "v");
    await sched.run({ untilMs: 1_150 });
    expect(attempts).toBeGreaterThan(0); // consumer failing → backoff timer pending

    // this append is flushed inside close(); the flush fan-out schedules
    // view/register materialize passes that close() must then cancel
    void node.log(T).append("note", { i: 1 });
    await node.close();

    const opsAtClose = state.ops;
    const attemptsAtClose = attempts;
    await sched.run(); // fire every remaining scheduled timer
    expect(state.ops).toBe(opsAtClose);
    expect(attempts).toBe(attemptsAtClose);

    // closed surface: appends refuse, second close is a no-op
    expect(() => node.log(T).append("note", {})).toThrowError(SeqscribeError);
    await node.close();
    expect(state.ops).toBe(opsAtClose);
  });
});
