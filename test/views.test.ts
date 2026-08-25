// §9 views: total-order fold, delta application + checkpoint verification,
// late-arrival recompute with epoch bump, faulted views.

import { describe, expect, it } from "vitest";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { chainOf, coreOf, createSeqscribe, seedOf, SeqscribeError } from "../src/index.js";
import type {
  Anomaly,
  Constants,
  JsonValue,
  LogEntry,
  Row,
  SeqscribeNode,
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

function makeNode(
  sched: Scheduler,
  constants?: Partial<Constants>,
): { node: SeqscribeNode; anomalies: Anomaly[] } {
  const anomalies: Anomaly[] = [];
  const node = createSeqscribe({
    writerId: "w1",
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.5,
    ...(constants ? { constants } : {}),
  });
  node.onAnomaly((a) => anomalies.push(a));
  node.defineTopic(T, FULL);
  return { node, anomalies };
}

// counts entries per kind
type CountState = { [kind: string]: number };
const countView: ViewDef<CountState, { kind: string; n: number }> = {
  version: "1",
  init: {},
  reduce: (s, e) => ({ ...s, [e.kind]: (s[e.kind] ?? 0) + 1 }),
  rows: (s) => Object.entries(s).map(([kind, n]) => ({ kind, n })),
  rowKey: "kind",
  schema: { kind: "TEXT", n: "INTEGER" },
};

// remembers the LAST payload.v in fold order — order-sensitive by construction
type LastState = { v: string };
const lastView: ViewDef<LastState, { id: string; v: string }> = {
  version: "1",
  init: { v: "" },
  reduce: (_s, e) => ({ v: String((e.payload as { v?: string }).v ?? "") }),
  rows: (s) => (s.v === "" ? [] : [{ id: "last", v: s.v }]),
  rowKey: "id",
  schema: { id: "TEXT", v: "TEXT" },
};

describe("view materialization", () => {
  it("folds appends and serves queries", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched);
    const h = node.view("counts", T, countView);
    void node.log(T).append("a", {});
    void node.log(T).append("a", {});
    void node.log(T).append("b", {});
    await sched.run();
    const hub = (node as unknown as { _views: { get: (n: string) => { table: string } } })._views;
    const table = hub.get("counts").table;
    const out = h.query<{ kind: string; n: number }>(`SELECT * FROM "${table}" ORDER BY kind`);
    expect(out).toEqual([
      { kind: "a", n: 2 },
      { kind: "b", n: 1 },
    ]);
  });

  it("applies deltas incrementally and passes checkpoint verification", async () => {
    const sched = new Scheduler(0);
    const { node, anomalies } = makeNode(sched, { CHECKPOINT_EVERY: 3 });
    const withDelta: ViewDef<CountState, { kind: string; n: number }> = {
      ...countView,
      delta: (s, e) => ({
        upserts: [{ kind: e.kind, n: (s[e.kind] ?? 0) + 1 }],
        deletes: [],
      }),
    };
    node.view("counts", T, withDelta);
    for (let i = 0; i < 7; i++) void node.log(T).append(i % 2 === 0 ? "a" : "b", {});
    await sched.run();
    const hub = (node as unknown as { _views: { get: (n: string) => { table: string } } })._views;
    const table = hub.get("counts").table;
    const rows = coreOf(node)
      ? (node.view === undefined
          ? []
          : ((node as unknown as { _core: { store: { raw: () => { all: Function } } } })._core.store
              .raw()
              .all(`SELECT * FROM "${table}" ORDER BY kind`) as { kind: string; n: number }[]))
      : [];
    expect(rows).toEqual([
      { kind: "a", n: 4 },
      { kind: "b", n: 3 },
    ]);
    expect(anomalies.filter((a) => a.kind === "delta_mismatch")).toHaveLength(0);
  });

  it("detects a lying delta at checkpoints and repairs the table", async () => {
    const sched = new Scheduler(0);
    const { node, anomalies } = makeNode(sched, { CHECKPOINT_EVERY: 2 });
    const lying: ViewDef<CountState, { kind: string; n: number }> = {
      ...countView,
      delta: () => ({ upserts: [{ kind: "wrong", n: 999 }], deletes: [] }),
    };
    node.view("counts", T, lying);
    for (let i = 0; i < 4; i++) void node.log(T).append("a", {});
    await sched.run();
    expect(anomalies.some((a) => a.kind === "delta_mismatch")).toBe(true);
    const hub = (node as unknown as { _views: { get: (n: string) => { table: string } } })._views;
    const table = hub.get("counts").table;
    const rows = (node as unknown as { _core: { store: { raw: () => { all: Function } } } })._core.store
      .raw()
      .all(`SELECT * FROM "${table}"`) as { kind: string; n: number }[];
    expect(rows).toEqual([{ kind: "a", n: 4 }]); // rows() is authoritative
  });

  it("recomputes the suffix on a late arrival (total order beats arrival order)", async () => {
    const sched = new Scheduler(1_000_000);
    const { node } = makeNode(sched);
    node.view("last", T, lastView);

    // own entries at "current" time
    void node.log(T).append("note", { v: "own-final" });
    await sched.run({ untilMs: 1_000_100 });

    // a late arrival from another writer with an EARLIER hlc — in total order it
    // lands before our entry, so the view outcome must not change to "late"
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

    const hub = (node as unknown as { _views: { get: (n: string) => { table: string } } })._views;
    const table = hub.get("last").table;
    const rows = (node as unknown as { _core: { store: { raw: () => { all: Function } } } })._core.store
      .raw()
      .all(`SELECT * FROM "${table}"`) as { id: string; v: string }[];
    expect(rows).toEqual([{ id: "last", v: "own-final" }]);

    // and one that lands AFTER ours in total order flips the outcome
    const later: LogEntry = {
      topic: T,
      writer: "wz",
      seq: 1,
      hlc: { l: 1_000_500, c: 0 },
      kind: "note",
      payload: { v: "z-final" },
      chain: "",
    };
    later.chain = chainOf(seedOf(T, "wz"), later);
    void coreOf(node).applyExternal(later, "peer");
    await sched.run({ untilMs: 1_002_000 });
    const rows2 = (node as unknown as { _core: { store: { raw: () => { all: Function } } } })._core.store
      .raw()
      .all(`SELECT * FROM "${table}"`) as { id: string; v: string }[];
    expect(rows2).toEqual([{ id: "last", v: "z-final" }]);
  });

  it("faults the view on a NaN row and blocks queries", async () => {
    const sched = new Scheduler(0);
    const { node, anomalies } = makeNode(sched);
    const bad: ViewDef<JsonValue, Row> = {
      version: "1",
      init: null,
      reduce: () => null,
      rows: () => [{ id: "x", v: Number.NaN }],
      rowKey: "id",
      schema: { id: "TEXT", v: "REAL" },
    };
    const h = node.view("bad", T, bad);
    void node.log(T).append("note", {});
    await sched.run();
    expect(anomalies.some((a) => a.kind === "view_faulted")).toBe(true);
    expect(() => h.query("SELECT 1")).toThrowError(SeqscribeError);
  });

  it("rejects custom views on ring topics", () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched);
    node.defineTopic("t.ring", {
      kind: "append",
      retention: { mode: "ring", size: 5 },
      replication: "subscribe-only",
      access: "content",
    });
    expect(() => node.view("rv", "t.ring", countView)).toThrowError(SeqscribeError);
  });
});
