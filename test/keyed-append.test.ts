// Keyed append topics (host-guide §4.7): TopicPolicy.keyed, the mandatory
// append key, pruneSuperseded, the newest-per-key "tail" SNAP default, the
// TailSource keyed reads, scanLatestPerKey / keyHead / tailSubscriberCount.
//
// Pins: keyed is local storage policy (topicSchemaHash unchanged, SUB wire
// shape unchanged — an old-policy subscriber interoperates); pruneSuperseded
// judges supersession AT the watermark, honors onEntry cursors, never
// resurrects a key, is allowed under a live subscriber; the default keyed SNAP
// is newest-per-key over the whole topic (not a 500-row window).
//
// Harness conventions follow prune-topic.test.ts: virtual-time scheduler,
// fire → sched.run() → await for every queue-backed promise, runRejecting for
// a promise expected to reject.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { fileHandle, memoryHandle } from "../harness/sqlite.js";
import { topicSchemaHashOf } from "../src/encoding.js";
import { Store } from "../src/store.js";
import { createSeqscribe, SeqscribeError } from "../src/index.js";
import type { LogEntry, Row, SeqscribeNodeExt, SqliteHandle, TailSource, TopicPolicy } from "../src/index.js";

const dir = mkdtempSync(join(tmpdir(), "seqscribe-keyed-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

const KEYED: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "subscribe-only",
  access: "content",
  keyed: { tombstoneKind: "del" },
};
const PLAIN: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "subscribe-only",
  access: "content",
};

function makeNode(sched: Scheduler, writerId = "w1", storage: SqliteHandle = memoryHandle()): SeqscribeNodeExt {
  return createSeqscribe({ writerId, storage, clock: sched.clock(), timers: sched.timers(), rng: () => 0.37 });
}

async function run<T>(sched: Scheduler, p: Promise<T>, untilMs?: number): Promise<T> {
  await sched.run(untilMs !== undefined ? { untilMs } : undefined);
  return p;
}

async function runRejecting<T>(sched: Scheduler, p: Promise<T>, untilMs?: number): Promise<T> {
  p.catch(() => {});
  await sched.run(untilMs !== undefined ? { untilMs } : undefined);
  return p;
}

// append helpers: payload carries the key and a version so assertions read
// straight off rows
const put = (n: SeqscribeNodeExt, t: string, key: string, v: number) =>
  void n.log(t).append("msg", { k: key, v }, { key });
const del = (n: SeqscribeNodeExt, t: string, key: string, v: number) =>
  void n.log(t).append("del", { k: key, v }, { key });

// every surviving row of the topic as "kind:key@v", rowid order
function allRows(n: SeqscribeNodeExt, topic: string): string[] {
  // every row, not newest-per-key: the writer-form scan per writer
  const out: string[] = [];
  for (const w of ["w1", "w2"]) {
    const s = n.scanEntries(topic, { writer: w, fromSeq: 1, limit: 10_000 });
    for (const e of s.entries) out.push(`${e.writer}/${e.seq}:${fmt(e)}`);
  }
  return out;
}

const fmt = (e: LogEntry) => {
  const p = e.payload as { k: string; v: number };
  return `${e.kind}:${p.k}@${p.v}`;
};

function latest(n: SeqscribeNodeExt, topic: string, uptoRowid?: number): string[] {
  const out: string[] = [];
  let afterRowid = 0;
  for (;;) {
    const page = n.scanLatestPerKey(topic, { afterRowid, limit: 3, ...(uptoRowid !== undefined ? { uptoRowid } : {}) });
    for (const r of page.entries) out.push(fmt(r.entry));
    if (page.complete) return out;
    afterRowid = page.nextAfterRowid!;
  }
}

describe("keyed append — policy and append discipline", () => {
  it("keyed is legal only on append + full + subscribe-only, with a non-empty tombstoneKind", () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    node.defineTopic("k.ok", KEYED);
    const bad: TopicPolicy[] = [
      { ...KEYED, replication: "full-sync" },
      { ...KEYED, retention: { mode: "ring", size: 8 } },
      { kind: "register", retention: { mode: "full" }, replication: "full-sync", access: "content", keyed: { tombstoneKind: "del" } },
      { ...KEYED, keyed: { tombstoneKind: "" } },
      { ...KEYED, keyed: null as unknown as { tombstoneKind: string } },
    ];
    bad.forEach((p, i) => expect(() => node.defineTopic(`k.bad${i}`, p), `policy ${i}`).toThrow(SeqscribeError));
  });

  it("keyed is NOT part of topicSchemaHash — a keyed and a plain definition hash identically", () => {
    expect(topicSchemaHashOf(KEYED)).toBe(topicSchemaHashOf(PLAIN));
  });

  it("a keyed append without a key rejects; a key on a non-keyed topic rejects; neither throws", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    node.defineTopic("k.t", KEYED);
    node.defineTopic("p.t", PLAIN);
    let noKey!: Promise<unknown>;
    let strayKey!: Promise<unknown>;
    expect(() => {
      noKey = node.log("k.t").append("msg", { v: 1 });
      strayKey = node.log("p.t").append("msg", { v: 1 }, { key: "m:1" });
    }).not.toThrow();
    noKey.catch(() => {});
    strayKey.catch(() => {});
    await expect(runRejecting(sched, noKey)).rejects.toMatchObject({ code: "ERR_MISUSE" });
    await expect(runRejecting(sched, strayKey)).rejects.toMatchObject({ code: "ERR_MISUSE" });
    // an oversized key is the usual entry-encoding rejection
    await expect(
      runRejecting(sched, node.log("k.t").append("msg", {}, { key: "x".repeat(513) })),
    ).rejects.toMatchObject({ code: "ERR_ENTRY_ENCODING" });
    expect(node.stats().topics["k.t"]?.logRows).toBe(0);
  });

  it("the key is stored, hashed into the chain, and returned by scans", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    node.defineTopic("k.t", KEYED);
    const id = await run(sched, node.log("k.t").append("msg", { v: 1 }, { key: "m:a" }));
    expect(id).toEqual(["k.t", "w1", 1]);
    const [e] = node.scanEntries("k.t", { writer: "w1" }).entries;
    expect(e!.key).toBe("m:a");
    // the same entry with a different key would chain differently
    const { chainOf, seedOf } = await import("../src/encoding.js");
    expect(chainOf(seedOf("k.t", "w1"), e!)).toBe(e!.chain);
    expect(chainOf(seedOf("k.t", "w1"), { ...e!, key: "m:b" })).not.toBe(e!.chain);
  });
});

describe("pruneSuperseded", () => {
  it("deletes rows superseded at or below the watermark, keeps each key's newest", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const t = "k.p1";
    node.defineTopic(t, KEYED);
    for (let v = 1; v <= 5; v++) for (const k of ["a", "b", "c"]) put(node, t, k, v);
    await sched.run();
    expect(node.stats().topics[t]?.logRows).toBe(15);
    const W = node.keyHead(t, "c")!.rowid;

    const r = await run(sched, node.pruneSuperseded(t, { uptoRowid: W }));
    expect(r.prunedRows).toBe(12);
    expect(allRows(node, t)).toEqual(["w1/13:msg:a@5", "w1/14:msg:b@5", "w1/15:msg:c@5"]);
    // idempotent
    expect((await run(sched, node.pruneSuperseded(t, { uptoRowid: W }))).prunedRows).toBe(0);
    // the stream continues: heads untouched
    const vecBefore = JSON.stringify(node.vectors()[t]);
    put(node, t, "a", 6);
    await sched.run();
    expect(node.scanEntries(t, { writer: "w1", fromSeq: 16 }).entries.map((e) => e.seq)).toEqual([16]);
    expect(vecBefore).toContain('"contig":15');
  });

  it("a row above the watermark never supersedes: the last committed version of a key an in-flight frame rewrites survives", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const t = "k.p2";
    node.defineTopic(t, KEYED);
    put(node, t, "a", 1);
    put(node, t, "a", 2); // committed version of a
    put(node, t, "commit", 1);
    await sched.run();
    const W = node.keyHead(t, "commit")!.rowid;
    put(node, t, "a", 3); // in-flight frame (above W)
    del(node, t, "b", 1); // in-flight tombstone of a key that never existed below W
    await sched.run();

    const r = await run(sched, node.pruneSuperseded(t, { uptoRowid: W }));
    expect(r.prunedRows).toBe(1); // only a@1
    expect(allRows(node, t)).toEqual(["w1/2:msg:a@2", "w1/3:msg:commit@1", "w1/4:msg:a@3", "w1/5:del:b@1"]);
    // newest-per-key judged at W vs. overall
    expect(latest(node, t, W)).toEqual(["msg:a@2", "msg:commit@1"]);
    expect(latest(node, t)).toEqual(["msg:commit@1", "msg:a@3", "del:b@1"]);
  });

  it("removes a tombstone left as the only row of its key; keeps one an older row still depends on", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const t = "k.p3";
    node.defineTopic(t, KEYED);
    put(node, t, "a", 1);
    put(node, t, "a", 2);
    del(node, t, "a", 3); // a deleted
    put(node, t, "b", 1);
    del(node, t, "b", 2); // b deleted …
    put(node, t, "c", 1);
    put(node, t, "commit", 1);
    await sched.run();
    const W = node.keyHead(t, "commit")!.rowid;
    put(node, t, "b", 3); // … and re-created above W (does not block b's tombstone)
    await sched.run();

    // maxRows 2: only part of (a) runs — a's tombstone must survive while a@1/a@2 are not both gone
    const first = await run(sched, node.pruneSuperseded(t, { uptoRowid: W, maxRows: 2 }));
    expect(first.prunedRows).toBe(2);
    let rows = allRows(node, t);
    const aLeft = rows.filter((r) => r.includes(":a@") && r.includes("msg"));
    if (aLeft.length > 0) expect(rows.some((r) => r.includes("del:a@3"))).toBe(true);

    let total = first.prunedRows;
    for (let i = 0; i < 5; i++) total += (await run(sched, node.pruneSuperseded(t, { uptoRowid: W, maxRows: 2 }))).prunedRows;
    rows = allRows(node, t);
    // a@1, a@2, del a, b@1, del b → gone; c@1, commit, b@3 (above W) stay
    expect(total).toBe(5);
    expect(rows).toEqual(["w1/6:msg:c@1", "w1/7:msg:commit@1", "w1/8:msg:b@3"]);
    expect(latest(node, t)).toEqual(["msg:c@1", "msg:commit@1", "msg:b@3"]);
  });

  it("never deletes a row an onEntry consumer has not read, and never resurrects through a held row", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const t = "k.p4";
    node.defineTopic(t, KEYED);
    put(node, t, "a", 1); // rowid 1
    put(node, t, "a", 2); // rowid 2
    await sched.run();
    // a consumer that has read exactly rowid 1, then went away (cursor persists)
    const unsub = node.onEntry(t, "reader", (e) => {
      if (e.seq >= 2) throw new Error("stop at 2");
    });
    await sched.run({ untilMs: sched.now() + 50 });
    unsub();
    expect(node.listConsumers(t)[0]?.lastRowid).toBe(1);
    del(node, t, "a", 3); // rowid 3: tombstone
    put(node, t, "x", 1); // rowid 4
    await sched.run();

    const r = await run(sched, node.pruneSuperseded(t, { uptoRowid: 4 }));
    // floor = cursor 1: only a@1 (read, superseded) goes. a@2 is unread; the
    // tombstone is above the floor anyway and a@2 would resurrect without it.
    expect(r.prunedRows).toBe(1);
    expect(allRows(node, t)).toEqual(["w1/2:msg:a@2", "w1/3:del:a@3", "w1/4:msg:x@1"]);

    node.deleteConsumer(t, "reader");
    const r2 = await run(sched, node.pruneSuperseded(t, { uptoRowid: 4 }));
    expect(r2.prunedRows).toBe(2); // a@2 superseded, then the lone tombstone
    expect(allRows(node, t)).toEqual(["w1/4:msg:x@1"]);
  });

  // (a) runs to completion before (b) in one transaction, so through the
  // node API an older row under a tombstone is always already gone; the
  // store-level probe still refuses a tombstone with ANY other row of its key
  // at or below the watermark — the no-resurrection guarantee does not rest
  // on step ordering alone.
  it("store: a tombstone with an older row of its key still present is never a lone tombstone", () => {
    const store = new Store(memoryHandle());
    store.init("normal");
    const ins = (seq: number, kind: string, key: string) =>
      store.insertEntry({ topic: "k.st", writer: "w1", seq, hlc: { l: seq, c: 0 }, kind, key, payload: {}, chain: `c${seq}` });
    store.transaction(() => {
      ins(1, "msg", "a"); // older row of a
      ins(2, "del", "a"); // tombstone of a
      ins(3, "del", "b"); // lone tombstone of b
      ins(4, "msg", "b"); // b re-created ABOVE the watermark below
    });
    expect(store.loneTombstoneRowids("k.st", "del", 3, 3, 10)).toEqual([3]);
    expect(store.supersededRowids("k.st", 3, 3, 10)).toEqual([1]);
    // at a watermark covering b's re-creation, b's tombstone is superseded, not lone
    expect(store.loneTombstoneRowids("k.st", "del", 3, 4, 10)).toEqual([]);
    expect(store.supersededRowids("k.st", 3, 4, 10).sort()).toEqual([1, 3]);
  });

  it("supersedeOtherWriters drops a previous writer's rows at or below the watermark", async () => {
    const sched = new Scheduler(0);
    const path = join(dir, "writer-change.db");
    const t = "k.p5";
    const a = makeNode(sched, "w1", fileHandle(path));
    a.defineTopic(t, KEYED);
    for (const k of ["a", "b", "c"]) put(a, t, k, 1);
    await sched.run();
    await a.close();

    const b = makeNode(sched, "w2", fileHandle(path));
    b.defineTopic(t, KEYED);
    for (const k of ["a", "c"]) put(b, t, k, 2); // base frame by the new writer
    put(b, t, "commit", 1);
    await sched.run();
    const W = b.keyHead(t, "commit")!.rowid;
    put(b, t, "d", 1); // above W
    await sched.run();

    // without the flag, w1's rows are separate keys' newest → only a@1, c@1 go
    const plain = await run(sched, b.pruneSuperseded(t, { uptoRowid: W }));
    expect(plain.prunedRows).toBe(2);
    expect(allRows(b, t)).toEqual(["w1/2:msg:b@1", "w2/1:msg:a@2", "w2/2:msg:c@2", "w2/3:msg:commit@1", "w2/4:msg:d@1"]);
    const r = await run(sched, b.pruneSuperseded(t, { uptoRowid: W, supersedeOtherWriters: true }));
    expect(r.prunedRows).toBe(1);
    expect(allRows(b, t)).toEqual(["w2/1:msg:a@2", "w2/2:msg:c@2", "w2/3:msg:commit@1", "w2/4:msg:d@1"]);
    await b.close();
  });

  it("refuses non-keyed topics and bad arguments (rejecting, not throwing)", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    node.defineTopic("p.t", PLAIN);
    node.defineTopic("k.t", KEYED);
    for (const call of [
      () => node.pruneSuperseded("p.t", { uptoRowid: 10 }),
      () => node.pruneSuperseded("k.t", { uptoRowid: -1 }),
      () => node.pruneSuperseded("k.t", { uptoRowid: 1.5 }),
      () => node.pruneSuperseded("k.t", { uptoRowid: 1, maxRows: 0 }),
    ])
      await expect(runRejecting(sched, call())).rejects.toMatchObject({ code: "ERR_MISUSE" });
    await expect(runRejecting(sched, node.pruneSuperseded("no.such", { uptoRowid: 1 }))).rejects.toMatchObject({
      code: "ERR_UNKNOWN_TOPIC",
    });
    expect((await run(sched, node.pruneSuperseded("k.t", { uptoRowid: 0 }))).prunedRows).toBe(0);
    await node.close();
    await expect(runRejecting(sched, node.pruneSuperseded("k.t", { uptoRowid: 1 }))).rejects.toMatchObject({
      code: "ERR_MISUSE",
    });
  });
});

describe("keyed reads — scanLatestPerKey / keyHead", () => {
  it("pages the newest row per key in rowid order; uptoRowid judges supersession at the watermark", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const t = "k.r1";
    node.defineTopic(t, KEYED);
    for (let v = 1; v <= 3; v++) for (const k of ["a", "b", "c", "d"]) put(node, t, k, v);
    put(node, t, "b", 4);
    await sched.run();
    expect(latest(node, t)).toEqual(["msg:a@3", "msg:c@3", "msg:d@3", "msg:b@4"]);
    const page = node.scanLatestPerKey(t, { limit: 2 });
    expect(page.complete).toBe(false);
    expect(page.entries.map((r) => r.rowid)).toEqual([9, 11]);
    expect(page.nextAfterRowid).toBe(11);
    expect(latest(node, t, 8)).toEqual(["msg:a@2", "msg:b@2", "msg:c@2", "msg:d@2"]);
    expect(node.keyHead(t, "b")).toMatchObject({ rowid: 13, entry: { key: "b", seq: 13 } });
    expect(node.keyHead(t, "zz")).toBeNull();
  });

  it("keyed reads are keyed-topic only", () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    node.defineTopic("p.t", PLAIN);
    expect(() => node.scanLatestPerKey("p.t")).toThrow(SeqscribeError);
    expect(() => node.keyHead("p.t", "a")).toThrow(SeqscribeError);
    expect(() => node.scanLatestPerKey("no.such")).toThrow(SeqscribeError);
  });
});

// ---- SUB: keyed "tail" SNAP default, selector reads, subscriber interplay ----

async function subHarness(topic: string, clientPolicy: TopicPolicy = KEYED) {
  const sched = new Scheduler(0);
  const rng = new SeededRng(91);
  const server = makeNode(sched, "server");
  const client = makeNode(sched, "client");
  server.defineTopic(topic, KEYED);
  client.defineTopic(topic, clientPolicy);
  const link = new VirtualLink(sched, rng);
  server.attach(link.a, { peerId: "client", peerClass: "content", grants: { [topic]: "serve" } });
  const handle = client.attach(link.b, { peerId: "server", peerClass: "content", grants: { [topic]: "none" } });
  await sched.run({ untilMs: 200 });
  const subscribe = () => {
    const got: { snaps: { rows: Row[]; reset: boolean }[]; deltas: Row[][] } = { snaps: [], deltas: [] };
    const sub = client.subscribe(handle, { view: "tail", params: { topic } });
    sub.onSnapshot((rows, reset) => got.snaps.push({ rows, reset }));
    sub.onDelta((d) => got.deltas.push(d.upserts));
    return { got, sub };
  };
  return { sched, server, client, subscribe };
}

const rowFmt = (r: Row) => {
  const p = JSON.parse(String(r.payload)) as { k: string; v: number };
  return `${String(r.kind)}:${p.k}@${p.v}`;
};

describe("keyed tail SUB", () => {
  it("default SNAP is newest-per-key over the whole topic — beyond the 500-row tail window", async () => {
    const t = "k.s1";
    const h = await subHarness(t);
    for (let i = 0; i < 600; i++) put(h.server, t, `m${i}`, 1);
    for (let i = 0; i < 600; i++) put(h.server, t, `m${i}`, 2);
    del(h.server, t, "m7", 3);
    await h.sched.run({ untilMs: 400 });
    const { got } = h.subscribe();
    await h.sched.run({ untilMs: 3_000 });
    expect(got.snaps).toHaveLength(1);
    const rows = got.snaps[0]!.rows;
    expect(got.snaps[0]!.reset).toBe(true);
    expect(rows).toHaveLength(600);
    expect(rows.filter((r) => rowFmt(r).endsWith("@2"))).toHaveLength(599);
    expect(rows.map(rowFmt)).toContain("del:m7@3");
    // wire Row shape is the unchanged tail projection
    expect(Object.keys(rows[0]!).sort()).toEqual(["hlc_c", "hlc_l", "key", "kind", "payload", "seq", "writer"]);
    expect(rows[0]!.key).toBe(`server:${String(rows[0]!.seq)}`);
  }, 20_000);

  it("an old-policy (non-keyed) subscriber interoperates: same schema hash, same wire", async () => {
    const t = "k.s2";
    const h = await subHarness(t, PLAIN);
    put(h.server, t, "a", 1);
    put(h.server, t, "a", 2);
    await h.sched.run({ untilMs: 400 });
    const { got } = h.subscribe();
    await h.sched.run({ untilMs: 1_000 });
    expect(got.snaps.map((s) => s.rows.map(rowFmt))).toEqual([["msg:a@2"]]);
    put(h.server, t, "a", 3);
    await h.sched.run({ untilMs: 1_400 });
    expect(got.deltas.map((d) => d.map(rowFmt))).toEqual([["msg:a@3"]]);
  });

  it("selector: latestPerKey(W) ∪ rowsAfter(W) keeps the committed version under an in-flight rewrite", async () => {
    const t = "k.s3";
    const h = await subHarness(t);
    put(h.server, t, "a", 1);
    put(h.server, t, "b", 1);
    put(h.server, t, "commit", 1);
    await h.sched.run({ untilMs: 300 });
    put(h.server, t, "a", 2); // in-flight frame
    await h.sched.run({ untilMs: 400 });

    const seen: TailSource[] = [];
    h.server.setTailSnapshotSelector((src) => {
      seen.push(src);
      if (!src.keyed) return null;
      const W = src.keyHead("commit")?.rowid ?? null;
      if (W === null) return null;
      return [...src.latestPerKey(W), ...src.rowsAfter(W)].map((r) => r.entry);
    });
    const { got } = h.subscribe();
    await h.sched.run({ untilMs: 1_000 });
    expect(seen[0]!.keyed).toBe(true);
    expect(got.snaps.map((s) => s.rows.map(rowFmt))).toEqual([["msg:a@1", "msg:b@1", "msg:commit@1", "msg:a@2"]]);
    // without the selector the default collapses a@1 away (latestPerKey(null))
    expect(latest(h.server, t)).toEqual(["msg:b@1", "msg:commit@1", "msg:a@2"]);
  });

  it("pruneSuperseded runs under a live subscriber (which keeps streaming); pruneTopic refuses on a keyed topic while subscribed", async () => {
    const t = "k.s4";
    const h = await subHarness(t);
    for (let v = 1; v <= 3; v++) for (const k of ["a", "b"]) put(h.server, t, k, v);
    await h.sched.run({ untilMs: 300 });
    expect(h.server.tailSubscriberCount(t)).toBe(0);
    const { got } = h.subscribe();
    await h.sched.run({ untilMs: 800 });
    expect(h.server.tailSubscriberCount(t)).toBe(1);

    const W = h.server.keyHead(t, "b")!.rowid;
    const r = await run(h.sched, h.server.pruneSuperseded(t, { uptoRowid: W }), 1_000);
    expect(r.prunedRows).toBe(4);
    await expect(
      runRejecting(h.sched, h.server.pruneTopic(t, { keepNewest: 1 }), 1_200),
    ).rejects.toMatchObject({ code: "ERR_MISUSE" });
    expect(h.server.stats().topics[t]?.logRows).toBe(2);

    put(h.server, t, "a", 4);
    await h.sched.run({ untilMs: 1_600 });
    expect(got.snaps.map((s) => s.rows.map(rowFmt))).toEqual([["msg:a@3", "msg:b@3"]]);
    expect(got.deltas.map((d) => d.map(rowFmt))).toEqual([["msg:a@4"]]);
  });

  it("tailSubscriberCount rejects an unknown topic", () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    expect(() => node.tailSubscriberCount("no.such")).toThrow(SeqscribeError);
  });
});
