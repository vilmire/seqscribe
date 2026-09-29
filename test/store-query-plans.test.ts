// sq_log per-topic hot-path query plans (incident 2026-09-28).
//
// sq_log's only per-topic indexes used to be the UNIQUE(topic, writer, seq)
// autoindex, sq_log_order and sq_log_key. "WHERE topic = ? [AND rowid < ?|>?]
// ORDER BY rowid [DESC] LIMIT ?" — the tail SNAP read, the tail-snapshot
// selector's backward page, the per-topic cursor walk and pruneTopic's batch
// select — then planned as an autoindex lookup plus USE TEMP B-TREE FOR ORDER
// BY: a full sort of every row of the topic, payload included, for a LIMIT 64
// page. On a 160k-row / 2.3 GB transcript topic each page blocked the event
// loop for ~2 minutes (2.65 GB temp file), long enough to time out every P2P
// peer. sq_log_topic_rowid (topic) carries rowid as its trailing key, so these
// become index range walks.
//
// The SQL checked here is captured from the real Store methods through a
// recording SqliteHandle — never a copy of the query text — so a future edit to
// a query is checked in the shape it actually ships.

import { describe, expect, it } from "vitest";
import { memoryHandle } from "../harness/sqlite.js";
import { Store } from "../src/store.js";
import type { SqliteHandle } from "../src/types.js";

interface Captured {
  sql: string;
  params: unknown[];
}

function recordingHandle(inner: SqliteHandle, log: Captured[], runLog?: Captured[]): SqliteHandle {
  return {
    run: (sql, params) => {
      runLog?.push({ sql, params: params ?? [] });
      return inner.run(sql, params);
    },
    get: (sql, params) => {
      log.push({ sql, params: params ?? [] });
      return inner.get(sql, params);
    },
    all: (sql, params) => {
      log.push({ sql, params: params ?? [] });
      return inner.all(sql, params);
    },
    transaction: (fn) => inner.transaction(fn),
    acquireOwnerLock: () => inner.acquireOwnerLock(),
    releaseOwnerLock: () => inner.releaseOwnerLock(),
  };
}

function seed(store: Store): void {
  // Several interleaved topics and two writers, so neither the PK range nor a
  // single-topic table degenerates into an accidentally cheap plan.
  store.transaction(() => {
    for (let i = 1; i <= 400; i++) {
      for (const topic of ["session.a.transcript", "session.b.transcript", "mesh.m.events"]) {
        const writer = i % 2 === 0 ? "w1" : "w2";
        store.insertEntry({
          topic,
          writer,
          seq: i,
          hlc: { l: 1_000 + i, c: 0 },
          kind: "chunk",
          payload: { i, pad: "x".repeat(64) },
          chain: `c${i}`,
        });
      }
    }
  });
}

// A keyed-append topic (host-guide §4.7): 40 keys rewritten 10 times each,
// every 7th row a tombstone, two writers — so per-key runs, tombstones and
// other-writer rows all exist for the keyed-query plans below.
const KEYED = "session.k.chat";
function seedKeyed(store: Store): void {
  store.transaction(() => {
    for (let i = 1; i <= 400; i++) {
      store.insertEntry({
        topic: KEYED,
        writer: i % 5 === 0 ? "w2" : "w1",
        seq: i,
        hlc: { l: 2_000 + i, c: 0 },
        kind: i % 7 === 0 ? "del" : "msg",
        key: `m:${i % 40}`,
        payload: { i, pad: "y".repeat(64) },
        chain: `k${i}`,
      });
    }
  });
}

function planOf(db: SqliteHandle, q: Captured): string {
  return db
    .all<{ detail: string }>(`EXPLAIN QUERY PLAN ${q.sql}`, q.params)
    .map((r) => r.detail)
    .join(" | ");
}

function capture(store: Store, log: Captured[], fn: () => unknown): Captured[] {
  const start = log.length;
  fn();
  return log.slice(start);
}

describe("sq_log per-topic hot-path query plans", () => {
  const inner = memoryHandle();
  const log: Captured[] = [];
  const runLog: Captured[] = [];
  const store = new Store(recordingHandle(inner, log, runLog));
  store.init("normal");
  seed(store);
  seedKeyed(store);
  // Plans are checked both without and with ANALYZE statistics — a planner
  // that only behaves with stats would regress on every DB that never ran it.
  const variants: [string, () => void][] = [
    ["no ANALYZE", () => inner.run("DROP TABLE IF EXISTS sqlite_stat1")],
    ["after ANALYZE", () => inner.run("ANALYZE")],
  ];

  const topic = "session.a.transcript";
  const rowidPaths: [string, () => unknown][] = [
    ["entriesTailByRowid", () => store.entriesTailByRowid(topic, 64)],
    ["entriesTailPage(head)", () => store.entriesTailPage(topic, null, 64)],
    ["entriesTailPage(before)", () => store.entriesTailPage(topic, 900, 64)],
    ["entriesForTopicFromRowid", () => store.entriesForTopicFromRowid(topic, 10, 64)],
    ["rowidAtTailOffset", () => store.rowidAtTailOffset(topic, 50)],
    ["deleteLogRowsUpToRowid(select)", () => store.deleteLogRowsUpToRowid(topic, 0, null)],
    ["maxRowid", () => store.maxRowid(topic)],
    ["minRowidForTopic", () => store.minRowidForTopic(topic)],
  ];

  for (const [variant, prepare] of variants) {
    for (const [name, fn] of rowidPaths) {
      it(`${name} walks sq_log_topic_rowid with no TEMP B-TREE (${variant})`, () => {
        prepare();
        const queries = capture(store, log, fn);
        expect(queries.length).toBeGreaterThan(0);
        for (const q of queries) {
          const plan = planOf(inner, q);
          expect(plan, q.sql).not.toMatch(/TEMP B-TREE/);
          expect(plan, q.sql).toMatch(/sq_log_topic_rowid/);
        }
      });
    }

    // Keyed-append reads/prunes (latestPerKey SNAP + scanLatestPerKey,
    // keyHead watermark lookup, pruneSuperseded's three candidate selects):
    // every one is an index walk plus sq_log_key seeks — no TEMP B-TREE, and
    // never a bare SCAN of sq_log.
    const keyedPaths: [string, RegExp[], () => unknown][] = [
      ["latestPerKeyPage(null)", [/sq_log_topic_rowid/, /sq_log_key/], () => store.latestPerKeyPage(KEYED, null, 0, 64)],
      ["latestPerKeyPage(upto)", [/sq_log_topic_rowid/, /sq_log_key/], () => store.latestPerKeyPage(KEYED, 1_000, 100, 64)],
      ["keyHead", [/sq_log_key/], () => store.keyHead(KEYED, "m:3")],
      ["supersededRowids", [/sq_log_key/], () => store.supersededRowids(KEYED, 1_000, 1_100, 64)],
      ["loneTombstoneRowids", [/sq_log_key/], () => store.loneTombstoneRowids(KEYED, "del", 1_000, 1_100, 64)],
      ["otherWriterRowids", [/sq_log_topic_rowid/], () => store.otherWriterRowids(KEYED, "w1", 1_000, 64)],
    ];
    for (const [name, indexes, fn] of keyedPaths) {
      it(`${name} uses its indexes with no TEMP B-TREE and no table scan (${variant})`, () => {
        prepare();
        const queries = capture(store, log, fn);
        expect(queries.length).toBeGreaterThan(0);
        for (const q of queries) {
          const plan = planOf(inner, q);
          expect(plan, q.sql).not.toMatch(/TEMP B-TREE/);
          expect(plan, q.sql).not.toMatch(/SCAN (a|b|sq_log)(?! USING)/);
          for (const ix of indexes) expect(plan, q.sql).toMatch(ix);
        }
      });
    }

    // Acknowledged retention (host-guide §4.8): the prefix walk and the head
    // probes stay on the UNIQUE(topic, writer, seq) key; the ack/floor tables
    // are read by their primary keys — none sorts, none scans.
    const retentionPaths: [string, RegExp, () => unknown][] = [
      ["streamHead", /sqlite_autoindex_sq_log_1/, () => store.streamHead("mesh.m.events", "w1", 64)],
      ["chainAt", /sqlite_autoindex_sq_log_1/, () => store.chainAt("mesh.m.events", "w1", 10)],
      ["hlcLAt", /sqlite_autoindex_sq_log_1/, () => store.hlcLAt("mesh.m.events", "w1", 10)],
      ["acksForTopic", /sqlite_autoindex_sq_acks_1/, () => store.acksForTopic("mesh.m.events")],
      ["ackNodes", /sqlite_autoindex_sq_ack_nodes_1/, () => store.ackNodes("mesh.m.events")],
      ["floorGet", /sqlite_autoindex_sq_floors_1/, () => store.floorGet("mesh.m.events", "w1")],
      ["floorsForTopic", /sqlite_autoindex_sq_floors_1/, () => store.floorsForTopic("mesh.m.events")],
    ];
    for (const [name, index, fn] of retentionPaths) {
      it(`${name} seeks ${index.source} with no TEMP B-TREE and no table scan (${variant})`, () => {
        prepare();
        const queries = capture(store, log, fn);
        expect(queries.length).toBeGreaterThan(0);
        for (const q of queries) {
          const plan = planOf(inner, q);
          expect(plan, q.sql).not.toMatch(/TEMP B-TREE/);
          expect(plan, q.sql).not.toMatch(/SCAN (sq_log|sq_acks|sq_ack_nodes|sq_floors)(?! USING)/);
          expect(plan, q.sql).toMatch(index);
        }
      });
    }

    // The DELETEs pruneAcked / floor adoption issue go through run(), which
    // the handle records separately. Captured from a throwaway topic so the
    // seeded rows the other plans read stay put; every one is a key-range
    // delete (no SCAN of any table).
    it(`retention DELETEs are key-range deletes (${variant})`, () => {
      prepare();
      const start = runLog.length;
      store.deleteStreamPrefix("mesh.none.events", "w1", 5);
      store.deletePendingUpTo("mesh.none.events", "w1", 5);
      store.ackForget("mesh.none.events", "n1");
      const deletes = runLog.slice(start).filter((q) => q.sql.startsWith("DELETE"));
      expect(deletes).toHaveLength(5);
      for (const q of deletes) {
        const plan = planOf(inner, q);
        expect(plan, q.sql).not.toMatch(/SCAN/);
        expect(plan, q.sql).toMatch(/USING (COVERING )?INDEX sqlite_autoindex_sq_/);
      }
    });

    it(`entriesAfterOrder walks sq_log_order with no TEMP B-TREE (${variant})`, () => {
      prepare();
      const queries = [
        ...capture(store, log, () => store.entriesAfterOrder(topic, null, 64)),
        ...capture(store, log, () =>
          store.entriesAfterOrder(topic, { l: 1_100, c: 0, writer: "w1", seq: 100 }, 64),
        ),
      ];
      for (const q of queries) {
        const plan = planOf(inner, q);
        expect(plan, q.sql).not.toMatch(/TEMP B-TREE/);
        expect(plan, q.sql).toMatch(/sq_log_order/);
      }
    });

    it(`entriesRange / getEntry stay on the (topic, writer, seq) key (${variant})`, () => {
      prepare();
      const queries = [
        ...capture(store, log, () => store.entriesRange(topic, "w1", 1, 64)),
        ...capture(store, log, () => store.getEntry(topic, "w1", 2)),
      ];
      for (const q of queries) {
        const plan = planOf(inner, q);
        expect(plan, q.sql).not.toMatch(/TEMP B-TREE/);
        expect(plan, q.sql).toMatch(/sqlite_autoindex_sq_log_1/);
      }
    });
  }

  it("rowidAtTailOffset returns the n-th newest rowid of the topic, or null", () => {
    const all = store.entriesTailByRowid(topic, 1_000).map((r) => r.rowid);
    expect(all).toHaveLength(400);
    expect(store.rowidAtTailOffset(topic, 1)).toBe(all[all.length - 1]);
    expect(store.rowidAtTailOffset(topic, 50)).toBe(all[all.length - 50]);
    expect(store.rowidAtTailOffset(topic, 400)).toBe(all[0]);
    expect(store.rowidAtTailOffset(topic, 401)).toBeNull();
    expect(store.rowidAtTailOffset(topic, 0)).toBeNull();
    expect(store.rowidAtTailOffset("no.such.topic", 1)).toBeNull();
  });
});
