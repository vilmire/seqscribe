// SPEC §8 — DDL and typed accessors. All sq_log/sq_writers mutations flow
// through LogCore's commit queue; this module never opens its own transactions
// except in init().

import { SeqscribeError } from "./errors.js";
import type { JsonValue, LogEntry, Order, Seq, SqliteHandle, Topic, WriterId } from "./types.js";

// sq_log_topic_rowid — per-topic rowid order. A single-column index carries
// rowid as its trailing key, so "WHERE topic=? [AND rowid<?|>?] ORDER BY rowid
// [DESC] LIMIT ?" (tail SNAP, tail-snapshot selector pages, per-topic cursor
// walks, pruneTopic) is an index range walk instead of the UNIQUE(topic,
// writer, seq) autoindex lookup + TEMP B-TREE sort of every row of the topic
// (payload included) that those reads paid before — measured at ~2 minutes of
// blocked event loop per 64-row page on a 160k-row / 2.3 GB topic
// (2026-09-28). IF NOT EXISTS: an existing DB builds it once on its next
// open — one pass over sq_log's leaf pages (topic sits in the local part of
// every row, so overflow pages holding large payloads are not read), ~2-3 s
// on a 3.5 GB / 296k-row DB. The DDL string below must stay free of ';'
// inside comments: init() splits it on ';'.
const DDL = `
CREATE TABLE IF NOT EXISTS sq_log (
  rowid INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL, writer TEXT NOT NULL, seq INTEGER NOT NULL,
  hlc_l INTEGER NOT NULL, hlc_c INTEGER NOT NULL,
  kind TEXT NOT NULL, key TEXT,
  causal_w TEXT, causal_s INTEGER,
  ref_t TEXT, ref_w TEXT, ref_s INTEGER,
  payload TEXT NOT NULL, chain TEXT NOT NULL,
  UNIQUE (topic, writer, seq));
CREATE INDEX IF NOT EXISTS sq_log_order ON sq_log (topic, hlc_l, hlc_c, writer, seq);
CREATE INDEX IF NOT EXISTS sq_log_key ON sq_log (topic, key) WHERE key IS NOT NULL;
CREATE INDEX IF NOT EXISTS sq_log_topic_rowid ON sq_log (topic);

CREATE TABLE IF NOT EXISTS sq_pending (topic TEXT, writer TEXT, seq INTEGER, entry TEXT,
  PRIMARY KEY (topic, writer, seq));
CREATE TABLE IF NOT EXISTS sq_quarantine (topic TEXT, writer TEXT, seq INTEGER, entry TEXT,
  reason TEXT, at TEXT, PRIMARY KEY (topic, writer, seq));

CREATE TABLE IF NOT EXISTS sq_writers (
  topic TEXT NOT NULL, writer TEXT NOT NULL,
  contig_seq INTEGER NOT NULL, contig_chain TEXT NOT NULL,
  seal_reason TEXT,
  rgen INTEGER NOT NULL DEFAULT 0, retired_at TEXT, final_seq INTEGER, final_chain TEXT,
  PRIMARY KEY (topic, writer));

CREATE TABLE IF NOT EXISTS sq_annotations (topic TEXT, writer TEXT, seq INTEGER, kind TEXT, at TEXT,
  PRIMARY KEY (topic, writer, seq, kind));

CREATE TABLE IF NOT EXISTS sq_cursors (consumer TEXT, topic TEXT, last_rowid INTEGER NOT NULL,
  updated_at TEXT NOT NULL, PRIMARY KEY (consumer, topic));

CREATE TABLE IF NOT EXISTS sq_checkpoints (
  topic TEXT NOT NULL, view TEXT NOT NULL, view_version TEXT NOT NULL,
  ord_l INTEGER NOT NULL, ord_c INTEGER NOT NULL,
  ord_w TEXT NOT NULL, ord_s INTEGER NOT NULL,
  state TEXT NOT NULL,
  PRIMARY KEY (topic, view, view_version, ord_l, ord_c, ord_w, ord_s));

CREATE TABLE IF NOT EXISTS sq_snapshots  (topic TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sq_finality   (topic TEXT PRIMARY KEY, cert TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sq_directives (topic TEXT, writer TEXT, rgen INTEGER, directive TEXT NOT NULL,
  PRIMARY KEY (topic, writer, rgen));
CREATE TABLE IF NOT EXISTS sq_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sq_acks (
  topic TEXT NOT NULL, node TEXT NOT NULL, writer TEXT NOT NULL, seq INTEGER NOT NULL,
  PRIMARY KEY (topic, node, writer));
CREATE TABLE IF NOT EXISTS sq_ack_nodes (
  topic TEXT NOT NULL, node TEXT NOT NULL, first_at INTEGER NOT NULL, seen_at INTEGER, proto INTEGER,
  PRIMARY KEY (topic, node));
CREATE TABLE IF NOT EXISTS sq_floors (
  topic TEXT NOT NULL, writer TEXT NOT NULL, seq INTEGER NOT NULL, chain TEXT NOT NULL, at INTEGER NOT NULL,
  PRIMARY KEY (topic, writer));
CREATE TABLE IF NOT EXISTS sq_archive (
  topic TEXT NOT NULL, writer TEXT NOT NULL, seq INTEGER NOT NULL,
  entry TEXT NOT NULL, archived_at TEXT NOT NULL,
  PRIMARY KEY (topic, writer, seq));
`;

export type SealReason = "fork" | "retired" | null;

export interface WriterRow {
  topic: Topic;
  writer: WriterId;
  contigSeq: Seq;
  contigChain: string;
  sealReason: SealReason;
  rgen: number;
  retiredAt: string | null;
  finalSeq: Seq | null;
  finalChain: string | null;
}

interface RawLogRow {
  rowid: number;
  topic: string;
  writer: string;
  seq: number;
  hlc_l: number;
  hlc_c: number;
  kind: string;
  key: string | null;
  causal_w: string | null;
  causal_s: number | null;
  ref_t: string | null;
  ref_w: string | null;
  ref_s: number | null;
  payload: string;
  chain: string;
}

function rowToEntry(r: RawLogRow): { entry: LogEntry; rowid: number } {
  const entry: LogEntry = {
    topic: r.topic,
    writer: r.writer,
    seq: r.seq,
    hlc: { l: r.hlc_l, c: r.hlc_c },
    kind: r.kind,
    payload: JSON.parse(r.payload) as JsonValue,
    chain: r.chain,
  };
  if (r.key !== null) entry.key = r.key;
  if (r.causal_w !== null && r.causal_s !== null) entry.causal = [r.causal_w, r.causal_s];
  if (r.ref_t !== null && r.ref_w !== null && r.ref_s !== null)
    entry.ref = [r.ref_t, r.ref_w, r.ref_s];
  return { entry, rowid: r.rowid };
}

export class Store {
  // count caches: exact via +1 on insert, invalidated (recount lazily) on any
  // delete/archive — stats() must not pay a COUNT(*) table scan per call
  private readonly logCounts = new Map<Topic, number>();
  private readonly archiveCounts = new Map<Topic, number>();

  constructor(private readonly db: SqliteHandle) {}

  init(durability: "normal" | "full"): void {
    try {
      this.db.acquireOwnerLock();
    } catch (e) {
      throw new SeqscribeError("ERR_DB_OWNED", e instanceof Error ? e.message : String(e));
    }
    this.db.run("PRAGMA journal_mode=WAL");
    this.db.run(`PRAGMA synchronous=${durability === "full" ? "FULL" : "NORMAL"}`);
    for (const stmt of DDL.split(";")) {
      const sql = stmt.trim();
      if (sql) this.db.run(sql);
    }
  }

  close(): void {
    this.db.releaseOwnerLock();
  }

  transaction<T>(fn: () => T): T {
    return this.db.transaction(fn);
  }

  insertEntry(e: LogEntry): number {
    const res = this.db.run(
      `INSERT INTO sq_log (topic, writer, seq, hlc_l, hlc_c, kind, key,
        causal_w, causal_s, ref_t, ref_w, ref_s, payload, chain)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        e.topic,
        e.writer,
        e.seq,
        e.hlc.l,
        e.hlc.c,
        e.kind,
        e.key ?? null,
        e.causal ? e.causal[0] : null,
        e.causal ? e.causal[1] : null,
        e.ref ? e.ref[0] : null,
        e.ref ? e.ref[1] : null,
        e.ref ? e.ref[2] : null,
        JSON.stringify(e.payload),
        e.chain,
      ],
    );
    const cached = this.logCounts.get(e.topic);
    if (cached !== undefined) this.logCounts.set(e.topic, cached + 1);
    return Number(res.lastInsertRowid);
  }

  getEntry(topic: Topic, writer: WriterId, seq: Seq): LogEntry | undefined {
    const r = this.db.get<RawLogRow>(
      "SELECT rowid, * FROM sq_log WHERE topic=? AND writer=? AND seq=?",
      [topic, writer, seq],
    );
    return r ? rowToEntry(r).entry : undefined;
  }

  entriesFromRowid(afterRowid: number, limit: number): { entry: LogEntry; rowid: number }[] {
    return this.db
      .all<RawLogRow>("SELECT rowid, * FROM sq_log WHERE rowid > ? ORDER BY rowid LIMIT ?", [
        afterRowid,
        limit,
      ])
      .map(rowToEntry);
  }

  entriesForTopicFromRowid(
    topic: Topic,
    afterRowid: number,
    limit: number,
  ): { entry: LogEntry; rowid: number }[] {
    return this.db
      .all<RawLogRow>(
        "SELECT rowid, * FROM sq_log INDEXED BY sq_log_topic_rowid WHERE topic = ? AND rowid > ? ORDER BY rowid LIMIT ?",
        [topic, afterRowid, limit],
      )
      .map(rowToEntry);
  }

  // SubHub's `tail` view on a `full`-retention topic (rowid order == insertion
  // order == append order, same as the ring tail's push order): last `limit`
  // rows, re-ascended to rowid order for the SNAP body. Two queries, not one
  // ORDER BY rowid DESC LIMIT ? in a subquery, because sqlite's query planner
  // does not reliably use the `sq_log` rowid ordering for a wrapped subquery
  // ORDER BY without a matching index hint, and this method runs on every
  // fresh/reset SUB — worth the second round trip to keep the plan obvious.
  // Every per-topic rowid-ordered read here names INDEXED BY
  // sq_log_topic_rowid: without it the planner may pick the (topic, writer,
  // seq) autoindex + a TEMP B-TREE sort of the whole topic, or (with ANALYZE
  // stats) a PK range walk across every other topic's rows — both
  // topic-size-proportional for a LIMIT-bounded page. init() always creates
  // the index, so the hint can never name a missing one.
  entriesTailByRowid(topic: Topic, limit: number): { entry: LogEntry; rowid: number }[] {
    const rows = this.db
      .all<RawLogRow>(
        "SELECT rowid, * FROM sq_log INDEXED BY sq_log_topic_rowid WHERE topic = ? ORDER BY rowid DESC LIMIT ?",
        [topic, limit],
      )
      .map(rowToEntry);
    rows.reverse(); // DESC fetch, ASC delivery — oldest-first, same as ringTail()
    return rows;
  }

  // Rowid of the `n`-th newest row of `topic` (n >= 1), or null when the topic
  // holds fewer than `n` rows. A rowid-only walk of sq_log_topic_rowid — never
  // reads a payload — so pruneTopic's keepNewest floor costs O(n) index
  // entries instead of materializing (and JSON-parsing) the n newest rows the
  // way entriesTailByRowid(topic, n) would. keepNewest is routinely "all but
  // the oldest few hundred rows" (writer-gc steps down from the current row
  // count), so that materialization was a whole-topic read: on a 160k-row /
  // 2.3 GB transcript topic it exhausted the V8 heap (daemon OOM, 2026-09-28).
  rowidAtTailOffset(topic: Topic, n: number): number | null {
    if (!Number.isSafeInteger(n) || n < 1) return null;
    const r = this.db.get<{ rowid: number }>(
      "SELECT rowid FROM sq_log INDEXED BY sq_log_topic_rowid WHERE topic = ? ORDER BY rowid DESC LIMIT 1 OFFSET ?",
      [topic, n - 1],
    );
    return r ? r.rowid : null;
  }

  // Newest-first page of `topic`'s rows strictly below `beforeRowid` (null =
  // from the head). The backward walk a tail-snapshot selector uses to find a
  // window boundary without materializing the whole FULL_TAIL_DEFAULT tail.
  entriesTailPage(
    topic: Topic,
    beforeRowid: number | null,
    limit: number,
  ): { entry: LogEntry; rowid: number }[] {
    const rows =
      beforeRowid === null
        ? this.db.all<RawLogRow>(
            "SELECT rowid, * FROM sq_log INDEXED BY sq_log_topic_rowid WHERE topic = ? ORDER BY rowid DESC LIMIT ?",
            [topic, limit],
          )
        : this.db.all<RawLogRow>(
            "SELECT rowid, * FROM sq_log INDEXED BY sq_log_topic_rowid WHERE topic = ? AND rowid < ? ORDER BY rowid DESC LIMIT ?",
            [topic, beforeRowid, limit],
          );
    return rows.map(rowToEntry);
  }

  // ---- keyed append topics (TopicPolicy.keyed, host-guide §4.7) ----
  //
  // Every query below is an index walk: the outer loop names its index
  // (sq_log_topic_rowid for rowid-ordered pages, sq_log_key for per-key
  // work), and the correlated per-key probe is a seek on sq_log_key, whose
  // trailing rowid makes "a newer row of this key at or below W" a range
  // check inside one (topic, key) run. sq_log_key is PARTIAL (key IS NOT
  // NULL); every probe carries an explicit `key IS NOT NULL` so the planner
  // can prove the index usable without relying on implied-not-null
  // inference. No query here sorts (no TEMP B-TREE) or materializes a topic:
  // callers page with LIMIT and resume by rowid.

  // One page of the newest row per key, in rowid order, restricted to rows
  // with afterRowid < rowid <= uptoRowid (null = no upper bound). A row is
  // "newest" when no row of the same key exists in (rowid, uptoRowid] — so
  // supersession is judged AT the watermark: a row newer than uptoRowid never
  // hides an older one. Key-less rows (never produced on a keyed topic) are
  // their own key and always qualify. Superseded rows are skipped inside the
  // index walk (their key column is read, their payload is not parsed).
  latestPerKeyPage(
    topic: Topic,
    uptoRowid: number | null,
    afterRowid: number,
    limit: number,
  ): { entry: LogEntry; rowid: number }[] {
    const upto = uptoRowid ?? Number.MAX_SAFE_INTEGER;
    return this.db
      .all<RawLogRow>(
        `SELECT a.* FROM sq_log AS a INDEXED BY sq_log_topic_rowid
         WHERE a.topic = ? AND a.rowid > ? AND a.rowid <= ?
           AND (a.key IS NULL OR NOT EXISTS (
             SELECT 1 FROM sq_log AS b INDEXED BY sq_log_key
             WHERE b.topic = ? AND b.key IS NOT NULL AND b.key = a.key
               AND b.rowid > a.rowid AND b.rowid <= ?))
         ORDER BY a.rowid LIMIT ?`,
        [topic, afterRowid, upto, topic, upto, limit],
      )
      .map(rowToEntry);
  }

  // Newest row of one key (any rowid), or undefined. A single descending
  // seek on sq_log_key — how a host finds its commit watermark W.
  keyHead(topic: Topic, key: string): { entry: LogEntry; rowid: number } | undefined {
    const r = this.db.get<RawLogRow>(
      `SELECT * FROM sq_log INDEXED BY sq_log_key
       WHERE topic = ? AND key IS NOT NULL AND key = ? ORDER BY rowid DESC LIMIT 1`,
      [topic, key],
    );
    return r ? rowToEntry(r) : undefined;
  }

  // pruneSuperseded (a): rows at or below `floorRowid` that a NEWER row of
  // the same key at or below `uptoRowid` supersedes. Walks sq_log_key in
  // (key, rowid) order — index-only for the candidate AND the probe.
  supersededRowids(topic: Topic, floorRowid: number, uptoRowid: number, limit: number): number[] {
    return this.db
      .all<{ rowid: number }>(
        `SELECT a.rowid AS rowid FROM sq_log AS a INDEXED BY sq_log_key
         WHERE a.topic = ? AND a.key IS NOT NULL AND a.rowid <= ?
           AND EXISTS (
             SELECT 1 FROM sq_log AS b INDEXED BY sq_log_key
             WHERE b.topic = ? AND b.key IS NOT NULL AND b.key = a.key
               AND b.rowid > a.rowid AND b.rowid <= ?)
         LIMIT ?`,
        [topic, floorRowid, topic, uptoRowid, limit],
      )
      .map((r) => r.rowid);
  }

  // pruneSuperseded (b): tombstones (`kind = tombstoneKind`) at or below
  // `floorRowid` that are the ONLY row of their key at or below `uptoRowid`.
  // An older row of the key still present (e.g. held by a consumer cursor)
  // keeps its tombstone — deleting it would resurrect that row. Rows of the
  // key newer than uptoRowid (an uncommitted re-creation) do not block.
  loneTombstoneRowids(
    topic: Topic,
    tombstoneKind: string,
    floorRowid: number,
    uptoRowid: number,
    limit: number,
  ): number[] {
    return this.db
      .all<{ rowid: number }>(
        `SELECT a.rowid AS rowid FROM sq_log AS a INDEXED BY sq_log_key
         WHERE a.topic = ? AND a.key IS NOT NULL AND a.rowid <= ? AND a.kind = ?
           AND NOT EXISTS (
             SELECT 1 FROM sq_log AS b INDEXED BY sq_log_key
             WHERE b.topic = ? AND b.key IS NOT NULL AND b.key = a.key
               AND b.rowid <> a.rowid AND b.rowid <= ?)
         LIMIT ?`,
        [topic, floorRowid, tombstoneKind, topic, uptoRowid, limit],
      )
      .map((r) => r.rowid);
  }

  // pruneSuperseded with supersedeOtherWriters: rows at or below `floorRowid`
  // authored by any writer other than `writer`, oldest first.
  otherWriterRowids(topic: Topic, writer: WriterId, floorRowid: number, limit: number): number[] {
    return this.db
      .all<{ rowid: number }>(
        `SELECT rowid FROM sq_log INDEXED BY sq_log_topic_rowid
         WHERE topic = ? AND rowid <= ? AND writer <> ? ORDER BY rowid LIMIT ?`,
        [topic, floorRowid, writer, limit],
      )
      .map((r) => r.rowid);
  }

  // Delete exactly these rows of `topic` — rowids the caller just selected in
  // the same transaction, so each exists (the count is not read from
  // run().changes, which the wasm/DO adapters do not report).
  deleteLogRowids(topic: Topic, rowids: number[]): void {
    for (const rowid of rowids) this.db.run("DELETE FROM sq_log WHERE rowid = ? AND topic = ?", [rowid, topic]);
    if (rowids.length > 0) this.logCounts.delete(topic); // recount lazily
  }

  // Total-order iteration (§1): entries strictly after `after` in
  // (hlc_l, hlc_c, writer, seq) order; after=null starts from the beginning.
  entriesAfterOrder(topic: Topic, after: Order | null, limit: number): LogEntry[] {
    const cond = after
      ? `AND (hlc_l > ? OR (hlc_l = ? AND (hlc_c > ? OR (hlc_c = ? AND
           (writer > ? OR (writer = ? AND seq > ?))))))`
      : "";
    const params: unknown[] = after
      ? [topic, after.l, after.l, after.c, after.c, after.writer, after.writer, after.seq, limit]
      : [topic, limit];
    return this.db
      .all<RawLogRow>(
        `SELECT rowid, * FROM sq_log WHERE topic = ? ${cond}
         ORDER BY hlc_l, hlc_c, writer, seq LIMIT ?`,
        params,
      )
      .map((r) => rowToEntry(r).entry);
  }

  maxOrderUpTo(topic: Topic, maxHlcL: number): Order | null {
    const r = this.db.get<RawLogRow>(
      `SELECT rowid, * FROM sq_log WHERE topic = ? AND hlc_l <= ?
       ORDER BY hlc_l DESC, hlc_c DESC, writer DESC, seq DESC LIMIT 1`,
      [topic, maxHlcL],
    );
    if (!r) return null;
    return { l: r.hlc_l, c: r.hlc_c, writer: r.writer, seq: r.seq };
  }

  // per-writer projection of a watermark: last seq with order ≤ P (§7.2)
  lastSeqAtOrBeforeOrder(topic: Topic, writer: WriterId, p: Order): Seq {
    const r = this.db.get<{ seq: number }>(
      `SELECT seq FROM sq_log WHERE topic = ? AND writer = ? AND
         (hlc_l < ? OR (hlc_l = ? AND (hlc_c < ? OR (hlc_c = ? AND
           (writer < ? OR (writer = ? AND seq <= ?))))))
       ORDER BY seq DESC LIMIT 1`,
      [topic, writer, p.l, p.l, p.c, p.c, p.writer, p.writer, p.seq],
    );
    return r?.seq ?? 0;
  }

  maxRowid(topic: Topic): number {
    const r = this.db.get<{ m: number | null }>(
      "SELECT MAX(rowid) AS m FROM sq_log WHERE topic = ?",
      [topic],
    );
    return r?.m ?? 0;
  }

  topicWriters(topic: Topic): WriterId[] {
    return this.db
      .all<{ writer: string }>("SELECT DISTINCT writer FROM sq_log WHERE topic = ?", [topic])
      .map((r) => r.writer);
  }

  entriesRange(
    topic: Topic,
    writer: WriterId,
    fromSeq: Seq,
    toSeq: Seq,
  ): { entry: LogEntry; rowid: number }[] {
    return this.db
      .all<RawLogRow>(
        "SELECT rowid, * FROM sq_log WHERE topic=? AND writer=? AND seq>=? AND seq<=? ORDER BY seq",
        [topic, writer, fromSeq, toSeq],
      )
      .map(rowToEntry);
  }

  getWriter(topic: Topic, writer: WriterId): WriterRow | undefined {
    const r = this.db.get<{
      topic: string;
      writer: string;
      contig_seq: number;
      contig_chain: string;
      seal_reason: string | null;
      rgen: number;
      retired_at: string | null;
      final_seq: number | null;
      final_chain: string | null;
    }>("SELECT * FROM sq_writers WHERE topic=? AND writer=?", [topic, writer]);
    if (!r) return undefined;
    return {
      topic: r.topic,
      writer: r.writer,
      contigSeq: r.contig_seq,
      contigChain: r.contig_chain,
      sealReason: (r.seal_reason as SealReason) ?? null,
      rgen: r.rgen,
      retiredAt: r.retired_at,
      finalSeq: r.final_seq,
      finalChain: r.final_chain,
    };
  }

  listWriters(topic?: Topic): WriterRow[] {
    const rows = topic
      ? this.db.all<Record<string, unknown>>("SELECT * FROM sq_writers WHERE topic=?", [topic])
      : this.db.all<Record<string, unknown>>("SELECT * FROM sq_writers");
    return rows.map((r) => ({
      topic: r.topic as string,
      writer: r.writer as string,
      contigSeq: r.contig_seq as number,
      contigChain: r.contig_chain as string,
      sealReason: (r.seal_reason as SealReason) ?? null,
      rgen: r.rgen as number,
      retiredAt: (r.retired_at as string | null) ?? null,
      finalSeq: (r.final_seq as number | null) ?? null,
      finalChain: (r.final_chain as string | null) ?? null,
    }));
  }

  upsertWriter(w: WriterRow): void {
    this.db.run(
      `INSERT INTO sq_writers (topic, writer, contig_seq, contig_chain, seal_reason, rgen,
        retired_at, final_seq, final_chain)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (topic, writer) DO UPDATE SET
         contig_seq=excluded.contig_seq, contig_chain=excluded.contig_chain,
         seal_reason=excluded.seal_reason, rgen=excluded.rgen,
         retired_at=excluded.retired_at, final_seq=excluded.final_seq,
         final_chain=excluded.final_chain`,
      [
        w.topic,
        w.writer,
        w.contigSeq,
        w.contigChain,
        w.sealReason,
        w.rgen,
        w.retiredAt,
        w.finalSeq,
        w.finalChain,
      ],
    );
  }

  // Writer-row GC (retireTopic/gcWriters). Deletes exactly one (topic, writer)
  // row — unlike sealing (upsertWriter with seal_reason='retired'), this is a
  // real DELETE, only ever used on subscribe-only ring/none topics with no
  // durable entries (LogCore.retireTopic enforces the preconditions before
  // calling this; this method itself does not re-check them).
  deleteWriter(topic: Topic, writer: WriterId): void {
    this.db.run("DELETE FROM sq_writers WHERE topic=? AND writer=?", [topic, writer]);
  }

  pendingPut(e: LogEntry): void {
    this.db.run(
      "INSERT OR REPLACE INTO sq_pending (topic, writer, seq, entry) VALUES (?, ?, ?, ?)",
      [e.topic, e.writer, e.seq, JSON.stringify(e)],
    );
  }

  pendingGet(topic: Topic, writer: WriterId, seq: Seq): LogEntry | undefined {
    const r = this.db.get<{ entry: string }>(
      "SELECT entry FROM sq_pending WHERE topic=? AND writer=? AND seq=?",
      [topic, writer, seq],
    );
    return r ? (JSON.parse(r.entry) as LogEntry) : undefined;
  }

  pendingDelete(topic: Topic, writer: WriterId, seq: Seq): void {
    this.db.run("DELETE FROM sq_pending WHERE topic=? AND writer=? AND seq=?", [
      topic,
      writer,
      seq,
    ]);
  }

  pendingCount(topic: Topic, writer: WriterId): number {
    const r = this.db.get<{ n: number }>(
      "SELECT COUNT(*) AS n FROM sq_pending WHERE topic=? AND writer=?",
      [topic, writer],
    );
    return r?.n ?? 0;
  }

  annotate(topic: Topic, writer: WriterId, seq: Seq, kind: string, at: string): void {
    this.db.run(
      "INSERT OR IGNORE INTO sq_annotations (topic, writer, seq, kind, at) VALUES (?, ?, ?, ?, ?)",
      [topic, writer, seq, kind, at],
    );
  }

  quarantinePut(e: LogEntry, reason: string, at: string): void {
    this.db.run(
      "INSERT OR REPLACE INTO sq_quarantine (topic, writer, seq, entry, reason, at) VALUES (?, ?, ?, ?, ?, ?)",
      [e.topic, e.writer, e.seq, JSON.stringify(e), reason, at],
    );
  }

  deleteLogRange(topic: Topic, writer: WriterId, fromSeq: Seq): void {
    this.db.run("DELETE FROM sq_log WHERE topic=? AND writer=? AND seq>=?", [
      topic,
      writer,
      fromSeq,
    ]);
    this.logCounts.delete(topic); // recount lazily
  }

  // Node.pruneTopic (local housekeeping GC — see log.ts processPruneTopic's
  // doc comment). Deletes rows at or below `belowRowid` (all writers), and,
  // when `hlcBefore` is given, additionally requires hlc_l < hlcBefore — the
  // caller passes the tighter of the two bounds it already computed
  // (keepNewest's rowid floor, olderThanMs's hlc_l floor) so this is always a
  // single bounded DELETE, never an unbounded scan. Batched like
  // archiveCovered: a topic's whole prunable region can be millions of rows
  // (the same reason archiveCovered doesn't do it in one statement), and
  // this runs inside the append queue's flush, which must not stall on one
  // giant transaction. Returns rows actually deleted.
  deleteLogRowsUpToRowid(topic: Topic, belowRowid: number, hlcBefore: number | null): number {
    const BATCH = 2_000;
    let total = 0;
    for (;;) {
      const cond = hlcBefore !== null ? "AND hlc_l < ?" : "";
      const params: unknown[] = hlcBefore !== null
        ? [topic, belowRowid, hlcBefore, BATCH]
        : [topic, belowRowid, BATCH];
      const rows = this.db.all<{ rowid: number }>(
        `SELECT rowid FROM sq_log WHERE topic = ? AND rowid <= ? ${cond} ORDER BY rowid LIMIT ?`,
        params,
      );
      if (rows.length === 0) break;
      this.db.transaction(() => {
        for (const r of rows) this.db.run("DELETE FROM sq_log WHERE rowid = ?", [r.rowid]);
      });
      total += rows.length;
      if (rows.length < BATCH) break;
    }
    if (total > 0) this.logCounts.delete(topic); // recount lazily
    return total;
  }

  // ---- acknowledged retention (host-guide §4.8) ----
  //
  // sq_acks / sq_ack_nodes: what each peer NODE (its HELLO writer id) last
  // advertised holding per stream of a full-sync topic, from its own HAVE —
  // tiny tables (nodes × writers per topic), every access a PK seek or a PK
  // prefix range. sq_floors: this node's per-stream retention floor — rows
  // with seq ≤ floor are not held here and are never served (a WANT below it
  // gets TRUNCATED). Monotone: floorSet never lowers a floor.

  ackUpsert(topic: Topic, node: WriterId, writer: WriterId, seq: Seq): void {
    this.db.run(
      `INSERT INTO sq_acks (topic, node, writer, seq) VALUES (?, ?, ?, ?)
       ON CONFLICT (topic, node, writer) DO UPDATE SET seq = excluded.seq`,
      [topic, node, writer, seq],
    );
  }

  // Observed liveness: first_at is set once (insert), seen_at/proto refresh.
  ackNodeSeen(topic: Topic, node: WriterId, at: number, proto: number | null): void {
    this.db.run(
      `INSERT INTO sq_ack_nodes (topic, node, first_at, seen_at, proto) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (topic, node) DO UPDATE SET seen_at = excluded.seen_at,
         proto = COALESCE(excluded.proto, sq_ack_nodes.proto)`,
      [topic, node, at, at, proto],
    );
  }

  // A host-named member never observed yet: start its max-lag clock (no-op if known).
  ackNodeName(topic: Topic, node: WriterId, at: number): void {
    this.db.run(
      "INSERT OR IGNORE INTO sq_ack_nodes (topic, node, first_at, seen_at, proto) VALUES (?, ?, ?, NULL, NULL)",
      [topic, node, at],
    );
  }

  ackNodes(topic: Topic): { node: WriterId; firstAt: number; seenAt: number | null; proto: number | null }[] {
    return this.db
      .all<{ node: string; first_at: number; seen_at: number | null; proto: number | null }>(
        "SELECT node, first_at, seen_at, proto FROM sq_ack_nodes WHERE topic = ?",
        [topic],
      )
      .map((r) => ({ node: r.node, firstAt: r.first_at, seenAt: r.seen_at, proto: r.proto }));
  }

  acksForTopic(topic: Topic): { node: WriterId; writer: WriterId; seq: Seq }[] {
    return this.db.all<{ node: string; writer: string; seq: number }>(
      "SELECT node, writer, seq FROM sq_acks WHERE topic = ?",
      [topic],
    );
  }

  ackForget(topic: Topic, node: WriterId): void {
    this.db.run("DELETE FROM sq_acks WHERE topic = ? AND node = ?", [topic, node]);
    this.db.run("DELETE FROM sq_ack_nodes WHERE topic = ? AND node = ?", [topic, node]);
  }

  floorGet(topic: Topic, writer: WriterId): { seq: Seq; chain: string } | undefined {
    return this.db.get<{ seq: number; chain: string }>(
      "SELECT seq, chain FROM sq_floors WHERE topic = ? AND writer = ?",
      [topic, writer],
    );
  }

  floorSet(topic: Topic, writer: WriterId, seq: Seq, chain: string, at: number): void {
    this.db.run(
      `INSERT INTO sq_floors (topic, writer, seq, chain, at) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (topic, writer) DO UPDATE SET seq = excluded.seq, chain = excluded.chain,
         at = excluded.at WHERE excluded.seq > sq_floors.seq`,
      [topic, writer, seq, chain, at],
    );
  }

  floorsForTopic(topic: Topic): { writer: WriterId; seq: Seq; chain: string }[] {
    return this.db.all<{ writer: string; seq: number; chain: string }>(
      "SELECT writer, seq, chain FROM sq_floors WHERE topic = ?",
      [topic],
    );
  }

  // The oldest `limit` rows of one stream, seq order — pruneAcked's prefix
  // walk. A range walk of the UNIQUE(topic, writer, seq) key; reads rowid and
  // hlc_l only (both precede the payload column, so an overflowing payload's
  // pages are never touched).
  streamHead(topic: Topic, writer: WriterId, limit: number): { rowid: number; seq: Seq; hlcL: number }[] {
    return this.db
      .all<{ rowid: number; seq: number; hlc_l: number }>(
        "SELECT rowid, seq, hlc_l FROM sq_log WHERE topic = ? AND writer = ? ORDER BY seq LIMIT ?",
        [topic, writer, limit],
      )
      .map((r) => ({ rowid: r.rowid, seq: r.seq, hlcL: r.hlc_l }));
  }

  chainAt(topic: Topic, writer: WriterId, seq: Seq): string | undefined {
    return this.db.get<{ chain: string }>(
      "SELECT chain FROM sq_log WHERE topic = ? AND writer = ? AND seq = ?",
      [topic, writer, seq],
    )?.chain;
  }

  hlcLAt(topic: Topic, writer: WriterId, seq: Seq): number | undefined {
    return this.db.get<{ hlc_l: number }>(
      "SELECT hlc_l FROM sq_log WHERE topic = ? AND writer = ? AND seq = ?",
      [topic, writer, seq],
    )?.hlc_l;
  }

  // Delete one stream's rows with seq ≤ uptoSeq — the caller has just walked
  // exactly that prefix inside the same transaction (so the DELETE is the
  // walked, bounded row set), plus the local judgments hanging off it.
  deleteStreamPrefix(topic: Topic, writer: WriterId, uptoSeq: Seq): void {
    this.db.run("DELETE FROM sq_log WHERE topic = ? AND writer = ? AND seq <= ?", [topic, writer, uptoSeq]);
    this.db.run("DELETE FROM sq_annotations WHERE topic = ? AND writer = ? AND seq <= ?", [
      topic,
      writer,
      uptoSeq,
    ]);
    this.logCounts.delete(topic); // recount lazily
  }

  // Out-of-order entries at or below an adopted floor can never drain.
  deletePendingUpTo(topic: Topic, writer: WriterId, uptoSeq: Seq): void {
    this.db.run("DELETE FROM sq_pending WHERE topic = ? AND writer = ? AND seq <= ?", [topic, writer, uptoSeq]);
  }

  checkpointPut(
    topic: Topic,
    view: string,
    version: string,
    ord: Order,
    state: string,
  ): void {
    this.db.run(
      `INSERT OR REPLACE INTO sq_checkpoints
         (topic, view, view_version, ord_l, ord_c, ord_w, ord_s, state)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [topic, view, version, ord.l, ord.c, ord.writer, ord.seq, state],
    );
  }

  // latest checkpoint strictly before `before` (for late-arrival recompute)
  checkpointBefore(
    topic: Topic,
    view: string,
    version: string,
    before: Order,
  ): { ord: Order; state: string } | undefined {
    const r = this.db.get<{
      ord_l: number;
      ord_c: number;
      ord_w: string;
      ord_s: number;
      state: string;
    }>(
      `SELECT ord_l, ord_c, ord_w, ord_s, state FROM sq_checkpoints
       WHERE topic = ? AND view = ? AND view_version = ? AND
         (ord_l < ? OR (ord_l = ? AND (ord_c < ? OR (ord_c = ? AND
           (ord_w < ? OR (ord_w = ? AND ord_s < ?))))))
       ORDER BY ord_l DESC, ord_c DESC, ord_w DESC, ord_s DESC LIMIT 1`,
      [topic, view, version, before.l, before.l, before.c, before.c,
       before.writer, before.writer, before.seq],
    );
    if (!r) return undefined;
    return { ord: { l: r.ord_l, c: r.ord_c, writer: r.ord_w, seq: r.ord_s }, state: r.state };
  }

  deleteCheckpoints(topic: Topic, view: string, version: string): void {
    this.db.run("DELETE FROM sq_checkpoints WHERE topic = ? AND view = ? AND view_version = ?", [
      topic,
      view,
      version,
    ]);
  }

  cursorsForTopic(topic: Topic): { consumer: string; lastRowid: number; updatedAt: string }[] {
    return this.db
      .all<{ consumer: string; last_rowid: number; updated_at: string }>(
        "SELECT consumer, last_rowid, updated_at FROM sq_cursors WHERE topic = ?",
        [topic],
      )
      .map((r) => ({ consumer: r.consumer, lastRowid: r.last_rowid, updatedAt: r.updated_at }));
  }

  cursorDelete(consumer: string, topic: Topic): void {
    this.db.run("DELETE FROM sq_cursors WHERE consumer = ? AND topic = ?", [consumer, topic]);
  }

  // §7.6 cold archiving: move canonical covered rows out of the hot log.
  // Bounded by maxRowid so rows a live consumer hasn't passed stay put.
  // Batched — a production FINALITY_WINDOW covers millions of rows (§20.8),
  // and the first archive pass must not materialize them all at once.
  archiveCovered(topic: Topic, writer: WriterId, maxSeq: Seq, maxRowid: number, at: string): number {
    const BATCH = 2_000;
    let total = 0;
    for (;;) {
      const moved = this.db.transaction(() => {
        const rows = this.db.all<RawLogRow>(
          `SELECT rowid, * FROM sq_log WHERE topic = ? AND writer = ? AND seq <= ? AND rowid <= ?
           ORDER BY seq LIMIT ?`,
          [topic, writer, maxSeq, maxRowid, BATCH],
        );
        for (const r of rows) {
          const { entry } = rowToEntry(r);
          this.db.run(
            "INSERT OR IGNORE INTO sq_archive (topic, writer, seq, entry, archived_at) VALUES (?, ?, ?, ?, ?)",
            [topic, writer, r.seq, JSON.stringify(entry), at],
          );
          this.db.run("DELETE FROM sq_log WHERE rowid = ?", [r.rowid]);
        }
        return rows.length;
      });
      total += moved;
      if (moved < BATCH) {
        if (total > 0) {
          this.logCounts.delete(topic); // recount lazily
          const a = this.archiveCounts.get(topic);
          if (a !== undefined) this.archiveCounts.set(topic, a + total);
        }
        return total;
      }
    }
  }

  archivedCount(topic: Topic): number {
    const cached = this.archiveCounts.get(topic);
    if (cached !== undefined) return cached;
    const n =
      this.db.get<{ n: number }>("SELECT COUNT(*) AS n FROM sq_archive WHERE topic = ?", [topic])
        ?.n ?? 0;
    this.archiveCounts.set(topic, n);
    return n;
  }

  archivedEntries(topic: Topic, writer: WriterId, fromSeq: Seq, toSeq: Seq): LogEntry[] {
    return this.db
      .all<{ entry: string }>(
        "SELECT entry FROM sq_archive WHERE topic=? AND writer=? AND seq>=? AND seq<=? ORDER BY seq",
        [topic, writer, fromSeq, toSeq],
      )
      .map((r) => JSON.parse(r.entry) as LogEntry);
  }

  deleteCheckpointsBefore(topic: Topic, view: string, version: string, before: Order): void {
    this.db.run(
      `DELETE FROM sq_checkpoints WHERE topic = ? AND view = ? AND view_version = ? AND
         (ord_l < ? OR (ord_l = ? AND (ord_c < ? OR (ord_c = ? AND
           (ord_w < ? OR (ord_w = ? AND ord_s < ?))))))`,
      [topic, view, version, before.l, before.l, before.c, before.c,
       before.writer, before.writer, before.seq],
    );
  }

  earliestCheckpoint(
    topic: Topic,
    view: string,
    version: string,
  ): { ord: Order; state: string } | undefined {
    const r = this.db.get<{
      ord_l: number;
      ord_c: number;
      ord_w: string;
      ord_s: number;
      state: string;
    }>(
      `SELECT ord_l, ord_c, ord_w, ord_s, state FROM sq_checkpoints
       WHERE topic = ? AND view = ? AND view_version = ?
       ORDER BY ord_l, ord_c, ord_w, ord_s LIMIT 1`,
      [topic, view, version],
    );
    if (!r) return undefined;
    return { ord: { l: r.ord_l, c: r.ord_c, writer: r.ord_w, seq: r.ord_s }, state: r.state };
  }

  logCount(topic: Topic): number {
    const cached = this.logCounts.get(topic);
    if (cached !== undefined) return cached;
    const n =
      this.db.get<{ n: number }>("SELECT COUNT(*) AS n FROM sq_log WHERE topic = ?", [topic])?.n ??
      0;
    this.logCounts.set(topic, n);
    return n;
  }

  pendingCountForTopic(topic: Topic): number {
    return (
      this.db.get<{ n: number }>("SELECT COUNT(*) AS n FROM sq_pending WHERE topic = ?", [topic])
        ?.n ?? 0
    );
  }

  quarantineCount(topic: Topic): number {
    return (
      this.db.get<{ n: number }>("SELECT COUNT(*) AS n FROM sq_quarantine WHERE topic = ?", [
        topic,
      ])?.n ?? 0
    );
  }

  minRowidForTopic(topic: Topic): number | null {
    return (
      this.db.get<{ m: number | null }>("SELECT MIN(rowid) AS m FROM sq_log WHERE topic = ?", [
        topic,
      ])?.m ?? null
    );
  }

  cursorGet(consumer: string, topic: Topic): number {
    const r = this.db.get<{ last_rowid: number }>(
      "SELECT last_rowid FROM sq_cursors WHERE consumer = ? AND topic = ?",
      [consumer, topic],
    );
    return r?.last_rowid ?? 0;
  }

  cursorSet(consumer: string, topic: Topic, lastRowid: number, at: string): void {
    this.db.run(
      "INSERT OR REPLACE INTO sq_cursors (consumer, topic, last_rowid, updated_at) VALUES (?, ?, ?, ?)",
      [consumer, topic, lastRowid, at],
    );
  }

  directivePut(topic: Topic, writer: WriterId, rgen: number, directive: string): void {
    this.db.run(
      "INSERT OR REPLACE INTO sq_directives (topic, writer, rgen, directive) VALUES (?, ?, ?, ?)",
      [topic, writer, rgen, directive],
    );
  }

  directiveLatest(topic: Topic, writer: WriterId): string | undefined {
    return this.db.get<{ directive: string }>(
      "SELECT directive FROM sq_directives WHERE topic=? AND writer=? ORDER BY rgen DESC LIMIT 1",
      [topic, writer],
    )?.directive;
  }

  directiveGet(topic: Topic, writer: WriterId, rgen: number): string | undefined {
    return this.db.get<{ directive: string }>(
      "SELECT directive FROM sq_directives WHERE topic=? AND writer=? AND rgen=?",
      [topic, writer, rgen],
    )?.directive;
  }

  directivesForTopic(topic: Topic): string[] {
    // the latest directive per writer (SnapshotBody carries signed originals, §7.7)
    return this.db
      .all<{ directive: string }>(
        `SELECT directive FROM sq_directives d WHERE rgen =
           (SELECT MAX(rgen) FROM sq_directives WHERE topic = d.topic AND writer = d.writer)
         AND topic = ?`,
        [topic],
      )
      .map((r) => r.directive);
  }

  finalityGet(topic: Topic): string | undefined {
    return this.db.get<{ cert: string }>("SELECT cert FROM sq_finality WHERE topic = ?", [topic])
      ?.cert;
  }

  finalitySet(topic: Topic, cert: string): void {
    this.db.run("INSERT OR REPLACE INTO sq_finality (topic, cert) VALUES (?, ?)", [topic, cert]);
  }

  // raw access for view tables (sqv_*) — everything else goes through typed accessors
  raw(): SqliteHandle {
    return this.db;
  }

  metaGet(k: string): string | undefined {
    return this.db.get<{ v: string }>("SELECT v FROM sq_meta WHERE k=?", [k])?.v;
  }

  metaSet(k: string, v: string): void {
    this.db.run("INSERT OR REPLACE INTO sq_meta (k, v) VALUES (?, ?)", [k, v]);
  }
}
