// SPEC §8 — DDL and typed accessors. All sq_log/sq_writers mutations flow
// through LogCore's commit queue; this module never opens its own transactions
// except in init().

import { SeqscribeError } from "./errors.js";
import type { JsonValue, LogEntry, Order, Seq, SqliteHandle, Topic, WriterId } from "./types.js";

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
        "SELECT rowid, * FROM sq_log WHERE topic = ? AND rowid > ? ORDER BY rowid LIMIT ?",
        [topic, afterRowid, limit],
      )
      .map(rowToEntry);
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
      if (moved < BATCH) return total;
    }
  }

  archivedCount(topic: Topic): number {
    return (
      this.db.get<{ n: number }>("SELECT COUNT(*) AS n FROM sq_archive WHERE topic = ?", [topic])
        ?.n ?? 0
    );
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
    return (
      this.db.get<{ n: number }>("SELECT COUNT(*) AS n FROM sq_log WHERE topic = ?", [topic])?.n ??
      0
    );
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
