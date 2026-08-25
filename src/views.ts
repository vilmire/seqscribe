// SPEC §9 — view materialization: pure reducers folded in total order,
// checkpoints with delta verification, late-arrival suffix recompute (restore
// checkpoint before p, refold, atomic swap, epoch bump), faulted-view state.

import { jcs, sha256HexUtf8, utf8ByteLength } from "./encoding.js";
import { misuse, SeqscribeError } from "./errors.js";
import { orderCompare, orderOf } from "./hlc.js";
import type { Store } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type {
  Anomaly,
  Constants,
  JsonValue,
  LogEntry,
  Order,
  Row,
  Timers,
  Topic,
  ViewDef,
  ViewHandle,
} from "./types.js";

const FOLD_BATCH = 500;
const IDENT_RE = /^[a-z_][a-z0-9_]*$/;

export interface ViewChange {
  view: string;
  epoch: string;
  upserts: Row[];
  deletes: string[];
  reset: boolean;
}

interface Instance {
  name: string;
  topic: Topic;
  def: ViewDef<JsonValue, Row>;
  table: string;
  rowKeyCol: string;
  columns: string[];
  state: JsonValue;
  ord: Order | null;
  lastRowid: number;
  sinceCheckpoint: number;
  epoch: string;
  faulted: boolean;
  bootstrapPartial: boolean;
  scheduled: boolean;
  materializing: boolean;
}

export interface ViewHubDeps {
  store: Store;
  topics: TopicRegistry;
  timers: Timers;
  constants: Constants;
  rng: () => number;
  emitAnomaly: (a: Anomaly) => void;
}

export class ViewHub {
  private readonly views = new Map<string, Instance>();
  private readonly byTopic = new Map<Topic, Instance[]>();
  private readonly changeListeners = new Set<(c: ViewChange) => void>();

  constructor(private readonly deps: ViewHubDeps) {}

  onViewChange(cb: (c: ViewChange) => void): () => void {
    this.changeListeners.add(cb);
    return () => this.changeListeners.delete(cb);
  }

  register(name: string, topic: Topic, def: ViewDef<JsonValue, Row>): ViewHandle {
    if (this.views.has(name)) throw misuse(`view already registered: ${name}`);
    const policy = this.deps.topics.get(topic).policy;
    if (policy.retention.mode !== "full")
      throw misuse(`custom views require retention "full" (${topic}) — ring topics expose only "tail" (§9)`);
    if (def.fts && def.fts.length > 0) throw misuse("fts views are not implemented yet");
    for (const col of Object.keys(def.schema)) {
      if (!IDENT_RE.test(col)) throw misuse(`bad view column identifier: ${col}`);
    }
    if (!IDENT_RE.test(def.rowKey) || !(def.rowKey in def.schema))
      throw misuse(`rowKey must be a schema column: ${def.rowKey}`);

    const table = `sqv_${name.replace(/[^a-z0-9_]/g, "_")}_${sha256HexUtf8(name).slice(0, 8)}`;
    const inst: Instance = {
      name,
      topic,
      def,
      table,
      rowKeyCol: def.rowKey,
      columns: Object.keys(def.schema),
      state: def.init,
      ord: null,
      lastRowid: 0,
      sinceCheckpoint: 0,
      epoch: this.mintEpoch(),
      faulted: false,
      bootstrapPartial: false,
      scheduled: false,
      materializing: false,
    };

    const versionKey = `view_version:${name}`;
    const storedVersion = this.deps.store.metaGet(versionKey);
    if (storedVersion !== undefined && storedVersion !== def.version) {
      this.deps.store.raw().run(`DROP TABLE IF EXISTS "${table}"`);
      this.deps.store.deleteCheckpoints(topic, name, storedVersion);
    }
    this.deps.store.metaSet(versionKey, def.version);
    const colDefs = inst.columns
      .map((c) => `"${c}" ${def.schema[c]}${c === inst.rowKeyCol ? " PRIMARY KEY" : ""}`)
      .join(", ");
    this.deps.store.raw().run(`CREATE TABLE IF NOT EXISTS "${table}" (${colDefs})`);

    this.views.set(name, inst);
    const list = this.byTopic.get(topic) ?? [];
    list.push(inst);
    this.byTopic.set(topic, list);

    this.restore(inst);
    this.schedule(inst);

    return {
      name,
      version: def.version,
      rebuild: () => this.rebuild(name),
      query: <T = Row>(sql: string, params?: unknown[]): T[] => {
        if (inst.faulted)
          throw new SeqscribeError("ERR_STORAGE", `view ${name} is faulted — rebuild required`);
        return this.deps.store.raw().all<T>(sql, params);
      },
    };
  }

  get(name: string): {
    table: string;
    epoch: string;
    version: string;
    topic: Topic;
    rowKeyCol: string;
    faulted: boolean;
  } {
    const inst = this.views.get(name);
    if (!inst) throw new SeqscribeError("ERR_UNKNOWN_VIEW", name);
    return {
      table: inst.table,
      epoch: inst.epoch,
      version: inst.def.version,
      topic: inst.topic,
      rowKeyCol: inst.rowKeyCol,
      faulted: inst.faulted,
    };
  }

  listForTopic(topic: Topic): { name: string; version: string }[] {
    return (this.byTopic.get(topic) ?? []).map((i) => ({ name: i.name, version: i.def.version }));
  }

  // view state exactly at `ord` (SnapshotBody build, §7.7): nearest checkpoint
  // at-or-before, then fold the remainder up to and including ord
  stateAt(name: string, ord: Order): { state: JsonValue; stateHash: string } {
    const inst = this.views.get(name);
    if (!inst) throw new SeqscribeError("ERR_UNKNOWN_VIEW", name);
    // ord "+1 seq" is the tightest strict upper bound over integer seqs
    const bound: Order = { l: ord.l, c: ord.c, writer: ord.writer, seq: ord.seq + 1 };
    const ckpt = this.deps.store.checkpointBefore(inst.topic, inst.name, inst.def.version, bound);
    let state: JsonValue = ckpt ? (JSON.parse(ckpt.state) as JsonValue) : inst.def.init;
    let cursor: Order | null = ckpt ? ckpt.ord : null;
    outer: for (;;) {
      const entries = this.deps.store.entriesAfterOrder(inst.topic, cursor, FOLD_BATCH);
      if (entries.length === 0) break;
      for (const e of entries) {
        const o = orderOf(e);
        if (orderCompare(o, ord) > 0) break outer;
        state = inst.def.reduce(state, e);
        cursor = o;
      }
    }
    const canonical = jcs(state);
    return { state: JSON.parse(canonical) as JsonValue, stateHash: sha256HexUtf8(canonical) };
  }

  // bootstrap adoption (§7.8): install the snapshot's at-cut state as this
  // view's base — recorded as a checkpoint so recomputes restore from it
  installState(name: string, state: JsonValue | undefined, ord: Order): void {
    const inst = this.views.get(name);
    if (!inst) throw new SeqscribeError("ERR_UNKNOWN_VIEW", name);
    inst.state = state === undefined ? inst.def.init : state; // absent → init at the cut (§7.8)
    inst.ord = ord;
    inst.sinceCheckpoint = 0;
    inst.faulted = false;
    inst.bootstrapPartial = false;
    this.deps.store.checkpointPut(inst.topic, inst.name, inst.def.version, ord, jcs(inst.state));
    inst.lastRowid = this.deps.store.maxRowid(inst.topic);
    this.deps.store.transaction(() => this.rewriteTable(inst));
    inst.epoch = this.mintEpoch();
    this.emitChange(inst, { upserts: [], deletes: [], reset: true });
  }

  markBootstrapPartial(name: string): void {
    const inst = this.views.get(name);
    if (inst) inst.bootstrapPartial = true;
  }

  isBootstrapPartial(name: string): boolean {
    return this.views.get(name)?.bootstrapPartial ?? false;
  }

  has(name: string): boolean {
    return this.views.has(name);
  }

  tableRowsSorted(name: string): Row[] {
    const inst = this.views.get(name);
    if (!inst) throw new SeqscribeError("ERR_UNKNOWN_VIEW", name);
    if (inst.faulted) throw new SeqscribeError("ERR_STORAGE", `view ${name} is faulted`);
    return this.tableRows(inst);
  }

  notifyApplied(topic: Topic): void {
    for (const inst of this.byTopic.get(topic) ?? []) this.schedule(inst);
  }

  async rebuild(name: string): Promise<void> {
    const inst = this.views.get(name);
    if (!inst) throw new SeqscribeError("ERR_UNKNOWN_VIEW", name);
    this.deps.store.deleteCheckpoints(inst.topic, inst.name, inst.def.version);
    inst.state = inst.def.init;
    inst.ord = null;
    inst.lastRowid = 0;
    inst.sinceCheckpoint = 0;
    inst.faulted = false;
    this.refoldAll(inst);
  }

  async settle(): Promise<void> {
    // test helper: force pending materializations now
    for (const inst of this.views.values()) {
      if (inst.scheduled) {
        inst.scheduled = false;
        this.materialize(inst);
      }
    }
  }

  private mintEpoch(): string {
    let s = "";
    for (let i = 0; i < 4; i++)
      s += Math.floor(this.deps.rng() * 0x10000)
        .toString(16)
        .padStart(4, "0");
    return s;
  }

  private schedule(inst: Instance): void {
    if (inst.scheduled || inst.faulted) return;
    inst.scheduled = true;
    this.deps.timers.setTimeout(() => {
      inst.scheduled = false;
      this.materialize(inst);
    }, 0);
  }

  private restore(inst: Instance): void {
    // resume from the latest checkpoint, then refold the tail
    const top: Order = { l: Number.MAX_SAFE_INTEGER, c: 0, writer: "", seq: 0 };
    const ckpt = this.deps.store.checkpointBefore(inst.topic, inst.name, inst.def.version, top);
    if (ckpt) {
      inst.state = JSON.parse(ckpt.state) as JsonValue;
      inst.ord = ckpt.ord;
    }
    this.refoldFrom(inst, inst.ord);
    inst.lastRowid = this.deps.store.maxRowid(inst.topic);
  }

  private materialize(inst: Instance): void {
    if (inst.faulted || inst.materializing) return;
    inst.materializing = true;
    try {
      for (;;) {
        const rows = this.deps.store.entriesForTopicFromRowid(
          inst.topic,
          inst.lastRowid,
          FOLD_BATCH,
        );
        if (rows.length === 0) return;
        const batch = rows.map((r) => r.entry).sort((a, b) => orderCompare(orderOf(a), orderOf(b)));
        const first = batch[0];
        if (!first) return;
        const lastRowid = rows[rows.length - 1]!.rowid;

        if (inst.ord !== null && orderCompare(orderOf(first), inst.ord) <= 0) {
          // late arrival below the watermark: restore the checkpoint before its
          // position and recompute the suffix (§9); epoch bumps, table swaps.
          inst.lastRowid = lastRowid;
          this.recomputeFrom(inst, orderOf(first));
          continue;
        }

        const changes: { upserts: Row[]; deletes: string[] } = { upserts: [], deletes: [] };
        this.deps.store.transaction(() => {
          for (const e of batch) {
            const pre = inst.state;
            if (inst.def.delta) {
              const d = inst.def.delta(pre, e);
              this.applyDelta(inst, d);
              changes.upserts.push(...d.upserts);
              changes.deletes.push(...d.deletes);
            }
            inst.state = inst.def.reduce(pre, e);
            inst.ord = orderOf(e);
            inst.sinceCheckpoint++;
            if (inst.sinceCheckpoint >= this.deps.constants.CHECKPOINT_EVERY)
              this.checkpoint(inst);
          }
          if (!inst.def.delta) this.rewriteTable(inst);
          inst.lastRowid = lastRowid;
        });
        if (inst.def.delta && (changes.upserts.length > 0 || changes.deletes.length > 0))
          this.emitChange(inst, { ...changes, reset: false });
        if (!inst.def.delta) this.emitChange(inst, { upserts: [], deletes: [], reset: true });
      }
    } catch (e) {
      this.fault(inst, e);
    } finally {
      inst.materializing = false;
    }
  }

  private recomputeFrom(inst: Instance, lateOrd: Order): void {
    const ckpt = this.deps.store.checkpointBefore(inst.topic, inst.name, inst.def.version, lateOrd);
    if (ckpt) {
      inst.state = JSON.parse(ckpt.state) as JsonValue;
      inst.ord = ckpt.ord;
    } else {
      inst.state = inst.def.init;
      inst.ord = null;
    }
    // checkpoints past the restore point are stale for the new fold
    this.deps.store.deleteCheckpoints(inst.topic, inst.name, inst.def.version);
    if (ckpt)
      this.deps.store.checkpointPut(
        inst.topic,
        inst.name,
        inst.def.version,
        ckpt.ord,
        ckpt.state,
      );
    inst.sinceCheckpoint = 0;
    this.refoldFrom(inst, inst.ord);
  }

  private refoldAll(inst: Instance): void {
    this.refoldFrom(inst, null);
    inst.lastRowid = this.deps.store.maxRowid(inst.topic);
  }

  private refoldFrom(inst: Instance, from: Order | null): void {
    try {
      let cursor = from;
      for (;;) {
        const entries = this.deps.store.entriesAfterOrder(inst.topic, cursor, FOLD_BATCH);
        if (entries.length === 0) break;
        for (const e of entries) {
          inst.state = inst.def.reduce(inst.state, e);
          inst.ord = orderOf(e);
          inst.sinceCheckpoint++;
          if (inst.sinceCheckpoint >= this.deps.constants.CHECKPOINT_EVERY) this.checkpoint(inst);
        }
        cursor = inst.ord;
      }
      this.deps.store.transaction(() => this.rewriteTable(inst));
      inst.epoch = this.mintEpoch();
      this.emitChange(inst, { upserts: [], deletes: [], reset: true });
    } catch (e) {
      this.fault(inst, e);
    }
  }

  private checkpoint(inst: Instance): void {
    if (inst.ord === null) return;
    inst.sinceCheckpoint = 0;
    this.deps.store.checkpointPut(
      inst.topic,
      inst.name,
      inst.def.version,
      inst.ord,
      jcs(inst.state),
    );
    if (inst.def.delta) this.verifyDelta(inst);
  }

  // rows() is authoritative; delta is an optimization verified at checkpoints (§9)
  private verifyDelta(inst: Instance): void {
    const expected = sha256HexUtf8(jcs(this.sortedRows(inst) as unknown as JsonValue));
    const actual = sha256HexUtf8(jcs(this.tableRows(inst) as unknown as JsonValue));
    if (expected !== actual) {
      this.deps.emitAnomaly({ kind: "delta_mismatch" });
      this.rewriteTable(inst);
      inst.epoch = this.mintEpoch();
      this.emitChange(inst, { upserts: [], deletes: [], reset: true });
    }
  }

  private sortedRows(inst: Instance): Row[] {
    const rows = [...inst.def.rows(inst.state)];
    for (const r of rows) this.validateRow(inst, r);
    return rows.sort((a, b) =>
      String(a[inst.rowKeyCol]) < String(b[inst.rowKeyCol]) ? -1 : 1,
    );
  }

  private tableRows(inst: Instance): Row[] {
    return this.deps.store
      .raw()
      .all<Row>(`SELECT * FROM "${inst.table}" ORDER BY "${inst.rowKeyCol}"`);
  }

  private rewriteTable(inst: Instance): void {
    const rows = this.sortedRows(inst);
    this.deps.store.raw().run(`DELETE FROM "${inst.table}"`);
    for (const r of rows) this.insertRow(inst, r);
  }

  private applyDelta(inst: Instance, d: { upserts: Row[]; deletes: string[] }): void {
    for (const key of d.deletes)
      this.deps.store
        .raw()
        .run(`DELETE FROM "${inst.table}" WHERE "${inst.rowKeyCol}" = ?`, [key]);
    for (const r of d.upserts) {
      this.validateRow(inst, r);
      this.insertRow(inst, r);
    }
  }

  private insertRow(inst: Instance, r: Row): void {
    const cols = inst.columns;
    const sql = `INSERT OR REPLACE INTO "${inst.table}" (${cols.map((c) => `"${c}"`).join(", ")})
                 VALUES (${cols.map(() => "?").join(", ")})`;
    this.deps.store.raw().run(
      sql,
      cols.map((c) => (c === inst.rowKeyCol ? String(r[c] ?? "") : (r[c] ?? null))),
    );
  }

  private validateRow(inst: Instance, r: Row): void {
    for (const [k, v] of Object.entries(r)) {
      if (typeof v === "number" && !Number.isFinite(v))
        throw new SeqscribeError("ERR_ENTRY_ENCODING", `NaN/Infinity in view row (${inst.name}.${k})`);
    }
    const bytes = utf8ByteLength(jcs(r as unknown as JsonValue));
    if (bytes > this.deps.constants.MAX_ROW_BYTES)
      throw new SeqscribeError("ERR_ENTRY_TOO_LARGE", `view row ${bytes}B > MAX_ROW_BYTES`);
  }

  private fault(inst: Instance, err: unknown): void {
    inst.faulted = true;
    this.deps.emitAnomaly({ kind: "view_faulted" });
    void err;
  }

  private emitChange(inst: Instance, c: { upserts: Row[]; deletes: string[]; reset: boolean }): void {
    for (const cb of this.changeListeners)
      cb({ view: inst.name, epoch: inst.epoch, ...c });
  }
}
