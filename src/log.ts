// SPEC §6.1–6.2, §8 write path — the single append queue with group commit.
// Everything that mutates sq_log/sq_writers flows through this queue; external
// (wire/import) application and, at later milestones, cert/directive effects
// are serialized through it too.

import { validateEntry } from "./codec.js";
import { assertJsonValue, chainOf, seedOf } from "./encoding.js";
import { SeqscribeError } from "./errors.js";
import { hlcCompare, isOverEpsilon, merge, orderCompare, orderOf, stamp, type HlcState } from "./hlc.js";
import type { Store, WriterRow } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type {
  Anomaly,
  Constants,
  EntryId,
  FinalityCert,
  HaveVectors,
  JsonValue,
  LogEntry,
  Seq,
  Timers,
  Topic,
  WriterDirective,
  WriterId,
} from "./types.js";

export type ApplyResult =
  | "applied"
  | "pending"
  | "duplicate"
  | "dropped_overflow"
  | "forked"
  | "sealed"
  | "rejected_finality";

interface AppendItem {
  t: "append";
  topic: Topic;
  kind: string;
  payload: JsonValue;
  key?: string | undefined;
  causal?: [WriterId, Seq] | undefined;
  // §11.2: causal is stamped at commit time, when the seq is assigned — the
  // provider sees the author's own uncommitted tail through serial processing
  causalProvider?: ((seq: Seq) => [WriterId, Seq] | undefined) | undefined;
  ref?: EntryId | undefined;
  resolve: (id: EntryId) => void;
  reject: (e: unknown) => void;
}

interface ExternalItem {
  t: "external";
  entry: LogEntry;
  via?: string | undefined; // peerId the entry arrived from (relay exclusion)
  resolve: (r: ApplyResult) => void;
  reject: (e: unknown) => void;
}

interface SealItem {
  t: "seal";
  topic: Topic;
  writer: WriterId;
  resolve: () => void;
  reject: (e: unknown) => void;
}

interface CertItem {
  t: "cert";
  cert: FinalityCert;
  resolve: (quarantined: number) => void;
  reject: (e: unknown) => void;
}

interface DirectiveItem {
  t: "directive";
  directive: WriterDirective;
  resolve: (r: "applied" | "stale" | "refused" | "recovery") => void;
  reject: (e: unknown) => void;
}

interface AdoptCutItem {
  t: "adopt";
  topic: Topic;
  cut: Record<WriterId, { seq: Seq; chain: string }>;
  resolve: () => void;
  reject: (e: unknown) => void;
}

type QueueItem = AppendItem | ExternalItem | SealItem | CertItem | DirectiveItem | AdoptCutItem;

export interface RecoveryTarget {
  finalSeq: Seq;
  finalChain: string;
  rgen: number;
  unavailableReported: boolean;
}

export interface LogCoreOpts {
  store: Store;
  topics: TopicRegistry;
  writerId: WriterId;
  clock: () => number;
  timers: Timers;
  constants: Constants;
  emitAnomaly: (a: Anomaly) => void;
}

export type AppliedHook = (e: LogEntry, rowid: number | null, via: string | undefined) => void;

const HLC_META_KEY = "hlc_state";

export class LogCore {
  private readonly store: Store;
  private readonly topics: TopicRegistry;
  private readonly writerId: WriterId;
  private readonly clock: () => number;
  private readonly timers: Timers;
  private readonly constants: Constants;
  private readonly emitAnomaly: (a: Anomaly) => void;
  private onApplied: AppliedHook | undefined;

  private hlcState: HlcState;
  private readonly heads = new Map<string, WriterRow>();
  private readonly certs = new Map<Topic, FinalityCert | null>();
  // incrementally maintained HAVE vectors — a fleet at the envelope has
  // thousands of topics, and rebuilding O(total streams) per HAVE_GET per peer
  // is the scan this cache removes. Expired tombstones may linger in the cache
  // until the next head write (TOMBSTONE_RETAIN_MS is a soft exposure bound).
  private vectorsCache: HaveVectors | null = null;
  private readonly recoveries = new Map<string, RecoveryTarget>();
  private readonly rings = new Map<Topic, LogEntry[]>();
  private queue: QueueItem[] = [];
  private flushTimer: unknown = null;
  private closed = false;
  // §8 rollback hygiene: side state that other hubs advance inside the commit
  // transaction (e.g. the register hub's causal scope tails, §11.2) registers
  // a guard here — snapshot() captures pre-txn state and returns the restore
  // to run when the transaction aborts.
  private readonly txnGuards: { snapshot(): () => void }[] = [];
  // per-flush copy-on-write journal for ring tails touched by persist() —
  // ring pushes are in-memory only (§14) and would otherwise survive an abort
  private ringUndo: Map<Topic, LogEntry[] | undefined> | null = null;

  constructor(opts: LogCoreOpts) {
    this.store = opts.store;
    this.topics = opts.topics;
    this.writerId = opts.writerId;
    this.clock = opts.clock;
    this.timers = opts.timers;
    this.constants = opts.constants;
    this.emitAnomaly = opts.emitAnomaly;
    const saved = this.store.metaGet(HLC_META_KEY);
    // Restoring the persisted HLC state keeps own-stream HLC monotonicity across
    // restarts even when the wall clock rewound (§1 per-writer monotonicity).
    this.hlcState = saved ? (JSON.parse(saved) as HlcState) : { l: 0, c: 0 };
  }

  append(
    topic: Topic,
    kind: string,
    payload: JsonValue,
    o?: {
      key?: string;
      causal?: [WriterId, Seq];
      causalProvider?: (seq: Seq) => [WriterId, Seq] | undefined;
      ref?: EntryId;
    },
  ): Promise<EntryId> {
    if (this.closed) throw new SeqscribeError("ERR_MISUSE", "node is closed");
    this.topics.get(topic); // ERR_UNKNOWN_TOPIC before enqueue
    assertJsonValue(payload);
    return new Promise<EntryId>((resolve, reject) => {
      this.push({
        t: "append",
        topic,
        kind,
        payload,
        key: o?.key,
        causal: o?.causal,
        causalProvider: o?.causalProvider,
        ref: o?.ref,
        resolve,
        reject,
      });
    });
  }

  applyExternal(entry: LogEntry, via?: string): Promise<ApplyResult> {
    if (this.closed) throw new SeqscribeError("ERR_MISUSE", "node is closed");
    validateEntry(entry, this.constants);
    this.topics.get(entry.topic);
    return new Promise<ApplyResult>((resolve, reject) => {
      this.push({ t: "external", entry, via, resolve, reject });
    });
  }

  // §6.4 fork path ② (HAVE contradiction) is detected outside the queue but the
  // seal itself is serialized through it like every sq_writers mutation.
  sealStream(topic: Topic, writer: WriterId): Promise<void> {
    if (this.closed) throw new SeqscribeError("ERR_MISUSE", "node is closed");
    return new Promise<void>((resolve, reject) => {
      this.push({ t: "seal", topic, writer, resolve, reject });
    });
  }

  setOnApplied(hook: AppliedHook | undefined): void {
    this.onApplied = hook;
  }

  // Internal wiring (not public API): hubs whose in-memory state is mutated
  // inside the commit transaction register here so a failed batch restores it
  // together with heads/certs/vectors (§8 rollback hygiene).
  addTxnGuard(guard: { snapshot(): () => void }): void {
    this.txnGuards.push(guard);
  }

  // Certificate application is serialized through the append queue (§8):
  // enqueued-but-uncommitted appends on a stream the cert quarantines are
  // rejected, never committed onto a quarantined chain.
  applyCert(cert: FinalityCert): Promise<number> {
    if (this.closed) throw new SeqscribeError("ERR_MISUSE", "node is closed");
    return new Promise<number>((resolve, reject) => {
      this.push({ t: "cert", cert, resolve, reject });
    });
  }

  applyDirective(directive: WriterDirective): Promise<"applied" | "stale" | "refused" | "recovery"> {
    if (this.closed) throw new SeqscribeError("ERR_MISUSE", "node is closed");
    return new Promise((resolve, reject) => {
      this.push({ t: "directive", directive, resolve, reject });
    });
  }

  // §7.8 snapshot adoption: stream heads jump to the cut (chains continue from
  // cut chains) — the pre-cut log is replaced by the snapshot, not replayed.
  adoptSnapshotCut(topic: Topic, cut: Record<WriterId, { seq: Seq; chain: string }>): Promise<void> {
    if (this.closed) throw new SeqscribeError("ERR_MISUSE", "node is closed");
    return new Promise((resolve, reject) => {
      this.push({ t: "adopt", topic, cut, resolve, reject });
    });
  }

  recoveryTarget(topic: Topic, writer: WriterId): RecoveryTarget | undefined {
    return this.recoveries.get(`${topic} ${writer}`);
  }

  reportCanonicalUnavailable(topic: Topic, writer: WriterId): void {
    const t = this.recoveries.get(`${topic} ${writer}`);
    if (t && !t.unavailableReported) {
      t.unavailableReported = true;
      this.emitAnomaly({ kind: "canonical_unavailable" });
    }
  }

  getCert(topic: Topic): FinalityCert | null {
    let c = this.certs.get(topic);
    if (c === undefined) {
      const raw = this.store.finalityGet(topic);
      c = raw ? (JSON.parse(raw) as FinalityCert) : null;
      this.certs.set(topic, c);
    }
    return c;
  }

  getStream(topic: Topic, writer: WriterId): WriterRow {
    const k = `${topic}\u0000${writer}`;
    let head = this.heads.get(k);
    if (!head) {
      head = this.store.getWriter(topic, writer) ?? {
        topic,
        writer,
        contigSeq: 0,
        contigChain: seedOf(topic, writer),
        sealReason: null,
        rgen: 0,
        retiredAt: null,
        finalSeq: null,
        finalChain: null,
      };
      this.heads.set(k, head);
    }
    return head;
  }

  // Persist a stream head and keep the HAVE vector cache in step.
  private saveHead(head: WriterRow): void {
    this.store.upsertWriter(head);
    if (this.vectorsCache) {
      const topicVec = (this.vectorsCache[head.topic] ??= { writers: {} });
      this.applyHeadToVec(topicVec, head, this.clock() - this.constants.TOMBSTONE_RETAIN_MS);
    }
  }

  private applyHeadToVec(
    topicVec: HaveVectors[Topic],
    w: WriterRow,
    tombstoneFloor: number,
  ): void {
    if (w.sealReason === "retired" && w.finalSeq !== null && w.finalChain !== null) {
      // TOMBSTONE_RETAIN_MS bounds HAVE exposure only; sq_writers rows are
      // permanent — no resurrection at day 401 (§8)
      if (w.retiredAt !== null && Date.parse(w.retiredAt) < tombstoneFloor) {
        delete topicVec.writers[w.writer];
        return;
      }
      topicVec.writers[w.writer] = {
        retired: true,
        finalSeq: w.finalSeq,
        finalChain: w.finalChain,
        rgen: w.rgen,
      };
      return;
    }
    const live: { contig: Seq; chain: string; rgen?: number } = {
      contig: w.contigSeq,
      chain: w.contigChain,
    };
    if (w.rgen > 0) live.rgen = w.rgen;
    topicVec.writers[w.writer] = live;
  }

  // NOTE: the returned object is the live cache — callers read, never mutate.
  vectors(): HaveVectors {
    if (this.vectorsCache) return this.vectorsCache;
    const out: HaveVectors = {};
    const tombstoneFloor = this.clock() - this.constants.TOMBSTONE_RETAIN_MS;
    for (const w of this.store.listWriters()) {
      const topicVec = (out[w.topic] ??= { writers: {} });
      this.applyHeadToVec(topicVec, w, tombstoneFloor);
    }
    // every defined full-sync topic appears even with no writers — the fgen
    // repair path (§7.4) needs the row to compare against, and a fresh replica
    // must advertise its (empty) state to receive the first FINALITY push
    for (const topic of this.topics.list()) {
      if (this.topics.get(topic).policy.replication !== "full-sync") continue;
      const topicVec = (out[topic] ??= { writers: {} });
      const cert = this.getCert(topic);
      if (cert) topicVec.fgen = cert.generation;
    }
    this.vectorsCache = out;
    return out;
  }

  ringTail(topic: Topic): LogEntry[] {
    return [...(this.rings.get(topic) ?? [])];
  }

  entries(topic: Topic, writer: WriterId, fromSeq: Seq, toSeq: Seq): LogEntry[] {
    return this.store.entriesRange(topic, writer, fromSeq, toSeq).map((r) => r.entry);
  }

  async flushNow(): Promise<void> {
    if (this.flushTimer !== null) {
      this.timers.clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.flush();
    await Promise.resolve();
  }

  async close(): Promise<void> {
    await this.flushNow();
    this.closed = true;
    // settle callbacks resolved during the final flush can race close and
    // enqueue more work on their microtasks — cancel the straggler flush and
    // reject the items: a closed core must neither commit nor leave callers
    // hanging (§14 close quiescence).
    if (this.flushTimer !== null) {
      this.timers.clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.rejectQueueClosed();
  }

  private rejectQueueClosed(): void {
    if (this.queue.length === 0) return;
    const batch = this.queue;
    this.queue = [];
    const e = new SeqscribeError("ERR_MISUSE", "node is closed");
    for (const item of batch) item.reject(e);
  }

  private push(item: QueueItem): void {
    this.queue.push(item);
    if (this.queue.length >= this.constants.GROUP_COMMIT_N) {
      if (this.flushTimer !== null) this.timers.clearTimeout(this.flushTimer);
      this.flushTimer = this.timers.setTimeout(() => this.flush(), 0);
    } else if (this.flushTimer === null) {
      this.flushTimer = this.timers.setTimeout(() => this.flush(), this.constants.GROUP_COMMIT_MS);
    }
  }

  private flush(): void {
    this.flushTimer = null;
    if (this.closed) {
      // §14 close quiescence: a straggler flush after close must not touch
      // the store — reject rather than commit or hang.
      this.rejectQueueClosed();
      return;
    }
    const batch = this.queue;
    if (batch.length === 0) return;
    this.queue = [];

    const settle: (() => void)[] = [];
    const applied: { entry: LogEntry; rowid: number | null; via: string | undefined }[] = [];
    const anomalies: Anomaly[] = [];

    // §8 rollback hygiene: heads/certs/vectors reload lazily from the store,
    // but the HLC, recovery targets, ring tails and hub side state (register
    // scope tails, §11.2) have no durable source of truth to reload from —
    // snapshot them up front so an aborted commit can restore them. Snapshots
    // (not deferral) because later items in the same batch must observe
    // earlier items' side state (e.g. intra-batch causal chaining, recovery
    // targets set by a directive and read by a following external ingest).
    const hlcBefore = this.hlcState;
    const recoveriesBefore = new Map(this.recoveries);
    const guardRestores = this.txnGuards.map((g) => g.snapshot());
    this.ringUndo = new Map();

    try {
      this.store.transaction(() => {
        for (const item of batch) {
          if (item.t === "append") this.processAppend(item, settle, applied);
          else if (item.t === "external") this.processExternal(item, settle, applied, anomalies);
          else if (item.t === "seal") this.processSeal(item, settle, anomalies);
          else if (item.t === "cert") this.processCert(item, settle, anomalies);
          else if (item.t === "directive") this.processDirective(item, settle, anomalies);
          else this.processAdopt(item, settle);
        }
        this.store.metaSet(HLC_META_KEY, JSON.stringify(this.hlcState));
      });
    } catch (err) {
      // A failed transaction rolls everything back — reject the whole batch,
      // invalidate the store-backed caches, and restore every piece of side
      // state that advanced inside the aborted txn (otherwise e.g. a register
      // scope tail names a seq that never committed and the next write stamps
      // a self-referential causal edge, §11.2).
      this.heads.clear();
      this.certs.clear();
      this.vectorsCache = null;
      this.hlcState = hlcBefore;
      this.recoveries.clear();
      for (const [k, v] of recoveriesBefore) this.recoveries.set(k, v);
      for (const [topic, prev] of this.ringUndo) {
        if (prev === undefined) this.rings.delete(topic);
        else this.rings.set(topic, prev);
      }
      this.ringUndo = null;
      for (const restore of guardRestores) restore();
      const e =
        err instanceof SeqscribeError
          ? err
          : new SeqscribeError("ERR_STORAGE", err instanceof Error ? err.message : String(err));
      for (const item of batch) item.reject(e);
      return;
    }
    this.ringUndo = null;

    for (const fn of settle) fn();
    for (const a of anomalies) this.emitAnomaly(a);
    if (this.onApplied)
      for (const { entry, rowid, via } of applied) this.onApplied(entry, rowid, via);
  }

  private processSeal(item: SealItem, settle: (() => void)[], anomalies: Anomaly[]): void {
    const head = this.getStream(item.topic, item.writer);
    if (head.sealReason === null) {
      head.sealReason = "fork";
      this.saveHead(head);
      anomalies.push({ kind: "writer_forked" });
    }
    settle.push(() => item.resolve());
  }

  // §7.5 on accepting a higher cert: (a) post-cut quarantine with the fully
  // defined transition (contig rewind, permanent seq reservation, 'fork'-
  // equivalent seal); (b) covered-region chain verification.
  private processCert(item: CertItem, settle: (() => void)[], anomalies: Anomaly[]): void {
    const cert = item.cert;
    const now = new Date(this.clock()).toISOString();
    let quarantined = 0;
    this.store.finalitySet(cert.topic, JSON.stringify(cert));
    this.certs.set(cert.topic, cert);
    if (this.vectorsCache) (this.vectorsCache[cert.topic] ??= { writers: {} }).fgen = cert.generation;

    for (const w of this.store.listWriters(cert.topic)) {
      const head = this.getStream(cert.topic, w.writer);
      const cut = cert.cut[w.writer]?.seq ?? 0;
      const lastCovered = this.store.lastSeqAtOrBeforeOrder(cert.topic, w.writer, cert.order);

      if (lastCovered > cut) {
        // applied entries with order ≤ P beyond the cut: the non-canonical late
        // region, plus the unverifiable suffix chaining from it
        const doomed = this.store.entriesRange(cert.topic, w.writer, cut + 1, head.contigSeq);
        const cutChain =
          cut === 0
            ? seedOf(cert.topic, w.writer)
            : (this.store.getEntry(cert.topic, w.writer, cut)?.chain ??
              cert.cut[w.writer]?.chain ??
              "");
        for (const { entry } of doomed) {
          this.store.quarantinePut(entry, "post_cut", now);
          anomalies.push({ kind: "entry_quarantined", entry });
          quarantined++;
        }
        this.store.deleteLogRange(cert.topic, w.writer, cut + 1);
        head.contigSeq = cut;
        head.contigChain = cutChain;
        if (head.sealReason === null) {
          head.sealReason = "fork";
          anomalies.push({ kind: "writer_forked" });
        }
        this.saveHead(head);
        continue;
      }

      if (cut > 0 && head.contigSeq >= cut) {
        const mine = this.store.getEntry(cert.topic, w.writer, cut)?.chain;
        const certChain = cert.cut[w.writer]?.chain;
        if (mine !== undefined && certChain !== undefined && mine !== certChain) {
          // local prefix diverged inside the covered region — §12 recovery
          // (canonical re-fetch) lands with directives; the seal is immediate
          if (head.sealReason === null) {
            head.sealReason = "fork";
            this.saveHead(head);
            anomalies.push({ kind: "writer_forked" });
          }
        }
      }
      // contig < cut: "verified, basis pending" — nothing to do
    }
    settle.push(() => item.resolve(quarantined));
  }

  // §13 directive application + §12 recovery entry. Highest rgen wins; equal
  // rgen with different content refuses both pending host intervention.
  private processDirective(
    item: DirectiveItem,
    settle: (() => void)[],
    anomalies: Anomaly[],
  ): void {
    const d = item.directive;
    const head = this.getStream(d.topic, d.writer);
    const key = `${d.topic} ${d.writer}`;
    const done = (r: "applied" | "stale" | "refused" | "recovery") =>
      settle.push(() => item.resolve(r));

    const stored = this.store.directiveGet(d.topic, d.writer, d.rgen);
    if (stored !== undefined && stored !== JSON.stringify(d)) {
      anomalies.push({ kind: "bad_directive" });
      done("refused");
      return;
    }
    if (d.rgen <= head.rgen && head.sealReason !== "fork") {
      this.store.directivePut(d.topic, d.writer, d.rgen, JSON.stringify(d));
      done("stale");
      return;
    }
    this.store.directivePut(d.topic, d.writer, d.rgen, JSON.stringify(d));

    if (d.state === "live") {
      if (head.sealReason === "fork") {
        // unretire cannot resolve an unresolved fork (§13)
        anomalies.push({ kind: "bad_directive" });
        done("refused");
        return;
      }
      head.sealReason = null;
      head.rgen = d.rgen;
      head.retiredAt = null;
      head.finalSeq = null;
      head.finalChain = null;
      this.saveHead(head);
      this.recoveries.delete(key);
      done("applied");
      return;
    }

    const finalSeq = d.finalSeq ?? 0;
    const finalChain = d.finalChain ?? seedOf(d.topic, d.writer);
    const localChainAtFinal =
      finalSeq === 0
        ? seedOf(d.topic, d.writer)
        : finalSeq === head.contigSeq
          ? head.contigChain
          : this.store.getEntry(d.topic, d.writer, finalSeq)?.chain;

    if (head.contigSeq >= finalSeq && localChainAtFinal === finalChain) {
      // local prefix agrees with canonical — quarantine anything beyond finalSeq
      const doomed = this.store.entriesRange(d.topic, d.writer, finalSeq + 1, head.contigSeq);
      const now = new Date(this.clock()).toISOString();
      for (const { entry } of doomed) {
        this.store.quarantinePut(entry, "canonicalized", now);
        anomalies.push({ kind: "entry_quarantined", entry });
      }
      this.store.deleteLogRange(d.topic, d.writer, finalSeq + 1);
      head.contigSeq = finalSeq;
      head.contigChain = finalChain;
      head.sealReason = "retired";
      head.rgen = d.rgen;
      head.retiredAt = now;
      head.finalSeq = finalSeq;
      head.finalChain = finalChain;
      this.saveHead(head);
      this.recoveries.delete(key);
      done("applied");
      return;
    }

    // divergent or missing canonical range — enter §12 recovery (steps 1–4 run
    // via rewind-on-mismatch during canonical ingest; step 5 on completion)
    if (head.sealReason === null) {
      head.sealReason = "fork";
      this.saveHead(head);
    }
    this.recoveries.set(key, { finalSeq, finalChain, rgen: d.rgen, unavailableReported: false });
    done("recovery");
  }

  private processAdopt(item: AdoptCutItem, settle: (() => void)[]): void {
    for (const [writer, cut] of Object.entries(item.cut)) {
      const head = this.getStream(item.topic, writer);
      if (head.contigSeq < cut.seq) {
        head.contigSeq = cut.seq;
        head.contigChain = cut.chain;
        this.saveHead(head);
      }
    }
    settle.push(() => item.resolve());
  }

  private recoveryIngest(
    head: WriterRow,
    e: LogEntry,
    target: RecoveryTarget,
    applied: { entry: LogEntry; rowid: number | null; via: string | undefined }[],
    anomalies: Anomaly[],
    via: string | undefined,
    done: (r: ApplyResult) => void,
  ): void {
    const now = new Date(this.clock()).toISOString();
    if (e.seq <= head.contigSeq) {
      const stored = this.store.getEntry(e.topic, e.writer, e.seq);
      if (stored && stored.chain === e.chain) {
        done("duplicate");
        return;
      }
      // canonical disagrees with the local prefix at this seq — quarantine the
      // divergent suffix back to just below it and rewind (bounds converge to
      // the last common point as earlier canonical entries arrive)
      const doomed = this.store.entriesRange(e.topic, e.writer, e.seq, head.contigSeq);
      for (const { entry } of doomed) {
        this.store.quarantinePut(entry, "canonicalized", now);
        anomalies.push({ kind: "entry_quarantined", entry });
      }
      this.store.deleteLogRange(e.topic, e.writer, e.seq);
      head.contigSeq = e.seq - 1;
      const prev =
        head.contigSeq === 0
          ? seedOf(e.topic, e.writer)
          : this.store.getEntry(e.topic, e.writer, head.contigSeq)?.chain;
      if (prev !== undefined) head.contigChain = prev;
      this.saveHead(head);
    }
    if (e.seq !== head.contigSeq + 1) {
      done("pending"); // recovery pulls contiguously via WANT — skip gaps
      return;
    }
    const prevChain = head.contigSeq === 0 ? seedOf(e.topic, e.writer) : head.contigChain;
    if (chainOf(prevChain, e) !== e.chain) {
      done("forked"); // not the canonical branch either — wait for lower seqs
      return;
    }
    const rowid = this.persist(e);
    head.contigSeq = e.seq;
    head.contigChain = e.chain;
    this.saveHead(head);
    applied.push({ entry: e, rowid, via });
    if (head.contigSeq === target.finalSeq) {
      if (head.contigChain === target.finalChain) {
        head.sealReason = "retired";
        head.rgen = target.rgen;
        head.retiredAt = now;
        head.finalSeq = target.finalSeq;
        head.finalChain = target.finalChain;
        this.saveHead(head);
        this.recoveries.delete(`${e.topic} ${e.writer}`);
      }
    }
    done("applied");
  }

  private processAppend(
    item: AppendItem,
    settle: (() => void)[],
    applied: { entry: LogEntry; rowid: number | null; via: string | undefined }[],
  ): void {
    const head = this.getStream(item.topic, this.writerId);
    if (head.sealReason !== null) {
      settle.push(() =>
        item.reject(new SeqscribeError("ERR_WRITER_SEALED", `${item.topic}/${this.writerId}`)),
      );
      return;
    }
    let hlc;
    try {
      const stamped = stamp(this.hlcState, this.clock());
      this.hlcState = stamped.state;
      hlc = stamped.hlc;
    } catch (e) {
      settle.push(() => item.reject(e));
      return;
    }

    const seq = head.contigSeq + 1;
    const prevChain = head.contigSeq === 0 ? seedOf(item.topic, this.writerId) : head.contigChain;
    const entry: LogEntry = {
      topic: item.topic,
      writer: this.writerId,
      seq,
      hlc,
      kind: item.kind,
      payload: item.payload,
      chain: "",
    };
    if (item.key !== undefined) entry.key = item.key;
    const causal = item.causalProvider ? item.causalProvider(seq) : item.causal;
    if (causal !== undefined) entry.causal = causal;
    if (item.ref !== undefined) entry.ref = item.ref;
    entry.chain = chainOf(prevChain, entry);

    try {
      validateEntry(entry, this.constants);
    } catch (e) {
      settle.push(() => item.reject(e));
      return;
    }

    const rowid = this.persist(entry);
    head.contigSeq = seq;
    head.contigChain = entry.chain;
    this.saveHead(head);
    applied.push({ entry, rowid, via: undefined });
    const id: EntryId = [entry.topic, entry.writer, entry.seq];
    settle.push(() => item.resolve(id));
  }

  private processExternal(
    item: ExternalItem,
    settle: (() => void)[],
    applied: { entry: LogEntry; rowid: number | null; via: string | undefined }[],
    anomalies: Anomaly[],
  ): void {
    const done = (r: ApplyResult) => settle.push(() => item.resolve(r));
    const e = item.entry;
    const head = this.getStream(e.topic, e.writer);

    if (head.sealReason !== null) {
      // §12 step 3: canonical-prefix ingest during recovery passes through the seal
      const target = this.recoveries.get(`${e.topic} ${e.writer}`);
      if (target && e.seq <= target.finalSeq) {
        this.recoveryIngest(head, e, target, applied, anomalies, item.via, done);
        return;
      }
      done("sealed");
      return;
    }

    // §7.5 enforcement: order ≤ P outside the cover is rejected at ingest
    const cert = this.getCert(e.topic);
    if (cert && orderCompare(orderOf(e), cert.order) <= 0) {
      const cover = cert.cut[e.writer]?.seq ?? 0;
      if (e.seq > cover) {
        this.store.annotate(
          e.topic,
          e.writer,
          e.seq,
          "pre_finality_rejected",
          new Date(this.clock()).toISOString(),
        );
        anomalies.push({ kind: "pre_finality_rejected", entry: e });
        done("rejected_finality");
        return;
      }
    }

    if (e.seq <= head.contigSeq) {
      const stored = this.store.getEntry(e.topic, e.writer, e.seq);
      if (stored && stored.chain === e.chain) done("duplicate");
      else if (stored) {
        // same-id-different-content — fork path, never a silent skip (§6.2)
        this.seal(head, e, anomalies);
        done("forked");
      } else done("duplicate"); // below local retention; contig already covers it
      return;
    }

    if (e.seq === head.contigSeq + 1) {
      const r = this.verifyAndApply(head, e, applied, anomalies, item.via);
      if (r === "applied") {
        this.mergeClock(e, anomalies);
        this.drainPending(head, applied, anomalies, item.via);
      }
      done(r);
      return;
    }

    // out-of-order → sq_pending (cap per stream; overflow drops, WANT recovers)
    const existing = this.store.pendingGet(e.topic, e.writer, e.seq);
    if (existing) {
      if (existing.chain === e.chain) done("duplicate");
      else {
        this.seal(head, e, anomalies);
        done("forked");
      }
      return;
    }
    if (this.store.pendingCount(e.topic, e.writer) >= this.constants.PENDING_CAP) {
      done("dropped_overflow");
      return;
    }
    this.store.pendingPut(e);
    this.mergeClock(e, anomalies);
    done("pending");
  }

  private verifyAndApply(
    head: WriterRow,
    e: LogEntry,
    applied: { entry: LogEntry; rowid: number | null; via: string | undefined }[],
    anomalies: Anomaly[],
    via: string | undefined,
  ): "applied" | "forked" {
    const prevChain = head.contigSeq === 0 ? seedOf(e.topic, e.writer) : head.contigChain;
    if (chainOf(prevChain, e) !== e.chain) {
      this.seal(head, e, anomalies);
      return "forked";
    }
    if (head.contigSeq > 0) {
      const prev = this.store.getEntry(e.topic, e.writer, head.contigSeq);
      // per-writer HLC monotonicity: non-decreasing with seq (§1)
      if (prev && hlcCompare(prev.hlc, e.hlc) > 0) {
        this.seal(head, e, anomalies);
        return "forked";
      }
    }
    const rowid = this.persist(e);
    head.contigSeq = e.seq;
    head.contigChain = e.chain;
    this.saveHead(head);
    applied.push({ entry: e, rowid, via });
    return "applied";
  }

  private drainPending(
    head: WriterRow,
    applied: { entry: LogEntry; rowid: number | null; via: string | undefined }[],
    anomalies: Anomaly[],
    via: string | undefined,
  ): void {
    for (;;) {
      const next = this.store.pendingGet(head.topic, head.writer, head.contigSeq + 1);
      if (!next) return;
      this.store.pendingDelete(head.topic, head.writer, next.seq);
      if (this.verifyAndApply(head, next, applied, anomalies, via) === "forked") return;
    }
  }

  private persist(e: LogEntry): number | null {
    const policy = this.topics.get(e.topic).policy;
    if (policy.retention.mode === "full") return this.store.insertEntry(e);
    if (policy.retention.mode === "ring") {
      const prev = this.rings.get(e.topic);
      // journal the pre-txn tail once per flush — restored if the commit aborts
      if (this.ringUndo && !this.ringUndo.has(e.topic))
        this.ringUndo.set(e.topic, prev ? [...prev] : undefined);
      const ring = prev ?? [];
      ring.push(e);
      const size = (policy.retention as { size: number }).size;
      if (ring.length > size) ring.splice(0, ring.length - size);
      this.rings.set(e.topic, ring);
    }
    return null; // ring/none: no durable log row; stream head still persists (§14)
  }

  private mergeClock(e: LogEntry, anomalies: Anomaly[]): void {
    const pt = this.clock();
    if (isOverEpsilon(e.hlc, pt, this.constants.HLC_EPSILON_MS)) {
      this.store.annotate(e.topic, e.writer, e.seq, "clock_outlier", new Date(pt).toISOString());
      anomalies.push({ kind: "clock_outlier", entry: e });
      return;
    }
    this.hlcState = merge(this.hlcState, e.hlc, pt);
  }

  private seal(head: WriterRow, offending: LogEntry, anomalies: Anomaly[]): void {
    head.sealReason = "fork";
    this.saveHead(head);
    anomalies.push({ kind: "writer_forked", entry: offending });
  }
}
