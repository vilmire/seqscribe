// SPEC §14 — createSeqscribe wiring: the full SeqscribeNode surface assembled
// from the hubs (log core, sync, views, consumers, finality, subscriptions,
// registers, directives, snapshots, beacon).

import { BeaconHub, type BeaconStartOpts } from "./beacon.js";
import { assertWriter } from "./codec.js";
import { ConsumerHub, type ConsumerInfo, type ConsumerResetResult } from "./consume.js";
import { orderCompare, orderOf } from "./hlc.js";
import { resolveConstants } from "./constants.js";
import { DirectiveHub } from "./directives.js";
import { misuse } from "./errors.js";
import { exportTopic, importTopic } from "./export.js";
import { FinalityHub } from "./finality.js";
import { LogCore } from "./log.js";
import { ArchiveHub } from "./archive.js";
import { RegisterHub } from "./register.js";
import { SnapshotHub } from "./snapshot.js";
import { Store } from "./store.js";
import { SubHub } from "./subs.js";
import { SyncEngine, type TopicSyncCounters } from "./sync.js";
import { TopicRegistry } from "./topics.js";
import { ViewHub } from "./views.js";
import type { PeerHandleExt } from "./session.js";
import type {
  Anomaly,
  BeaconHandle,
  BeaconReport,
  BeaconTransport,
  Channel,
  CreateOpts,
  EntryId,
  JsonValue,
  LogEntry,
  Order,
  Seq,
  SeqscribeNode,
  Timers,
  Topic,
  Unsub,
  WriterId,
} from "./types.js";

// Bounded read-only inspection (proposals-v3.5 P21, P25). Two mutually
// exclusive forms: canonical total-order (`after`/`through` Orders — pin
// `through` via headOrder() so both sides of a comparison share one closed
// interval) and per-writer seq range (`writer` + fromSeq/toSeq — spans the
// cold archive and, for ring topics, the in-memory tail).
// Never creates a durable cursor, never gates archiving.
export interface ScanOptions {
  after?: Order; // canonical form: exclusive lower bound
  through?: Order; // canonical form: inclusive upper bound (a pinned head)
  writer?: WriterId; // writer form: selects the seq-range scan
  fromSeq?: Seq; // writer form: inclusive, default 1
  toSeq?: Seq; // writer form: inclusive, default the local contiguous head
  limit?: number; // page bound — default 500, hard cap 10_000
}

export interface ScanResult {
  entries: LogEntry[];
  complete: boolean; // false = limit truncated the page; resume via nextAfter/nextFromSeq
  // the requested lower bound predates locally available retention: canonical
  // scans do not see cold-archived rows (§7.6), writer scans DO span the
  // archive and the ring tail (P25), so here it means rows below the first
  // locally held seq — for a ring topic, below what the ring still holds
  truncatedBelow: boolean;
  nextAfter?: Order; // canonical form resume token
  nextFromSeq?: Seq; // writer form resume token
}

// Observability surface (extension beyond SPEC §14 — proposals-v3.5). The
// host-guide's baseline metrics, one call away.
export interface NodeStats {
  topics: Record<
    Topic,
    {
      writers: number;
      logRows: number;
      pending: number;
      quarantined: number;
      archived: number;
      finalityGeneration: number | null;
      certOrderAgeMs: number | null;
      consumers: Record<string, { lastRowid: number; lagRows: number }>;
      // cumulative non-applied wire-apply outcomes (proposals-v3.5 P22) —
      // rejected_finality / sealed / forked / dropped_overflow / error
      applyRejects: Record<string, number>;
      // interval sync throughput (proposals-v3.5 P24): counts accumulated
      // since the PREVIOUS stats() call — every stats() read resets them
      // (interval, not cumulative; applyRejects above stays cumulative).
      // "hot and busy" is now distinguishable from P22's "hot and stuck".
      sync: TopicSyncCounters;
    }
  >;
  peers: {
    peerId: string;
    state: string;
    dirtyStreams: number;
    queuedData: number;
    stalledStreams: number; // P22 — streams suspended for non-progress
  }[];
  // P24 — top-5 (topic, peer) pairs by served+applied bytes this interval;
  // bounded, and both identifiers are already public elsewhere in stats()
  syncHotspots: { topic: Topic; peerId: string; bytes: number }[];
}

export interface SeqscribeNodeExt extends SeqscribeNode {
  stats(): NodeStats;
  // Durable-consumer lifecycle (proposals-v3.5 P17–P19) — reset/delete/prune
  // are inactive-consumer operations; caughtUp needs the consumer registered.
  resetConsumer(
    topic: Topic,
    consumer: string,
    o?: { from?: "earliest-retained" | "head" },
  ): ConsumerResetResult;
  deleteConsumer(topic: Topic, consumer: string): { existed: boolean };
  listConsumers(topic: Topic): ConsumerInfo[];
  pruneConsumers(topic: Topic, o?: { prefix?: string; inactiveBefore?: number }): string[];
  consumerCaughtUp(topic: Topic, consumer: string): Promise<{ throughRowid: number }>;
  // Bounded inspection (P21)
  scanEntries(topic: Topic, o?: ScanOptions): ScanResult;
  headOrder(topic: Topic): Order | null; // pin scan `through` / comparison heads
  // §5.7 hint supply (proposals-v3.5 P27, a v3.6-cycle extension). The ratified
  // §14 `beacon(t)` is unchanged and still complete on its own; this overload
  // only adds the optional host-supplied hints callback, so existing callers
  // and the base SeqscribeNode contract are untouched.
  beacon(t: BeaconTransport, o?: BeaconStartOpts): BeaconHandle;
  // The handle attach really returns (proposals-v3.5 P15/P16): the SPEC §14
  // PeerHandle plus reasoned lifecycle and runtime grant re-advertisement.
  attach(
    ch: Channel,
    o: {
      peerId: string;
      peerClass: "content" | "metadata";
      grants: Record<Topic, "full" | "serve" | "none">;
    },
  ): PeerHandleExt;
}

// Host globals via globalThis — the core compiles without platform lib types.
const g = globalThis as unknown as {
  setTimeout(cb: () => void, ms: number): unknown;
  clearTimeout(h: unknown): void;
  crypto?: { getRandomValues(a: Uint32Array): Uint32Array };
};
const defaultTimers: Timers = {
  setTimeout: (cb, ms) => g.setTimeout(cb, ms),
  clearTimeout: (h) => g.clearTimeout(h),
};

// production epochs come from crypto randomness; the harness injects opts.rng (§10)
function defaultRng(): () => number {
  if (g.crypto) {
    const buf = new Uint32Array(1);
    return () => {
      g.crypto!.getRandomValues(buf);
      return buf[0]! / 2 ** 32;
    };
  }
  return Math.random;
}

export function createSeqscribe(opts: CreateOpts): SeqscribeNodeExt {
  assertWriter(opts.writerId);
  const constants = resolveConstants(opts.constants);
  const clock = opts.clock ?? (() => Date.now());
  const timers = opts.timers ?? defaultTimers;
  const rng = opts.rng ?? defaultRng();
  const topics = new TopicRegistry();
  const store = new Store(opts.storage);
  store.init(constants.durability);

  const anomalyListeners = new Set<(a: Anomaly) => void>();
  const emitAnomaly = (a: Anomaly) => {
    for (const cb of anomalyListeners) cb(a);
  };

  const core = new LogCore({
    store,
    topics,
    writerId: opts.writerId,
    clock,
    timers,
    constants,
    emitAnomaly,
  });

  const sync = new SyncEngine({
    core,
    store,
    topics,
    writerId: opts.writerId,
    constants,
    timers,
    clock,
    emitAnomaly,
  });

  const consumers = new ConsumerHub({ store, topics, timers, constants, clock });
  const views = new ViewHub({ store, topics, timers, constants, rng, emitAnomaly });
  const finalityHub = new FinalityHub({
    core,
    store,
    topics,
    constants,
    clock,
    emitAnomaly,
    authority: opts.authority,
  });
  sync.setFinalityHub(finalityHub);
  const registers = new RegisterHub({
    core,
    store,
    topics,
    writerId: opts.writerId,
    constants,
    timers,
    clock,
    emitAnomaly,
    authority: opts.authority,
  });
  const subs = new SubHub({ views, core, topics, constants, timers, rng, registers });
  sync.setSubHub(subs);
  registers.onChange((topic) => subs.handleRegisterChanged(topic));
  const directives = new DirectiveHub({
    core,
    store,
    topics,
    writerId: opts.writerId,
    emitAnomaly,
    authority: opts.authority,
    registers,
  });
  sync.setDirectiveHub(directives);
  const snapshots = new SnapshotHub({
    core,
    store,
    topics,
    views,
    registers,
    constants,
    emitAnomaly,
    authority: opts.authority,
  });
  sync.setSnapshotHub(snapshots);
  const archive = new ArchiveHub({ core, store, topics, views, registers, constants, clock, emitAnomaly });
  consumers.setOnAdvance((topic) => archive.onConsumerAdvance(topic));
  // registers is passed so the beacon can source §5.7 hints from the register
  // fold for hintKeys-opted topics without host involvement (P27)
  const beaconHub = new BeaconHub({
    core,
    topics,
    writerId: opts.writerId,
    constants,
    timers,
    clock,
    registers,
  });
  finalityHub.setOnAccepted((cert) => {
    // §7.5c: certificate effects can rewrite covered history — recompute views
    views.notifyApplied(cert.topic);
    sync.onCertAccepted(); // unblocks §7.8-deferred WANTs
    void archive.onCertAccepted(cert.topic); // §7.6: rebase + cold-archive
  });

  core.setOnApplied((e, rowid, via) => {
    sync.handleApplied(e, via);
    beaconHub.notifyApplied();
    if (rowid !== null) {
      consumers.notifyApplied(e.topic);
      views.notifyApplied(e.topic);
      registers.notifyApplied(e.topic);
    } else {
      subs.handleRingApplied(e); // ring topics: no durable row, tail groups feed live
    }
  });

  let closed = false;

  const node: SeqscribeNode = {
    defineTopic(topic, policy) {
      if (closed) throw misuse("node is closed");
      topics.define(topic, policy, opts.authority);
    },

    log(topic: Topic) {
      return {
        append(kind: string, payload: JsonValue, o?: { ref?: EntryId }): Promise<EntryId> {
          // One asynchronous failure contract (proposals-v3.5 P11): every
          // data-dependent preflight failure — closed node, unknown topic,
          // encoding — rejects the returned Promise (SPEC §14 error carriage:
          // "Promise-returning APIs reject"). The single surviving synchronous
          // throw is §11.1's raw append on a register topic ("throws" is
          // normative there): a static API-misuse, not a runtime condition.
          if (topics.has(topic) && topics.get(topic).policy.kind === "register")
            throw misuse(`raw append on register topic ${topic} — use register(topic) helpers`);
          return core.append(topic, kind, payload, o?.ref ? { ref: o.ref } : undefined);
        },
      };
    },

    register: (topic) => registers.handle(topic),

    onEntry: (topic, consumer, cb) => consumers.onEntry(topic, consumer, cb),

    attach(ch, o) {
      if (closed) throw misuse("node is closed");
      for (const topic of Object.keys(o.grants)) topics.get(topic); // defined-before-attach
      return sync.attach(ch, o);
    },

    vectors: () => core.vectors(),

    setKnownVectors: (v: BeaconReport[]) => beaconHub.setKnownVectors(v),
    staleness: (topic, key) => beaconHub.staleness(topic, key),
    beacon: (t: BeaconTransport, o?: BeaconStartOpts) => beaconHub.start(t, o),

    finality: (topic) => finalityHub.finality(topic),
    proposeFinality: (topic) => finalityHub.proposeFinality(topic),
    ingestFinality: (cert) => finalityHub.ingestFinality(cert),
    publishWriterDirective: (d) => directives.publishWriterDirective(d),
    retire: (writer, o) => directives.retire(writer, o),
    unretire: (writer) => directives.unretire(writer),

    view: (name, topic, def) => views.register(name, topic, def as never),
    serveView: (name, resolver) => subs.serveView(name, resolver),
    subscribe: (peer, o) => sync.subscribe(peer.peerId, o),

    onConflict: (topic, cb) => registers.onConflict(topic, cb),
    onOwnedRequest: (topic, cb) => registers.onOwnedRequest(topic, cb),

    onAnomaly(cb: (a: Anomaly) => void): Unsub {
      anomalyListeners.add(cb);
      return () => anomalyListeners.delete(cb);
    },

    ownerOf: (topic, key) => registers.ownerOf(topic, key),
    pendingRequests: (topic, now) => registers.pendingRequests(topic, now),
    rebuildView: (name) => views.rebuild(name),
    export: (topic, format) => {
      if (format !== "jsonl") throw misuse(`unsupported export format ${String(format)}`);
      return exportTopic({ core, store, topics, constants }, topic);
    },
    import: (topic, lines) => importTopic({ core, store, topics, constants }, topic, lines),

    async close() {
      if (closed) return;
      closed = true;
      sync.closeAll();
      await core.close();
      // §14 close quiescence: the final flush above fans out through
      // onApplied and schedules hub timers — close the hubs AFTER the core so
      // those timers are cancelled and nothing touches the store once the
      // owner lock releases. The deferred work is recoverable on next open
      // (cursors, checkpoints, lastRowid floors).
      consumers.close();
      views.close();
      registers.close();
      beaconHub.close();
      store.close();
    },
  };

  // P21 — bounded read-only scans over what this node holds locally.
  const SCAN_DEFAULT_LIMIT = 500;
  const SCAN_MAX_LIMIT = 10_000;
  const scanEntries = (topic: Topic, o: ScanOptions = {}): ScanResult => {
    if (closed) throw misuse("node is closed");
    topics.get(topic);
    const limit = Math.min(Math.max(1, Math.floor(o.limit ?? SCAN_DEFAULT_LIMIT)), SCAN_MAX_LIMIT);
    if (o.writer !== undefined) {
      if (o.after !== undefined || o.through !== undefined)
        throw misuse("scanEntries: writer form takes fromSeq/toSeq, not after/through");
      const head = core.getStream(topic, o.writer);
      const fromSeq = o.fromSeq ?? 1;
      const toSeq = o.toSeq ?? head.contigSeq;
      if (toSeq < fromSeq) return { entries: [], complete: true, truncatedBelow: false };
      // a seq window of `limit` bounds the page regardless of gaps — an
      // under-filled page just resumes at nextFromSeq
      const windowEnd = Math.min(toSeq, fromSeq + limit - 1);
      const bySeq = new Map<number, LogEntry>();
      for (const e of store.archivedEntries(topic, o.writer, fromSeq, windowEnd)) bySeq.set(e.seq, e);
      for (const { entry } of store.entriesRange(topic, o.writer, fromSeq, windowEnd))
        bySeq.set(entry.seq, entry);
      // P25 — a ring-retention topic's live entries are the in-memory tail and
      // nothing else: persist() writes no sq_log/sq_archive row for them
      // (§14). Merging the tail as a third source keeps the seq-keyed dedup
      // and window bounds of the two durable sources above, so no new merge
      // semantics appear; without it a ring scan returns an empty page while
      // the stream head says the seq is locally present.
      for (const e of core.ringTail(topic))
        if (e.writer === o.writer && e.seq >= fromSeq && e.seq <= windowEnd) bySeq.set(e.seq, e);
      const entries = [...bySeq.values()].sort((a, b) => a.seq - b.seq);
      const complete = windowEnd >= toSeq;
      const firstAvail = entries[0]?.seq;
      const truncatedBelow =
        firstAvail !== undefined ? firstAvail > fromSeq : head.contigSeq >= fromSeq;
      const r: ScanResult = { entries, complete, truncatedBelow };
      if (!complete) r.nextFromSeq = windowEnd + 1;
      return r;
    }
    if (o.fromSeq !== undefined || o.toSeq !== undefined)
      throw misuse("scanEntries: fromSeq/toSeq require writer");
    const fetched = store.entriesAfterOrder(topic, o.after ?? null, limit + 1);
    // canonical order is the fetch order, so rows beyond `through` are a suffix
    const within = o.through
      ? fetched.filter((e) => orderCompare(orderOf(e), o.through!) <= 0)
      : fetched;
    const entries = within.slice(0, limit);
    const complete = within.length <= limit && (within.length < fetched.length || fetched.length <= limit);
    // canonical scans read the hot log only — cold-archived rows (§7.6) sit
    // below the cut; a lower bound under the cut order is therefore incomplete
    const cert = core.getCert(topic);
    const truncatedBelow =
      store.archivedCount(topic) > 0 &&
      (o.after === undefined || (cert !== null && orderCompare(o.after, cert.order) < 0));
    const r: ScanResult = { entries, complete, truncatedBelow };
    if (!complete && entries.length > 0) r.nextAfter = orderOf(entries[entries.length - 1]!);
    return r;
  };

  const stats = (): NodeStats => {
    // P24 — one drain per stats() call: the interval counters cover
    // [previous stats() read, this one] and reset atomically here
    const interval = sync.drainIntervalStats();
    const out: NodeStats = { topics: {}, peers: sync.peerStats(), syncHotspots: interval.hotspots };
    const now = clock();
    for (const topic of topics.list()) {
      const cert = core.getCert(topic);
      const maxRowid = store.maxRowid(topic);
      const consumers: NodeStats["topics"][string]["consumers"] = {};
      for (const c of store.cursorsForTopic(topic)) {
        consumers[c.consumer] = {
          lastRowid: c.lastRowid,
          lagRows: Math.max(0, maxRowid - c.lastRowid),
        };
      }
      out.topics[topic] = {
        writers: store.listWriters(topic).length,
        logRows: store.logCount(topic),
        pending: store.pendingCountForTopic(topic),
        quarantined: store.quarantineCount(topic),
        archived: store.archivedCount(topic),
        finalityGeneration: cert?.generation ?? null,
        certOrderAgeMs: cert ? Math.max(0, now - cert.order.l) : null,
        consumers,
        applyRejects: sync.rejectStats(topic),
        sync: interval.topics.get(topic) ?? {
          servedEntries: 0,
          servedBytes: 0,
          appliedEntries: 0,
          appliedBytes: 0,
          wantRoundsRequested: 0,
          wantRoundsServed: 0,
        },
      };
    }
    return out;
  };

  // The double cast is sound: node.attach delegates to sync.attach, which
  // really returns PeerHandleExt — the literal is merely annotated with the
  // SPEC-shaped SeqscribeNode, whose attach names the base PeerHandle.
  return Object.assign(node, {
    stats,
    resetConsumer: (topic: Topic, consumer: string, o?: { from?: "earliest-retained" | "head" }) =>
      consumers.resetConsumer(topic, consumer, o),
    deleteConsumer: (topic: Topic, consumer: string) => consumers.deleteConsumer(topic, consumer),
    listConsumers: (topic: Topic) => consumers.listConsumers(topic),
    pruneConsumers: (topic: Topic, o?: { prefix?: string; inactiveBefore?: number }) =>
      consumers.pruneConsumers(topic, o),
    consumerCaughtUp: (topic: Topic, consumer: string) => consumers.caughtUp(topic, consumer),
    scanEntries,
    headOrder: (topic: Topic): Order | null => {
      if (closed) throw misuse("node is closed");
      topics.get(topic);
      return store.maxOrderUpTo(topic, Number.MAX_SAFE_INTEGER);
    },
    _core: core,
    _views: views,
    _registers: registers,
    _directives: directives,
    _sync: sync,
  }) as unknown as SeqscribeNodeExt;
}

// Internal escape hatch for the harness and tests (not part of the public API).
export function coreOf(node: SeqscribeNode): LogCore {
  return (node as unknown as { _core: LogCore })._core;
}

export type { LogCore };
export type ApplyExternal = (entry: LogEntry) => Promise<string>;
