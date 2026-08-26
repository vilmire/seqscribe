// SPEC §14 — createSeqscribe wiring: the full SeqscribeNode surface assembled
// from the hubs (log core, sync, views, consumers, finality, subscriptions,
// registers, directives, snapshots, beacon).

import { BeaconHub } from "./beacon.js";
import { assertWriter } from "./codec.js";
import { ConsumerHub } from "./consume.js";
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
import { SyncEngine } from "./sync.js";
import { TopicRegistry } from "./topics.js";
import { ViewHub } from "./views.js";
import type {
  Anomaly,
  BeaconReport,
  CreateOpts,
  EntryId,
  JsonValue,
  LogEntry,
  SeqscribeNode,
  Timers,
  Topic,
  Unsub,
} from "./types.js";

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

export function createSeqscribe(opts: CreateOpts): SeqscribeNode {
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
  const beaconHub = new BeaconHub({ core, topics, writerId: opts.writerId, constants, timers, clock });
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
      const policy = topics.get(topic).policy;
      return {
        append(kind: string, payload: JsonValue, o?: { ref?: EntryId }): Promise<EntryId> {
          // §11.1: raw append on a register topic is a synchronous ERR_MISUSE —
          // register writes go only through helpers so causal stamping stays sound.
          if (policy.kind === "register")
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
    beacon: (t) => beaconHub.start(t),

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
      store.close();
    },
  };

  return Object.assign(node, {
    _core: core,
    _views: views,
    _registers: registers,
    _directives: directives,
    _sync: sync,
  }) as SeqscribeNode;
}

// Internal escape hatch for the harness and tests (not part of the public API).
export function coreOf(node: SeqscribeNode): LogCore {
  return (node as unknown as { _core: LogCore })._core;
}

export type { LogCore };
export type ApplyExternal = (entry: LogEntry) => Promise<string>;
