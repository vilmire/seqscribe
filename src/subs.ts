// SPEC §10, §5.4 SUB/SNAP/DELTA — Tier-2 subscriptions. Server: serving groups
// per (view, JCS(params)) with an in-memory delta journal, "compute once,
// broadcast", SNAP with byte-level chunking, DELTA-overflow downgrade. Client:
// subscribe/resume with opaque {epoch, deltaSeq} cursors.
//
// Resync discipline (host-guide §4.6): a subscriber that cannot take a DELTA —
// tail-dropped by a full §5.2 data lane, or over MAX_FRAME_BYTES — is marked
// for ONE coalesced SNAP(reset), generated once its lane has drained and paced
// onto it by capacity. Before this, every applied write past the queue cap
// re-read and re-serialized the whole tail, and the SNAP's own chunks were
// tail-dropped by the same full queue — O(writes × tail bytes) of event-loop
// work that never delivered a complete SNAP.

import { jcs, utf8ByteLength } from "./encoding.js";
import { misuse, SeqscribeError } from "./errors.js";
import type { LogCore } from "./log.js";
import type { RegisterHub } from "./register.js";
import type { MsgDelta, MsgSnap, MsgSub, MsgSubErr, MsgUnsub } from "./messages.js";
import type { Session } from "./session.js";
import type { TopicRegistry } from "./topics.js";
import type {
  Anomaly,
  Constants,
  JsonValue,
  LogEntry,
  Row,
  Subscription,
  Timers,
  Topic,
  Unsub,
  ViewHandle,
} from "./types.js";
import type { ViewChange, ViewHub } from "./views.js";

// ---- base64 (platform-agnostic, byte-level chunking per §5.4) ----

const B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
// Byte tables instead of per-character string building / Map lookups: a SNAP
// body is up to tens of MiB, and the string-concatenating encoder spent ~0.9 s
// per 23 MiB (the largest single cost of a full-tail SNAP). Output is
// byte-identical; decoding keeps the lenient "unknown character → 0" rule.
const B64_CODES = Uint8Array.from(B64, (c) => c.charCodeAt(0));
const B64_REV = new Uint8Array(256);
for (let i = 0; i < 64; i++) B64_REV[B64.charCodeAt(i)] = i;
const PAD = 61; // "="

// "tail" SUB view window for a full-retention subscribe-only topic (G2b) —
// the durable counterpart to a ring topic's `retention.size`. Deliberately
// NOT a `Constants` field: SPEC §14's "Not constants" carve-out (the §14.1
// scan page bounds) applies identically here — this bounds one synchronous
// local read, not a protocol or storage behavior, so there is nothing for a
// fleet to agree on. Same magnitude as RING_DEFAULT so the existing
// MAX_REASSEMBLY_BYTES sizing note (constants.ts) — "largest legitimate SNAP
// body ≈ RING_DEFAULT × MAX_ROW_BYTES" — stays true for full-topic tails too.
export const FULL_TAIL_DEFAULT = 500;

export function b64encode(bytes: Uint8Array): string {
  const n = bytes.length;
  const out = new Uint8Array(Math.ceil(n / 3) * 4);
  let o = 0;
  let i = 0;
  for (; i + 2 < n; i += 3) {
    const x = (bytes[i]! << 16) | (bytes[i + 1]! << 8) | bytes[i + 2]!;
    out[o++] = B64_CODES[x >> 18]!;
    out[o++] = B64_CODES[(x >> 12) & 63]!;
    out[o++] = B64_CODES[(x >> 6) & 63]!;
    out[o++] = B64_CODES[x & 63]!;
  }
  if (i < n) {
    const a = bytes[i]!;
    const b = i + 1 < n ? bytes[i + 1]! : undefined;
    out[o++] = B64_CODES[a >> 2]!;
    out[o++] = B64_CODES[((a & 3) << 4) | ((b ?? 0) >> 4)]!;
    out[o++] = b === undefined ? PAD : B64_CODES[(b & 15) << 2]!;
    out[o++] = PAD;
  }
  return textDec.decode(out);
}

export function b64decode(s: string): Uint8Array {
  let end = s.length;
  while (end > 0 && s.charCodeAt(end - 1) === PAD) end--;
  const at = (i: number): number => {
    if (i >= end) return 0;
    const c = s.charCodeAt(i);
    return c < 256 ? B64_REV[c]! : 0;
  };
  const out = new Uint8Array(Math.floor((end * 3) / 4));
  let o = 0;
  for (let i = 0; i < end; i += 4) {
    const n = (at(i) << 18) | (at(i + 1) << 12) | (at(i + 2) << 6) | at(i + 3);
    if (o < out.length) out[o++] = (n >> 16) & 0xff;
    if (o < out.length) out[o++] = (n >> 8) & 0xff;
    if (o < out.length) out[o++] = n & 0xff;
  }
  return out;
}

const textEnc = new (globalThis as unknown as {
  TextEncoder: new () => { encode(s: string): Uint8Array };
}).TextEncoder();
const textDec = new (globalThis as unknown as {
  TextDecoder: new () => { decode(b: Uint8Array): string };
}).TextDecoder();

interface CursorVal {
  e: string; // epoch
  d: number; // deltaSeq
}

function encodeCursor(c: CursorVal): string {
  return JSON.stringify(c);
}

function decodeCursor(s: string): CursorVal | null {
  try {
    const v = JSON.parse(s) as CursorVal;
    if (typeof v.e === "string" && Number.isSafeInteger(v.d)) return v;
  } catch {
    /* fallthrough */
  }
  return null;
}

// ---- serving groups ----

interface JournalEntry {
  seq: number;
  changes: { upserts: Row[]; deletes: string[] };
  bytes: number; // utf8 length of JSON(changes) — computed once per publish
}

interface Group {
  key: string;
  viewName: string | null; // null for ring tail groups
  ringTopic: Topic | null;
  epoch: string;
  deltaSeq: number;
  journal: JournalEntry[];
  subs: Map<Session, Map<number, Serve>>; // subId → serving state, per session
  rowsProvider: () => Row[];
  // "compute once, broadcast" for SNAP bodies: every subscriber resyncing at
  // the same (epoch, deltaSeq) shares one serialized body. Cleared by publish
  // (deltaSeq moves) and whenever the group empties.
  snapCache: { epoch: string; seq: number; body: Uint8Array } | null;
}

// A SNAP being paced onto one subscriber's data lane (see pumpSnap).
interface SnapOut {
  body: Uint8Array;
  of: number;
  next: number; // next chunk index to enqueue (1-based)
  cursor: string;
  epoch: string;
  seq: number; // group.deltaSeq the body reflects
}

// Per-(session, subId) serving state. Resync is coalesced HERE: once a
// subscriber cannot take a DELTA (tail-dropped by a full data lane, or a
// DELTA over MAX_FRAME_BYTES), it stops receiving DELTAs and is marked
// `pending`; exactly one SNAP is generated once its session's queue has
// drained, and at most one SNAP is ever in flight per subscriber. Writes that
// land meanwhile only bump a counter — the SNAP (plus the journal replay after
// it) covers them.
interface Serve {
  group: Group;
  session: Session;
  subId: number;
  pending: boolean;
  snap: SnapOut | null;
  timer: unknown | null;
  backoffMs: number;
  closed: boolean;
}

interface ClientSub {
  session: Session;
  subId: number;
  cursor: string | undefined;
  snapshotCbs: Set<(rows: Row[], reset: boolean) => void>;
  deltaCbs: Set<(c: { upserts: Row[]; deletes: string[] }) => void>;
  view: string;
  params: JsonValue;
  chunks: Map<number, string>; // pending SNAP chunks
  chunksOf: number;
  chunkBytes: number; // accumulated b64 payload — bounded by MAX_REASSEMBLY_BYTES
  chunkCursor: string;
  chunkReset: boolean;
  closed: boolean;
}

export type SubResyncReason = "backpressure" | "oversized";

// Cumulative counters since node start (gauges marked). A pure read.
export interface SubStats {
  subscribers: number; // gauge — serving (session, subId) pairs
  resyncPending: number; // gauge — subscribers waiting for a coalesced SNAP
  snapsInFlight: number; // gauge — SNAPs still being paced out
  snapsStarted: number;
  snapsCompleted: number;
  snapsAbandoned: number; // superseded mid-flight by an epoch reset
  snapBytes: number; // serialized body bytes of started SNAPs (pre-base64)
  snapChunksSent: number;
  snapCacheHits: number; // SNAP starts served from the group's shared body
  resyncs: number; // subscribers entering resync
  resyncsBackpressure: number;
  resyncsOversized: number;
  resyncWritesCoalesced: number; // DELTAs withheld while a resync was pending or in flight
  deltasSent: number;
}

// The backward walk a tail-snapshot selector gets over one topic's "tail"
// window (durable sq_log rows for `full` topics, the in-memory ring for `ring`
// topics). `page` returns newest-first; `rowid` is strictly decreasing and is
// the next call's `beforeRowid`. `defaultLimit` is the window the default
// SNAP would have served — a selector should bound its walk by it.
export interface TailSource {
  readonly topic: Topic;
  readonly retention: "ring" | "full";
  readonly defaultLimit: number;
  page(beforeRowid: number | null, limit: number): { entry: LogEntry; rowid: number }[];
  // Keyed-append topics (TopicPolicy.keyed, host-guide §4.7). For a keyed
  // topic the default SNAP is latestPerKey(null) — the whole newest-per-key
  // set, NOT the last `defaultLimit` rows — and a selector typically returns
  // latestPerKey(W) followed by rowsAfter(W), W being its own commit
  // watermark (keyHead of its commit key). latestPerKey/keyHead throw
  // ERR_MISUSE on a non-keyed topic; rowsAfter on a ring topic (both are a
  // selector fault → default SNAP).
  readonly keyed: boolean;
  latestPerKey(uptoRowid: number | null): { entry: LogEntry; rowid: number }[];
  rowsAfter(rowid: number): { entry: LogEntry; rowid: number }[];
  keyHead(key: string): { entry: LogEntry; rowid: number } | null;
}

// Host hook (extension): chooses which rows a "tail" SNAP carries for a topic.
// Returns entries oldest-first, drawn from `src`; null/undefined → the default
// window (last `defaultLimit` rows). Only SNAP bodies are affected — DELTAs,
// cursors and epochs are unchanged — so it is sound exactly when every reader
// of the topic treats a `reset:true` SNAP as "replace your state with this",
// and the selected rows are sufficient to rebuild it (e.g. a topic whose rows
// form self-contained revisions only needs the newest complete one). A
// throwing selector falls back to the default window.
export type TailSnapshotSelector = (src: TailSource) => LogEntry[] | null | undefined;

export interface SubHubDeps {
  views: ViewHub;
  core: LogCore;
  topics: TopicRegistry;
  constants: Constants;
  timers: Timers;
  rng: () => number;
  registers?: RegisterHub | undefined;
  emitAnomaly?: ((a: Anomaly) => void) | undefined;
}

export class SubHub {
  private readonly families = new Map<string, (params: JsonValue) => ViewHandle>();
  private readonly groups = new Map<string, Group>();
  private readonly groupsByView = new Map<string, Group[]>();
  private readonly ringEpochs = new Map<Topic, string>();
  private readonly clientSubs = new Map<string, ClientSub>(); // `${peerId} ${subId}`
  private readonly serves = new Map<Session, Map<number, Serve>>();
  private tailSelector: TailSnapshotSelector | null = null;
  private nextSubId = 1;
  private readonly counters = {
    snapsStarted: 0,
    snapsCompleted: 0,
    snapsAbandoned: 0,
    snapBytes: 0,
    snapChunksSent: 0,
    snapCacheHits: 0,
    resyncs: 0,
    resyncsBackpressure: 0,
    resyncsOversized: 0,
    resyncWritesCoalesced: 0,
    deltasSent: 0,
  };

  constructor(private readonly deps: SubHubDeps) {
    deps.views.onViewChange((c) => this.onViewChange(c));
  }

  // ---- server: registration ----

  serveView(name: string, resolver: (params: JsonValue) => ViewHandle): void {
    if (this.families.has(name)) throw misuse(`serveView name already registered: ${name}`);
    this.families.set(name, resolver);
  }

  setTailSnapshotSelector(sel: TailSnapshotSelector | null): void {
    this.tailSelector = sel;
    // a cached body may have been produced by the previous selector
    for (const g of this.groups.values()) if (g.ringTopic !== null && g.viewName === null) g.snapCache = null;
  }

  stats(): SubStats {
    let subscribers = 0;
    let resyncPending = 0;
    let snapsInFlight = 0;
    for (const bySub of this.serves.values()) {
      for (const s of bySub.values()) {
        subscribers++;
        if (s.pending) resyncPending++;
        if (s.snap) snapsInFlight++;
      }
    }
    return { subscribers, resyncPending, snapsInFlight, ...this.counters };
  }

  // ---- server: wire handlers ----

  handleSub(session: Session, m: MsgSub): void {
    let group: Group;
    try {
      group = this.resolveGroup(m.view, m.params);
    } catch (e) {
      const code = e instanceof SeqscribeError ? e.code : "ERR_UNKNOWN_VIEW";
      session.sendControl({ t: "SUB_ERR", subId: m.subId, code } satisfies MsgSubErr);
      return;
    }
    const topic = group.ringTopic ?? this.deps.views.get(group.viewName!).topic;
    if (!session.peerMaySub(topic)) {
      session.sendControl({ t: "SUB_ERR", subId: m.subId, code: "ERR_ACL_DENIED" });
      return;
    }

    // A SUB retry (§5.3: re-sent every CONTROL_RETRY_MS until the first SNAP/
    // DELTA frame lands) for a subscriber whose SNAP is still queued or being
    // paced out: that SNAP answers it. Restarting would abandon a half-sent
    // body on every retry — on a slow link, forever.
    const existing = this.serves.get(session)?.get(m.subId);
    if (existing && existing.group === group && (existing.pending || existing.snap)) return;

    const cursor = m.fromCursor !== undefined ? decodeCursor(m.fromCursor) : null;
    if (cursor && cursor.e === group.epoch) {
      if (cursor.d > group.deltaSeq) {
        session.sendControl({ t: "SUB_ERR", subId: m.subId, code: "ERR_FUTURE_CURSOR" });
        return;
      }
      const oldest = group.journal[0]?.seq ?? group.deltaSeq + 1;
      if (cursor.d + 1 >= oldest) {
        // resumable: register, replay missed deltas from the journal
        const serve = this.addSubscriber(group, session, m.subId);
        for (const j of group.journal) {
          if (j.seq > cursor.d) this.deliverDelta(serve, j);
          if (serve.pending || serve.snap) break; // fell into resync — the SNAP covers the rest
        }
        return;
      }
    }
    // fresh or beyond retention or epoch mismatch → SNAP reset
    const serve = this.addSubscriber(group, session, m.subId);
    serve.pending = true;
    this.tryStartSnap(serve);
  }

  handleUnsub(session: Session, m: MsgUnsub): void {
    const serve = this.serves.get(session)?.get(m.subId);
    if (serve) this.removeServe(serve);
  }

  handleSessionClosed(session: Session): void {
    for (const serve of [...(this.serves.get(session)?.values() ?? [])]) this.removeServe(serve);
    this.serves.delete(session);
    for (const [key, sub] of [...this.clientSubs]) {
      if (sub.session === session) {
        sub.closed = true;
        this.clientSubs.delete(key);
      }
    }
  }

  // The session's data-lane queue drained below SEND_QUEUE_CAP (an ACK
  // advanced). This is the drain signal pending resyncs and paced SNAPs wait
  // on; a backoff timer covers the case where it never comes.
  handleCapacity(session: Session): void {
    const bySub = this.serves.get(session);
    if (!bySub) return;
    for (const serve of [...bySub.values()]) {
      if (serve.closed) continue;
      if (serve.snap) this.pumpSnap(serve);
      else if (serve.pending) this.tryStartSnap(serve);
    }
  }

  // register materialization rewrote the built-in table — SNAP-reset the group
  handleRegisterChanged(topic: Topic): void {
    const group = this.groups.get(this.registerKey(topic));
    if (!group) return;
    group.epoch = this.mintEpoch();
    group.deltaSeq = 0;
    group.journal = [];
    this.resetGroup(group);
  }

  private registerKey(topic: Topic): string {
    return `register\u0000${jcs({ topic })}`;
  }

  // Live DELTA feed for "tail" groups — ring topics (rowid-null applies,
  // node.ts's `else` branch) AND full-retention subscribe-only topics
  // (durable applies, rowid !== null) share this: both are keyed by
  // `ringKey(topic)` in resolveGroup above, so one lookup covers either
  // shape and a subscriber sees the identical DELTA wire message regardless
  // of which retention mode its topic uses.
  handleTailApplied(e: LogEntry): void {
    const group = this.groups.get(this.ringKey(e.topic));
    if (!group) return;
    const row = this.ringRow(e);
    this.publish(group, { upserts: [row], deletes: [] });
  }

  // Writer-row GC precondition (retireTopic/gcWriters, C7-7): true if any
  // session currently holds an active SUB on `topic`'s ring tail group. Ring
  // topics (retireTopic's only target — full-sync topics are refused before
  // this check runs) are served exclusively through the "tail" group keyed
  // by ringTopic, never through a named view, so scanning `groups` for a
  // matching `ringTopic` with a non-empty `subs` map is complete for that
  // case. `groups` has no topic-keyed index (it's keyed by `view\0params`),
  // so this is a linear scan — acceptable here: called once per candidate
  // topic in a boot-time sweep, not per-request.
  // Serving subscribers of `topic`'s built-in "tail" group (0 when none) —
  // host-guide §4.7 `tailSubscriberCount`. Unlike hasActiveSubscribersFor it
  // does not count a register topic's "register" group.
  tailSubscriberCount(topic: Topic): number {
    const group = this.groups.get(this.ringKey(topic));
    if (!group) return 0;
    let n = 0;
    for (const inGroup of group.subs.values()) n += inGroup.size;
    return n;
  }

  hasActiveSubscribersFor(topic: Topic): boolean {
    for (const group of this.groups.values()) {
      if (group.ringTopic === topic && group.subs.size > 0) return true;
    }
    return false;
  }

  // ---- server: internals ----

  private resolveGroup(view: string, params: JsonValue): Group {
    const key = `${view}\u0000${jcs(params ?? null)}`;
    const existing = this.groups.get(key);
    if (existing) return existing;

    // built-in register table: view "register", params {topic} — SNAP-only (§9)
    if (view === "register") {
      const topic = (params as { topic?: string } | null)?.topic;
      if (typeof topic !== "string")
        throw new SeqscribeError("ERR_UNKNOWN_VIEW", "register needs {topic}");
      if (this.deps.topics.get(topic).policy.kind !== "register" || !this.deps.registers)
        throw new SeqscribeError("ERR_UNKNOWN_VIEW", `not a register topic (${topic})`);
      const registers = this.deps.registers;
      const group: Group = {
        key: this.registerKey(topic),
        viewName: null,
        ringTopic: topic, // reuses the ring slot: "the topic this group serves"
        epoch: this.mintEpoch(),
        deltaSeq: 0,
        journal: [],
        subs: new Map(),
        rowsProvider: () => registers.tableRowsSorted(topic) as Row[],
        snapCache: null,
      };
      this.groups.set(group.key, group);
      return group;
    }

    // built-in ring/full tail: view "tail", params {topic}. Ring topics
    // serve their in-memory tail (§14: no durable row). `full`-retention
    // subscribe-only topics (e.g. `session.<id>.transcript` under G2b) serve
    // the last FULL_TAIL_DEFAULT durable rows the SAME way — identical group
    // shape, identical wire messages (SNAP/DELTA/Row), identical cursor-resume
    // and epoch-reset semantics — so TranscriptReplicaStore-shaped consumers
    // need zero wire-format change, only the topic's retention policy switch.
    // `full-sync` and `register` topics are NOT served here: full-sync live
    // delivery is the sync engine's push path (peers replicate the log
    // itself), and a second "tail" delivery path would double-deliver;
    // register topics have their own built-in "register" group above.
    if (view === "tail") {
      const topic = (params as { topic?: string } | null)?.topic;
      if (typeof topic !== "string") throw new SeqscribeError("ERR_UNKNOWN_VIEW", "tail needs {topic}");
      const policy = this.deps.topics.get(topic).policy;
      const mode = policy.retention.mode;
      if (mode !== "ring" && mode !== "full")
        throw new SeqscribeError(
          "ERR_UNKNOWN_VIEW",
          `tail serves ring or full subscribe-only topics only (${topic})`,
        );
      if (mode === "full" && policy.replication !== "subscribe-only")
        throw new SeqscribeError(
          "ERR_UNKNOWN_VIEW",
          `tail on a full-retention topic requires subscribe-only replication (${topic})`,
        );
      let epoch = this.ringEpochs.get(topic);
      if (epoch === undefined) {
        epoch = this.mintEpoch(); // restart = new epoch → SNAP reset (§9)
        this.ringEpochs.set(topic, epoch);
      }
      const defaultLimit =
        mode === "ring"
          ? (policy.retention as { size?: number }).size ?? this.deps.constants.RING_DEFAULT
          : FULL_TAIL_DEFAULT;
      // A keyed topic's state is its newest row per key, which no fixed
      // window of recent rows can hold once live keys outnumber the window —
      // its default SNAP is the newest-per-key set instead (host-guide §4.7).
      const keyed = policy.keyed !== undefined;
      const defaultRows =
        mode === "ring"
          ? () => this.deps.core.ringTail(topic)
          : keyed
            ? () => this.deps.core.latestPerKey(topic, null).map((r) => r.entry)
            : () => this.deps.core.fullTail(topic, FULL_TAIL_DEFAULT);
      const rowsProvider = () =>
        (this.selectTail(topic, mode, defaultLimit, keyed) ?? defaultRows()).map((e) => this.ringRow(e));
      const group: Group = {
        key: this.ringKey(topic),
        viewName: null,
        ringTopic: topic,
        epoch,
        deltaSeq: 0,
        journal: [],
        subs: new Map(),
        rowsProvider,
        snapCache: null,
      };
      this.groups.set(group.key, group);
      return group;
    }

    let handle: ViewHandle;
    const family = this.families.get(view);
    if (family) handle = family(params);
    else if (this.deps.views.has(view) && (params === null || params === undefined))
      handle = { name: view } as ViewHandle; // concrete view, no params
    else throw new SeqscribeError("ERR_UNKNOWN_VIEW", view);

    const meta = this.deps.views.get(handle.name);
    const group: Group = {
      key,
      viewName: handle.name,
      ringTopic: null,
      epoch: meta.epoch,
      deltaSeq: 0,
      journal: [],
      subs: new Map(),
      rowsProvider: () => this.deps.views.tableRowsSorted(handle.name),
      snapCache: null,
    };
    this.groups.set(key, group);
    const list = this.groupsByView.get(handle.name) ?? [];
    list.push(group);
    this.groupsByView.set(handle.name, list);
    return group;
  }

  private selectTail(
    topic: Topic,
    mode: "ring" | "full",
    defaultLimit: number,
    keyed: boolean,
  ): LogEntry[] | null {
    const sel = this.tailSelector;
    if (!sel) return null;
    const core = this.deps.core;
    try {
      return (
        sel({
          topic,
          retention: mode,
          defaultLimit,
          page: (beforeRowid, limit) => core.tailPage(topic, mode === "ring", beforeRowid, limit),
          keyed,
          latestPerKey: (uptoRowid) => core.latestPerKey(topic, uptoRowid),
          rowsAfter: (rowid) => core.rowsAfter(topic, rowid),
          keyHead: (key) => core.keyHead(topic, key),
        }) ?? null
      );
    } catch {
      return null; // a faulty selector degrades to the default window, never to no SNAP
    }
  }

  private ringKey(topic: Topic): string {
    return `tail\u0000${jcs({ topic })}`;
  }

  private ringRow(e: LogEntry): Row {
    return {
      key: `${e.writer}:${e.seq}`,
      writer: e.writer,
      seq: e.seq,
      hlc_l: e.hlc.l,
      hlc_c: e.hlc.c,
      kind: e.kind,
      payload: JSON.stringify(e.payload),
    };
  }

  private addSubscriber(group: Group, session: Session, subId: number): Serve {
    const existing = this.serves.get(session)?.get(subId);
    if (existing && existing.group === group) return existing;
    if (existing) this.removeServe(existing); // subId re-bound to a different view
    const serve: Serve = {
      group,
      session,
      subId,
      pending: false,
      snap: null,
      timer: null,
      backoffMs: 0,
      closed: false,
    };
    const inGroup = group.subs.get(session) ?? new Map<number, Serve>();
    inGroup.set(subId, serve);
    group.subs.set(session, inGroup);
    const bySub = this.serves.get(session) ?? new Map<number, Serve>();
    bySub.set(subId, serve);
    this.serves.set(session, bySub);
    return serve;
  }

  private removeServe(serve: Serve): void {
    serve.closed = true;
    if (serve.timer !== null) {
      this.deps.timers.clearTimeout(serve.timer);
      serve.timer = null;
    }
    const inGroup = serve.group.subs.get(serve.session);
    if (inGroup?.get(serve.subId) === serve) {
      inGroup.delete(serve.subId);
      if (inGroup.size === 0) serve.group.subs.delete(serve.session);
    }
    if (serve.group.subs.size === 0) serve.group.snapCache = null;
    const bySub = this.serves.get(serve.session);
    if (bySub?.get(serve.subId) === serve) {
      bySub.delete(serve.subId);
      if (bySub.size === 0) this.serves.delete(serve.session);
    }
  }

  private *servesOf(group: Group): Iterable<Serve> {
    for (const inGroup of [...group.subs.values()]) yield* [...inGroup.values()];
  }

  private onViewChange(c: ViewChange): void {
    for (const group of this.groupsByView.get(c.view) ?? []) {
      if (c.reset) {
        // materialization revision: new epoch, journal invalid, everyone re-SNAPs
        group.epoch = c.epoch;
        group.deltaSeq = 0;
        group.journal = [];
        this.resetGroup(group);
      } else {
        this.publish(group, { upserts: c.upserts, deletes: c.deletes });
      }
    }
  }

  // Epoch reset: any SNAP still being paced carries a dead cursor — abandon it
  // (the client discards a partial reassembly when the cursor changes) and
  // resync everyone from the new epoch.
  private resetGroup(group: Group): void {
    group.snapCache = null;
    for (const serve of this.servesOf(group)) {
      if (serve.snap) {
        serve.snap = null;
        this.counters.snapsAbandoned++;
      }
      serve.pending = true;
      this.tryStartSnap(serve);
    }
  }

  // compute once, broadcast within the group (§10)
  private publish(group: Group, changes: { upserts: Row[]; deletes: string[] }): void {
    group.deltaSeq++;
    group.snapCache = null;
    const j: JournalEntry = {
      seq: group.deltaSeq,
      changes,
      bytes: utf8ByteLength(JSON.stringify(changes)),
    };
    group.journal.push(j);
    if (group.journal.length > this.deps.constants.SUB_DELTA_RETAIN) group.journal.shift();
    for (const serve of this.servesOf(group)) this.deliverDelta(serve, j);
  }

  // Worst-case envelope around `changes` in a DELTA frame: the fixed keys,
  // a mid of up to 16 digits, a subId, and the cursor. Over-estimating only
  // means a DELTA within a few dozen bytes of MAX_FRAME_BYTES resyncs instead.
  private deltaFrameBytes(j: JournalEntry, cursor: string): number {
    return j.bytes + utf8ByteLength(cursor) + 96;
  }

  private deliverDelta(serve: Serve, j: JournalEntry): void {
    if (serve.pending || serve.snap) {
      // a resync is already owed/under way; its SNAP + journal replay covers this
      this.counters.resyncWritesCoalesced++;
      return;
    }
    const cursor = encodeCursor({ e: serve.group.epoch, d: j.seq });
    // DELTA is never chunked — an oversized delta downgrades this subscriber
    // to SNAP(reset), coalesced like any other resync
    if (this.deltaFrameBytes(j, cursor) > this.deps.constants.MAX_FRAME_BYTES) {
      this.enterResync(serve, "oversized");
      return;
    }
    const subId = serve.subId;
    const changes = j.changes;
    const ok = serve.session.sendData((mid): MsgDelta => ({ t: "DELTA", mid, subId, changes, cursor }));
    if (!ok) {
      this.enterResync(serve, "backpressure"); // tail-dropped → resync (once)
      return;
    }
    this.counters.deltasSent++;
  }

  private enterResync(serve: Serve, reason: SubResyncReason): void {
    if (serve.pending || serve.snap) {
      this.counters.resyncWritesCoalesced++;
      return;
    }
    serve.pending = true;
    this.counters.resyncs++;
    if (reason === "backpressure") this.counters.resyncsBackpressure++;
    else this.counters.resyncsOversized++;
    const g = serve.group;
    const topic = g.ringTopic ?? this.deps.views.get(g.viewName!).topic;
    this.deps.emitAnomaly?.({
      kind: "sub_resync",
      topic,
      peerId: serve.session.peerId,
      view: g.viewName ?? (g.key.startsWith("register\u0000") ? "register" : "tail"),
      reason,
    });
    // Deferred, never inline: every write applied in the same synchronous
    // flush lands on the `pending` flag instead of generating its own SNAP.
    this.schedule(serve, 0);
  }

  // Room to START a SNAP: the data lane has drained to half its cap. Pacing
  // (pumpSnap) then keeps at most that many of this SNAP's chunks queued, so
  // a large body never tail-drops its own chunks and other traffic keeps the
  // remaining headroom.
  private snapWindow(): number {
    return Math.max(1, Math.floor(this.deps.constants.SEND_QUEUE_CAP / 2));
  }

  private hasRoom(session: Session): boolean {
    return session.hasSendCapacity() && session.queuedData() < this.snapWindow();
  }

  private schedule(serve: Serve, delayMs: number): void {
    if (serve.timer !== null || serve.closed) return;
    serve.timer = this.deps.timers.setTimeout(() => {
      serve.timer = null;
      if (serve.closed) return;
      if (serve.snap) this.pumpSnap(serve);
      else if (serve.pending) this.tryStartSnap(serve);
    }, delayMs);
  }

  // Backoff for when no drain signal arrives (handleCapacity is the fast
  // path): 50 ms doubling to CONTROL_RETRY_MS. A peer that never drains is
  // closed by the §5.2 stall check, which tears this state down.
  private backoff(serve: Serve): void {
    serve.backoffMs = Math.min(
      Math.max(50, serve.backoffMs * 2),
      Math.max(50, this.deps.constants.CONTROL_RETRY_MS),
    );
    this.schedule(serve, serve.backoffMs);
  }

  private tryStartSnap(serve: Serve): void {
    if (serve.closed || !serve.pending || serve.snap) return;
    if (!this.hasRoom(serve.session)) {
      this.backoff(serve);
      return;
    }
    if (serve.timer !== null) {
      this.deps.timers.clearTimeout(serve.timer);
      serve.timer = null;
    }
    const g = serve.group;
    let body: Uint8Array;
    const cached = g.snapCache;
    if (cached && cached.epoch === g.epoch && cached.seq === g.deltaSeq) {
      body = cached.body;
      this.counters.snapCacheHits++;
    } else {
      let rows: Row[];
      try {
        rows = g.rowsProvider();
      } catch {
        serve.pending = false;
        serve.session.sendControl({ t: "SUB_ERR", subId: serve.subId, code: "ERR_STORAGE" });
        return;
      }
      body = textEnc.encode(jcs(rows as unknown as JsonValue));
      g.snapCache = { epoch: g.epoch, seq: g.deltaSeq, body };
    }
    const rawBudget = this.rawChunkBudget();
    serve.pending = false;
    serve.snap = {
      body,
      of: Math.max(1, Math.ceil(body.length / rawBudget)),
      next: 1,
      cursor: encodeCursor({ e: g.epoch, d: g.deltaSeq }),
      epoch: g.epoch,
      seq: g.deltaSeq,
    };
    this.counters.snapsStarted++;
    this.counters.snapBytes += body.length;
    this.pumpSnap(serve);
  }

  private rawChunkBudget(): number {
    return Math.floor((this.deps.constants.MAX_FRAME_BYTES / 2) * 0.75);
  }

  // Enqueue this SNAP's next chunks while the lane has room. base64 is done
  // per chunk as it is enqueued, so a large body is encoded across ACK-driven
  // turns rather than in one blocking pass.
  private pumpSnap(serve: Serve): void {
    const s = serve.snap;
    if (!s || serve.closed) return;
    const rawBudget = this.rawChunkBudget();
    const subId = serve.subId;
    while (s.next <= s.of && this.hasRoom(serve.session)) {
      const chunk = s.next;
      const data = b64encode(s.body.subarray((chunk - 1) * rawBudget, chunk * rawBudget));
      const ok = serve.session.sendData(
        (mid): MsgSnap => ({
          t: "SNAP",
          mid,
          subId,
          chunk,
          of: s.of,
          data,
          cursor: s.cursor,
          reset: true,
        }),
      );
      if (!ok) break;
      s.next++;
      this.counters.snapChunksSent++;
    }
    if (s.next <= s.of) {
      this.backoff(serve); // resumes on handleCapacity, or on this timer
      return;
    }
    serve.snap = null;
    serve.backoffMs = 0;
    this.counters.snapsCompleted++;
    this.catchUp(serve, s);
  }

  // After a SNAP is fully enqueued: deltas published while it was paced are
  // replayed from the journal (the data lane is ordered, so they land after
  // the SNAP). A journal that no longer reaches back is another resync.
  private catchUp(serve: Serve, s: SnapOut): void {
    const g = serve.group;
    if (g.epoch !== s.epoch) {
      serve.pending = true;
      this.schedule(serve, 0);
      return;
    }
    if (g.deltaSeq === s.seq) return;
    const oldest = g.journal[0]?.seq ?? g.deltaSeq + 1;
    if (s.seq + 1 < oldest) {
      this.enterResync(serve, "backpressure");
      return;
    }
    for (const j of g.journal) {
      if (j.seq <= s.seq) continue;
      this.deliverDelta(serve, j);
      if (serve.pending || serve.snap) return;
    }
  }

  // ---- client ----

  subscribe(
    session: Session,
    o: { view: string; params: JsonValue; fromCursor?: string | undefined },
  ): Subscription {
    const subId = this.nextSubId++;
    const sub: ClientSub = {
      session,
      subId,
      cursor: o.fromCursor,
      snapshotCbs: new Set(),
      deltaCbs: new Set(),
      view: o.view,
      params: o.params,
      chunks: new Map(),
      chunksOf: 0,
      chunkBytes: 0,
      chunkCursor: "",
      chunkReset: false,
      closed: false,
    };
    this.clientSubs.set(`${session.peerId} ${subId}`, sub);
    this.sendSubRequest(sub);
    const self = this;
    return {
      onSnapshot(cb): Unsub {
        sub.snapshotCbs.add(cb);
        return () => sub.snapshotCbs.delete(cb);
      },
      onDelta(cb): Unsub {
        sub.deltaCbs.add(cb);
        return () => sub.deltaCbs.delete(cb);
      },
      get cursor(): string | undefined {
        return sub.cursor;
      },
      set cursor(_v: string | undefined) {
        throw misuse("cursor is read-only");
      },
      close(): void {
        if (sub.closed) return;
        sub.closed = true;
        self.clientSubs.delete(`${session.peerId} ${subId}`);
        session.satisfyRequest(`SUB:${subId}`);
        session.sendControl({ t: "UNSUB", subId });
      },
    };
  }

  private sendSubRequest(sub: ClientSub): void {
    sub.session.request(`SUB:${sub.subId}`, (): MsgSub => {
      const m: MsgSub = { t: "SUB", subId: sub.subId, view: sub.view, params: sub.params };
      if (sub.cursor !== undefined) m.fromCursor = sub.cursor;
      return m;
    });
  }

  handleSnap(session: Session, m: MsgSnap): void {
    const sub = this.clientSubs.get(`${session.peerId} ${m.subId}`);
    if (!sub) return;
    session.satisfyRequest(`SUB:${m.subId}`);
    if (m.of !== sub.chunksOf || m.cursor !== sub.chunkCursor) {
      sub.chunks.clear();
      sub.chunkBytes = 0;
      sub.chunksOf = m.of;
      sub.chunkCursor = m.cursor;
      sub.chunkReset = m.reset;
    }
    // MAX_REASSEMBLY_BYTES (proposals-v3.5 P7): chunk indices sit in [1, of]
    // (parseMsg) but `of` is peer-chosen, so per-frame caps alone leave this
    // map unbounded. Delta-aware accounting — retransmits overwrite, they don't
    // double-count. Overflow is a protocol violation, not congestion: same
    // ERR + close discipline as the §5.2 credit-window bound (state resets on
    // redial, so a buggy-but-honest peer recovers).
    sub.chunkBytes += m.data.length - (sub.chunks.get(m.chunk)?.length ?? 0);
    if (sub.chunkBytes > this.deps.constants.MAX_REASSEMBLY_BYTES) {
      sub.chunks.clear();
      sub.chunksOf = 0;
      sub.chunkBytes = 0;
      session.sendControl({
        t: "ERR",
        code: session.violationCode(), // P38
        detail: `SNAP reassembly exceeds MAX_REASSEMBLY_BYTES (subId ${m.subId})`,
      });
      session.close("protocol");
      return;
    }
    sub.chunks.set(m.chunk, m.data);
    if (sub.chunks.size < m.of) return;
    const parts: Uint8Array[] = [];
    for (let i = 1; i <= m.of; i++) parts.push(b64decode(sub.chunks.get(i) ?? ""));
    const total = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let off = 0;
    for (const p of parts) {
      total.set(p, off);
      off += p.length;
    }
    sub.chunks.clear();
    sub.chunksOf = 0;
    sub.chunkBytes = 0;
    // The reassembled body is peer-supplied: it must be a JCS row array (§5.4)
    // before it reaches subscriber callbacks. A bad body throws — the Session
    // dispatch guard turns that into ERR + drop instead of a host-visible crash.
    const rows = JSON.parse(textDec.decode(total)) as Row[];
    if (!Array.isArray(rows))
      throw new SeqscribeError("ERR_ENTRY_ENCODING", "SNAP body is not a row array");
    sub.cursor = m.cursor;
    for (const cb of sub.snapshotCbs) cb(rows, sub.chunkReset);
  }

  handleDelta(session: Session, m: MsgDelta): void {
    const sub = this.clientSubs.get(`${session.peerId} ${m.subId}`);
    if (!sub) return;
    session.satisfyRequest(`SUB:${m.subId}`);
    sub.cursor = m.cursor;
    for (const cb of sub.deltaCbs) cb(m.changes);
  }

  handleSubErr(session: Session, m: MsgSubErr): void {
    const sub = this.clientSubs.get(`${session.peerId} ${m.subId}`);
    if (!sub) return;
    if (m.code === "ERR_FUTURE_CURSOR") {
      // §6 case ①: discard the cursor and SUB fresh
      sub.cursor = undefined;
      this.sendSubRequest(sub);
      return;
    }
    session.satisfyRequest(`SUB:${sub.subId}`);
  }

  private mintEpoch(): string {
    let s = "";
    for (let i = 0; i < 4; i++)
      s += Math.floor(this.deps.rng() * 0x10000)
        .toString(16)
        .padStart(4, "0");
    return s;
  }
}
