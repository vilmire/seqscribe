// SPEC §10, §5.4 SUB/SNAP/DELTA — Tier-2 subscriptions. Server: serving groups
// per (view, JCS(params)) with an in-memory delta journal, "compute once,
// broadcast", SNAP with byte-level chunking, DELTA-overflow downgrade. Client:
// subscribe/resume with opaque {epoch, deltaSeq} cursors.

import { jcs, utf8ByteLength } from "./encoding.js";
import { misuse, SeqscribeError } from "./errors.js";
import type { LogCore } from "./log.js";
import type { RegisterHub } from "./register.js";
import type { MsgDelta, MsgSnap, MsgSub, MsgSubErr, MsgUnsub } from "./messages.js";
import type { Session } from "./session.js";
import type { TopicRegistry } from "./topics.js";
import type {
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
const B64REV = new Map([...B64].map((c, i) => [c, i] as const));

export function b64encode(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const a = bytes[i]!;
    const b = bytes[i + 1];
    const c = bytes[i + 2];
    out += B64[a >> 2]! + B64[((a & 3) << 4) | ((b ?? 0) >> 4)]!;
    out += b === undefined ? "=" : B64[((b & 15) << 2) | ((c ?? 0) >> 6)]!;
    out += c === undefined ? "=" : B64[c & 63]!;
  }
  return out;
}

export function b64decode(s: string): Uint8Array {
  const clean = s.replace(/=+$/, "");
  const out = new Uint8Array(Math.floor((clean.length * 3) / 4));
  let o = 0;
  for (let i = 0; i < clean.length; i += 4) {
    const n =
      ((B64REV.get(clean[i]!) ?? 0) << 18) |
      ((B64REV.get(clean[i + 1] ?? "A") ?? 0) << 12) |
      ((B64REV.get(clean[i + 2] ?? "A") ?? 0) << 6) |
      (B64REV.get(clean[i + 3] ?? "A") ?? 0);
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

interface Group {
  key: string;
  viewName: string | null; // null for ring tail groups
  ringTopic: Topic | null;
  epoch: string;
  deltaSeq: number;
  journal: { seq: number; changes: { upserts: Row[]; deletes: string[] } }[];
  subs: Map<Session, Set<number>>; // subId set per session
  rowsProvider: () => Row[];
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

export interface SubHubDeps {
  views: ViewHub;
  core: LogCore;
  topics: TopicRegistry;
  constants: Constants;
  timers: Timers;
  rng: () => number;
  registers?: RegisterHub | undefined;
}

export class SubHub {
  private readonly families = new Map<string, (params: JsonValue) => ViewHandle>();
  private readonly groups = new Map<string, Group>();
  private readonly groupsByView = new Map<string, Group[]>();
  private readonly ringEpochs = new Map<Topic, string>();
  private readonly clientSubs = new Map<string, ClientSub>(); // `${peerId} ${subId}`
  private nextSubId = 1;

  constructor(private readonly deps: SubHubDeps) {
    deps.views.onViewChange((c) => this.onViewChange(c));
  }

  // ---- server: registration ----

  serveView(name: string, resolver: (params: JsonValue) => ViewHandle): void {
    if (this.families.has(name)) throw misuse(`serveView name already registered: ${name}`);
    this.families.set(name, resolver);
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

    const cursor = m.fromCursor !== undefined ? decodeCursor(m.fromCursor) : null;
    if (cursor && cursor.e === group.epoch) {
      if (cursor.d > group.deltaSeq) {
        session.sendControl({ t: "SUB_ERR", subId: m.subId, code: "ERR_FUTURE_CURSOR" });
        return;
      }
      const oldest = group.journal[0]?.seq ?? group.deltaSeq + 1;
      if (cursor.d + 1 >= oldest) {
        // resumable: register, replay missed deltas from the journal
        this.addSubscriber(group, session, m.subId);
        for (const j of group.journal) {
          if (j.seq > cursor.d) this.sendDelta(group, session, m.subId, j.changes, j.seq);
        }
        return;
      }
    }
    // fresh or beyond retention or epoch mismatch → SNAP reset
    this.addSubscriber(group, session, m.subId);
    this.sendSnap(group, session, m.subId, true);
  }

  handleUnsub(session: Session, m: MsgUnsub): void {
    for (const group of this.groups.values()) {
      const set = group.subs.get(session);
      if (set?.delete(m.subId) && set.size === 0) group.subs.delete(session);
    }
  }

  handleSessionClosed(session: Session): void {
    for (const group of this.groups.values()) group.subs.delete(session);
    for (const [key, sub] of [...this.clientSubs]) {
      if (sub.session === session) {
        sub.closed = true;
        this.clientSubs.delete(key);
      }
    }
  }

  // register materialization rewrote the built-in table — SNAP-reset the group
  handleRegisterChanged(topic: Topic): void {
    const group = this.groups.get(this.registerKey(topic));
    if (!group) return;
    group.epoch = this.mintEpoch();
    group.deltaSeq = 0;
    group.journal = [];
    for (const [session, subIds] of group.subs) {
      for (const subId of subIds) this.sendSnap(group, session, subId, true);
    }
  }

  private registerKey(topic: Topic): string {
    return `register\u0000${jcs({ topic })}`;
  }

  // ring topic entries feed their topic's tail groups (rowid-null applies)
  handleRingApplied(e: LogEntry): void {
    const group = this.groups.get(this.ringKey(e.topic));
    if (!group) return;
    const row = this.ringRow(e);
    this.publish(group, { upserts: [row], deletes: [] });
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
      };
      this.groups.set(group.key, group);
      return group;
    }

    // built-in ring tail: view "tail", params {topic}
    if (view === "tail") {
      const topic = (params as { topic?: string } | null)?.topic;
      if (typeof topic !== "string") throw new SeqscribeError("ERR_UNKNOWN_VIEW", "tail needs {topic}");
      const policy = this.deps.topics.get(topic).policy;
      if (policy.retention.mode !== "ring")
        throw new SeqscribeError("ERR_UNKNOWN_VIEW", `tail serves ring topics only (${topic})`);
      let epoch = this.ringEpochs.get(topic);
      if (epoch === undefined) {
        epoch = this.mintEpoch(); // restart = new epoch → SNAP reset (§9)
        this.ringEpochs.set(topic, epoch);
      }
      const group: Group = {
        key: this.ringKey(topic),
        viewName: null,
        ringTopic: topic,
        epoch,
        deltaSeq: 0,
        journal: [],
        subs: new Map(),
        rowsProvider: () => this.deps.core.ringTail(topic).map((e) => this.ringRow(e)),
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
    };
    this.groups.set(key, group);
    const list = this.groupsByView.get(handle.name) ?? [];
    list.push(group);
    this.groupsByView.set(handle.name, list);
    return group;
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

  private addSubscriber(group: Group, session: Session, subId: number): void {
    const set = group.subs.get(session) ?? new Set<number>();
    set.add(subId);
    group.subs.set(session, set);
  }

  private onViewChange(c: ViewChange): void {
    for (const group of this.groupsByView.get(c.view) ?? []) {
      if (c.reset) {
        // materialization revision: new epoch, journal invalid, everyone re-SNAPs
        group.epoch = c.epoch;
        group.deltaSeq = 0;
        group.journal = [];
        for (const [session, subIds] of group.subs) {
          for (const subId of subIds) this.sendSnap(group, session, subId, true);
        }
      } else {
        this.publish(group, { upserts: c.upserts, deletes: c.deletes });
      }
    }
  }

  // compute once, broadcast within the group (§10)
  private publish(group: Group, changes: { upserts: Row[]; deletes: string[] }): void {
    group.deltaSeq++;
    group.journal.push({ seq: group.deltaSeq, changes });
    if (group.journal.length > this.deps.constants.SUB_DELTA_RETAIN) group.journal.shift();
    for (const [session, subIds] of group.subs) {
      for (const subId of subIds) this.sendDelta(group, session, subId, changes, group.deltaSeq);
    }
  }

  private sendDelta(
    group: Group,
    session: Session,
    subId: number,
    changes: { upserts: Row[]; deletes: string[] },
    seq: number,
  ): void {
    const cursor = encodeCursor({ e: group.epoch, d: seq });
    const probe: MsgDelta = { t: "DELTA", mid: 0, subId, changes, cursor };
    // DELTA is never chunked — oversized deltas downgrade the group to SNAP(reset)
    if (utf8ByteLength(JSON.stringify(probe)) > this.deps.constants.MAX_FRAME_BYTES) {
      this.sendSnap(group, session, subId, true);
      return;
    }
    const ok = session.sendData((mid): MsgDelta => ({ ...probe, mid }));
    if (!ok) this.sendSnap(group, session, subId, true); // tail-dropped → resync
  }

  private sendSnap(group: Group, session: Session, subId: number, reset: boolean): void {
    let rows: Row[];
    try {
      rows = group.rowsProvider();
    } catch {
      session.sendControl({ t: "SUB_ERR", subId, code: "ERR_STORAGE" });
      return;
    }
    const cursor = encodeCursor({ e: group.epoch, d: group.deltaSeq });
    const body = textEnc.encode(jcs(rows as unknown as JsonValue));
    const rawBudget = Math.floor((this.deps.constants.MAX_FRAME_BYTES / 2) * 0.75);
    const of = Math.max(1, Math.ceil(body.length / rawBudget));
    const totalHash = undefined; // single-frame SNAPs skip totalHash; multi set below
    void totalHash;
    for (let chunk = 1; chunk <= of; chunk++) {
      const slice = body.subarray((chunk - 1) * rawBudget, chunk * rawBudget);
      const data = b64encode(slice);
      session.sendData(
        (mid): MsgSnap => ({
          t: "SNAP",
          mid,
          subId,
          chunk,
          of,
          data,
          cursor,
          reset,
        }),
      );
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
        code: "ERR_ENTRY_ENCODING",
        detail: `SNAP reassembly exceeds MAX_REASSEMBLY_BYTES (subId ${m.subId})`,
      });
      session.close();
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
