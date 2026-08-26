// SPEC §6 Tier-1 synchronization — the per-node engine coordinating peer
// sessions: HAVE rounds (request/response with captured pages), WANT/ENTRIES
// deficits, eager push + bounded relay, HAVE-contradiction fork detection, and
// PROBE service for fork evidence.

import { validateEntry } from "./codec.js";
import { utf8ByteLength } from "./encoding.js";
import { SeqscribeError } from "./errors.js";
import type { LogCore } from "./log.js";
import type {
  ControlMsg,
  MsgDelta,
  MsgEntries,
  MsgHave,
  MsgHaveGet,
  MsgProbe,
  MsgProbeRes,
  MsgSnap,
  MsgSnapshot,
  MsgWant,
} from "./messages.js";
import { Session } from "./session.js";
import type { DirectiveHub } from "./directives.js";
import type { FinalityHub } from "./finality.js";
import type { SnapshotHub } from "./snapshot.js";
import type { SubHub } from "./subs.js";
import type { Store } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type {
  Anomaly,
  Channel,
  Constants,
  HaveVectors,
  JsonValue,
  LogEntry,
  PeerHandle,
  Seq,
  Timers,
  Topic,
  Unsub,
  WriterId,
} from "./types.js";

const MAX_WANT_CONCURRENT = 4; // §6.2
const ENTRIES_BATCH_MAX = 100;

interface WantState {
  req: number;
  topic: Topic;
  writer: WriterId;
  fromSeq: Seq;
}

interface PeerState {
  session: Session;
  lastActivity: number;
  haveReq: number;
  wantReq: number;
  expectedHaveReq: number | null;
  havePages: Map<number, MsgHave>;
  captured: { req: number; pages: MsgHave[] } | null;
  activeWants: Map<number, WantState>;
  wantQueue: { topic: Topic; writer: WriterId }[];
  wantedStreams: Set<string>;
  recoveryProgress: Map<string, number>;
  antiEntropyTimer: unknown;
  probes: Map<string, (res: MsgProbeRes) => void>;
  // last-known peer stream heads (from HAVE rounds, received ENTRIES, and our
  // accepted pushes) — the targeting basis for knowledge-based push
  known: Map<string, Seq>;
  // streams where our contig may exceed the peer's known head; drained by a
  // capacity-aware pump into batched ENTRIES pushes
  dirty: Map<string, { topic: Topic; writer: WriterId; readyAt: number }>;
  pumpTimer: unknown;
}

export interface SyncEngineOpts {
  core: LogCore;
  store: Store;
  topics: TopicRegistry;
  writerId: WriterId;
  constants: Constants;
  timers: Timers;
  clock: () => number;
  emitAnomaly: (a: Anomaly) => void;
}

export class SyncEngine {
  private readonly o: SyncEngineOpts;
  private readonly peers = new Map<Session, PeerState>();
  private finalityHub: FinalityHub | null = null;
  private subHub: SubHub | null = null;
  private directiveHub: DirectiveHub | null = null;
  private snapshotHub: SnapshotHub | null = null;

  constructor(opts: SyncEngineOpts) {
    this.o = opts;
  }

  setSubHub(hub: SubHub): void {
    this.subHub = hub;
  }

  setDirectiveHub(hub: DirectiveHub): void {
    this.directiveHub = hub;
    hub.setBroadcast((d) => {
      for (const ps of this.peers.values()) {
        if (ps.session.state() === "ready" && ps.session.mutualFull(d.topic))
          ps.session.sendControl({ t: "WRITER_DIRECTIVE", directive: d });
      }
    });
    // a directive that opens a recovery needs the canonical range — go look for it
    hub.setOnApplied((d, result) => {
      if (result !== "recovery") return;
      for (const ps of this.peers.values()) {
        if (ps.session.state() === "ready" && ps.session.mutualFull(d.topic))
          this.queueWant(ps, d.topic, d.writer);
      }
    });
  }

  subscribe(
    peerId: string,
    o: { view: string; params: JsonValue; fromCursor?: string | undefined },
  ) {
    if (!this.subHub) throw new SeqscribeError("ERR_MISUSE", "subscriptions unavailable");
    for (const ps of this.peers.values()) {
      if (ps.session.peerId === peerId && ps.session.state() !== "closed")
        return this.subHub.subscribe(ps.session, o);
    }
    throw new SeqscribeError("ERR_MISUSE", `no open session for peer ${peerId}`);
  }

  setSnapshotHub(hub: SnapshotHub): void {
    this.snapshotHub = hub;
    // adoption changes the basis — refresh deficits right away
    hub.setOnAdopted(() => {
      for (const ps of this.peers.values()) {
        if (ps.session.state() === "ready") this.startHaveRound(ps);
      }
    });
  }

  // a newly accepted cert unblocks §7.8-deferred WANTs — refresh HAVE rounds
  onCertAccepted(): void {
    for (const ps of this.peers.values()) {
      if (ps.session.state() === "ready") this.startHaveRound(ps);
    }
  }

  setFinalityHub(hub: FinalityHub): void {
    this.finalityHub = hub;
    hub.setBroadcast((topic, cert) => {
      for (const ps of this.peers.values()) {
        if (ps.session.state() === "ready" && ps.session.mutualFull(topic))
          ps.session.sendControl({ t: "FINALITY", topic, cert });
      }
    });
  }

  attach(
    ch: Channel,
    o: {
      peerId: string;
      peerClass: "content" | "metadata";
      grants: Record<Topic, "full" | "serve" | "none">;
    },
  ): PeerHandle {
    const session = new Session({
      channel: ch,
      peerId: o.peerId,
      peerClass: o.peerClass,
      grants: o.grants,
      localNode: this.o.writerId,
      topics: this.o.topics,
      constants: this.o.constants,
      timers: this.o.timers,
      clock: this.o.clock,
      onReady: (s) => this.onReady(s),
      onClose: (s) => this.onClosed(s),
      onControl: (s, m) => this.onControl(s, m),
      onData: (s, m) => this.onData(s, m),
      onCapacity: (s) => {
        const ps2 = this.peers.get(s);
        if (ps2) this.pumpDirty(ps2);
      },
    });
    const ps: PeerState = {
      session,
      lastActivity: this.o.clock(),
      haveReq: 0,
      wantReq: 0,
      expectedHaveReq: null,
      havePages: new Map(),
      captured: null,
      activeWants: new Map(),
      wantQueue: [],
      wantedStreams: new Set(),
      recoveryProgress: new Map(),
      antiEntropyTimer: null,
      probes: new Map(),
      known: new Map(),
      dirty: new Map(),
      pumpTimer: null,
    };
    this.peers.set(session, ps);
    return {
      peerId: o.peerId,
      state: () => session.state(),
      onStateChange: (cb): Unsub => session.onStateChange(cb),
      detach: () => session.close(),
    };
  }

  closeAll(): void {
    for (const s of [...this.peers.keys()]) s.close();
  }

  peerStats(): { peerId: string; state: string; dirtyStreams: number; queuedData: number }[] {
    return [...this.peers.values()].map((ps) => ({
      peerId: ps.session.peerId,
      state: ps.session.state(),
      dirtyStreams: ps.dirty.size,
      queuedData: ps.session.queuedData(),
    }));
  }

  // ---- lifecycle ----

  private onReady(s: Session): void {
    const ps = this.peers.get(s);
    if (!ps) return;
    ps.lastActivity = this.o.clock();
    this.startHaveRound(ps);
    const tick = () => {
      if (s.state() !== "ready") return;
      this.startHaveRound(ps);
      ps.antiEntropyTimer = this.o.timers.setTimeout(tick, this.o.constants.ANTI_ENTROPY_MS);
    };
    ps.antiEntropyTimer = this.o.timers.setTimeout(tick, this.o.constants.ANTI_ENTROPY_MS);
  }

  private onClosed(s: Session): void {
    const ps = this.peers.get(s);
    if (ps?.antiEntropyTimer != null) this.o.timers.clearTimeout(ps.antiEntropyTimer);
    if (ps?.pumpTimer != null) this.o.timers.clearTimeout(ps.pumpTimer);
    this.subHub?.handleSessionClosed(s);
    this.peers.delete(s);
  }

  // ---- HAVE round (requester side) ----

  private startHaveRound(ps: PeerState): void {
    const req = ++ps.haveReq;
    ps.expectedHaveReq = req;
    ps.havePages.clear();
    ps.session.request(`HAVE:${req}`, (): MsgHaveGet => ({ t: "HAVE_GET", req }));
  }

  private onHavePage(ps: PeerState, m: MsgHave): void {
    if (m.req !== ps.expectedHaveReq) return; // stale round
    ps.havePages.set(m.page, m);
    if (ps.havePages.size < m.of) return;
    ps.session.satisfyRequest(`HAVE:${m.req}`);
    ps.expectedHaveReq = null;
    const merged: HaveVectors = {};
    for (const page of ps.havePages.values()) {
      for (const [topic, v] of Object.entries(page.vectors)) {
        const t = (merged[topic] ??= { writers: {} });
        if (v.fgen !== undefined) t.fgen = v.fgen;
        Object.assign(t.writers, v.writers);
      }
    }
    ps.havePages.clear();
    this.processPeerVectors(ps, merged);
  }

  private processPeerVectors(ps: PeerState, vectors: HaveVectors): void {
    for (const [topic, v] of Object.entries(vectors)) {
      if (!this.o.topics.has(topic)) continue;
      if (!ps.session.mutualFull(topic)) continue;
      // fgen lag rule (§7.4): peer behind on finality → push my latest cert
      const myCert = this.o.core.getCert(topic);
      if (myCert && (v.fgen ?? 0) < myCert.generation)
        ps.session.sendControl({ t: "FINALITY", topic, cert: myCert });
      // §7.8 ordering: no WANTs below a topic's cut before holding a verified
      // certificate — with none held and the peer certified, wait for the push
      if (!myCert && (v.fgen ?? 0) > 0) continue;
      for (const [writer, w] of Object.entries(v.writers)) {
        const head = this.o.core.getStream(topic, writer);
        // authoritative knowledge refresh from the peer's own HAVE — writer
        // names are charter-validated at parseMsg (§1 regexes); ps.known growth
        // is bounded only by the peer's real writer census, which sits inside
        // the §4.1 full-sync trust boundary (grants are the containment line)
        ps.known.set(`${topic}\u0000${writer}`, "retired" in w ? w.finalSeq : w.contig);
        const peerRgen = w.rgen ?? 0;
        // rgen lag rule (§13): a peer advertising a lower rgen gets the latest directive
        if (peerRgen < head.rgen && this.directiveHub) {
          const latest = this.directiveHub.latest(topic, writer);
          if (latest && latest.rgen === head.rgen)
            ps.session.sendControl({ t: "WRITER_DIRECTIVE", directive: latest });
        }
        if ("retired" in w) {
          // tombstones are a derived cache — recovery may still pull the
          // canonical range from the tombstone holder
          if (this.o.core.recoveryTarget(topic, writer)) this.queueWant(ps, topic, writer);
          continue;
        }
        if (head.sealReason !== null) {
          if (this.o.core.recoveryTarget(topic, writer)) this.queueWant(ps, topic, writer);
          continue;
        }
        // fork path ②: HAVE contradiction — peer's chain at a seq ≤ my contig
        // disagrees with my verified prefix
        if (w.contig > 0 && w.contig <= head.contigSeq) {
          const myChainAt =
            w.contig === head.contigSeq
              ? head.contigChain
              : this.o.store.getEntry(topic, writer, w.contig)?.chain;
          if (myChainAt !== undefined && myChainAt !== w.chain) {
            void this.o.core.sealStream(topic, writer);
            this.locateFork(ps, topic, writer, Math.min(w.contig, head.contigSeq));
            continue;
          }
        }
        if (w.contig > head.contigSeq) this.queueWant(ps, topic, writer);
        else if (w.contig < head.contigSeq) this.markDirty(ps, topic, writer, 0);
      }
    }
  }

  // ---- knowledge-based push (catch-up gossip) ----

  private markDirty(ps: PeerState, topic: Topic, writer: WriterId, delayMs: number): void {
    if (ps.session.state() !== "ready" || !ps.session.mutualFull(topic)) return;
    const key = `${topic}\u0000${writer}`;
    const readyAt = this.o.clock() + delayMs;
    const existing = ps.dirty.get(key);
    if (existing) existing.readyAt = Math.min(existing.readyAt, readyAt);
    else ps.dirty.set(key, { topic, writer, readyAt });
    this.schedulePump(ps, delayMs);
  }

  private schedulePump(ps: PeerState, delayMs: number): void {
    if (ps.pumpTimer !== null) return;
    ps.pumpTimer = this.o.timers.setTimeout(() => {
      ps.pumpTimer = null;
      this.pumpDirty(ps);
    }, delayMs);
  }

  private pumpDirty(ps: PeerState): void {
    if (ps.session.state() !== "ready") return;
    const now = this.o.clock();
    let earliestFuture: number | null = null;
    for (const [key, d] of [...ps.dirty]) {
      if (d.readyAt > now) {
        earliestFuture = earliestFuture === null ? d.readyAt : Math.min(earliestFuture, d.readyAt);
        continue;
      }
      if (!ps.session.hasSendCapacity()) return; // resume on onCapacity
      const head = this.o.core.getStream(d.topic, d.writer);
      const known = ps.known.get(key) ?? 0;
      if (known >= head.contigSeq) {
        ps.dirty.delete(key);
        continue;
      }
      const budget = Math.floor(this.o.constants.MAX_FRAME_BYTES / 2);
      const rows = this.o.store.entriesRange(
        d.topic,
        d.writer,
        known + 1,
        Math.min(head.contigSeq, known + ENTRIES_BATCH_MAX),
      );
      if (rows.length === 0) {
        ps.dirty.delete(key); // below local retention — the peer bootstraps via snapshot
        continue;
      }
      const batch: LogEntry[] = [];
      let bytes = 0;
      for (const { entry } of rows) {
        const cost = utf8ByteLength(JSON.stringify(entry));
        if (batch.length > 0 && bytes + cost > budget) break;
        batch.push(entry);
        bytes += cost;
      }
      const last = batch[batch.length - 1]!;
      const done = last.seq >= head.contigSeq;
      const accepted = ps.session.sendData((mid) => ({
        t: "ENTRIES",
        mid,
        topic: d.topic,
        writer: d.writer,
        fromSeq: batch[0]!.seq,
        toSeq: head.contigSeq,
        entries: batch,
        done,
      }));
      if (!accepted) return; // queue full — keep dirty, resume on onCapacity
      ps.known.set(key, last.seq); // optimistic; the next HAVE round is authoritative
      if (done) ps.dirty.delete(key);
    }
    if (earliestFuture !== null) this.schedulePump(ps, Math.max(0, earliestFuture - now));
  }

  // ---- WANT (requester side) ----

  private queueWant(ps: PeerState, topic: Topic, writer: WriterId): void {
    const key = `${topic} ${writer}`;
    if (ps.wantedStreams.has(key)) return;
    ps.wantedStreams.add(key);
    ps.wantQueue.push({ topic, writer });
    this.pumpWants(ps);
  }

  private pumpWants(ps: PeerState): void {
    while (ps.activeWants.size < MAX_WANT_CONCURRENT && ps.wantQueue.length > 0) {
      const next = ps.wantQueue.shift();
      if (!next) return;
      const head = this.o.core.getStream(next.topic, next.writer);
      const recovery = this.o.core.recoveryTarget(next.topic, next.writer);
      if (head.sealReason !== null && !recovery) {
        ps.wantedStreams.delete(`${next.topic} ${next.writer}`);
        continue;
      }
      const req = ++ps.wantReq;
      // §12 steps 1–2: PROBE evidence bounds the re-verification window; without
      // it the canonical prefix re-verifies from the start (mismatching local
      // rows quarantine at the first divergence either way)
      let recoveryFrom = 1;
      if (recovery) {
        const ev = this.o.store.metaGet(
          `fork_evidence:${next.topic}:${next.writer}:${ps.session.peerId}`,
        );
        if (ev) {
          const { lastCommon } = JSON.parse(ev) as { lastCommon: number };
          if (Number.isSafeInteger(lastCommon) && lastCommon > 0) recoveryFrom = lastCommon + 1;
        }
      }
      const want: WantState = {
        req,
        topic: next.topic,
        writer: next.writer,
        fromSeq: recovery ? recoveryFrom : head.contigSeq + 1,
      };
      ps.activeWants.set(req, want);
      ps.session.request(
        `WANT:${req}`,
        (): MsgWant => ({
          t: "WANT",
          req,
          topic: want.topic,
          writer: want.writer,
          fromSeq: want.fromSeq,
        }),
      );
    }
  }

  private finishWant(ps: PeerState, req: number): void {
    const want = ps.activeWants.get(req);
    if (!want) return;
    ps.session.satisfyRequest(`WANT:${req}`);
    ps.activeWants.delete(req);
    ps.wantedStreams.delete(`${want.topic} ${want.writer}`);
    this.pumpWants(ps);
  }

  // ---- control dispatch ----

  private onControl(s: Session, m: ControlMsg): void {
    const ps = this.peers.get(s);
    if (!ps) return;
    ps.lastActivity = this.o.clock();
    switch (m.t) {
      case "HAVE_GET":
        this.serveHave(ps, m);
        break;
      case "HAVE":
        this.onHavePage(ps, m);
        break;
      case "WANT":
        this.serveWant(ps, m);
        break;
      case "PROBE":
        this.serveProbe(ps, m);
        break;
      case "PROBE_RES": {
        const cb = ps.probes.get(`${m.topic} ${m.writer}`);
        if (cb) cb(m);
        break;
      }
      case "FINALITY":
        if (this.finalityHub && ps.session.mutualFull(m.topic))
          void this.finalityHub.ingestFromWire(m.cert);
        break;
      case "WRITER_DIRECTIVE":
        if (this.directiveHub && ps.session.mutualFull(m.directive.topic))
          void this.directiveHub.ingestFromWire(m.directive);
        break;
      case "SNAPSHOT_OFFER":
        if (ps.session.mutualFull(m.topic)) this.snapshotHub?.handleOffer(ps.session, m);
        break;
      case "SNAPSHOT_GET":
        if (ps.session.mutualFull(m.topic)) this.snapshotHub?.handleGet(ps.session, m);
        break;
      case "SUB":
        this.subHub?.handleSub(ps.session, m);
        break;
      case "UNSUB":
        this.subHub?.handleUnsub(ps.session, m);
        break;
      case "SUB_ERR":
        this.subHub?.handleSubErr(ps.session, m);
        break;
      case "ERR":
        // ERR_SCHEMA_MISMATCH per topic is recorded by the peer side; nothing
        // actionable here beyond not retrying that topic.
        break;
      default:
        break; // SNAPSHOT/SUB families land in milestones ③d–④
    }
  }

  // ---- HAVE (responder side) ----

  private serveHave(ps: PeerState, m: MsgHaveGet): void {
    // A retry with the SAME req returns the identically captured round; a new
    // req discards the previous capture (§5.4).
    if (ps.captured?.req !== m.req) {
      const full = this.o.core.vectors();
      const visible: HaveVectors = {};
      for (const [topic, v] of Object.entries(full)) {
        if (ps.session.mutualFull(topic)) visible[topic] = v;
      }
      ps.captured = { req: m.req, pages: this.paginateHave(m.req, visible) };
    }
    for (const page of ps.captured.pages) ps.session.sendControl(page);
  }

  private paginateHave(req: number, vectors: HaveVectors): MsgHave[] {
    const budget = Math.floor(this.o.constants.MAX_FRAME_BYTES / 2);
    const pages: HaveVectors[] = [];
    let current: HaveVectors = {};
    let bytes = 0;
    for (const [topic, v] of Object.entries(vectors)) {
      for (const [writer, w] of Object.entries(v.writers)) {
        const cost = utf8ByteLength(JSON.stringify({ [topic]: { [writer]: w } }));
        if (bytes + cost > budget && Object.keys(current).length > 0) {
          pages.push(current);
          current = {};
          bytes = 0;
        }
        const t = (current[topic] ??= { writers: {} });
        if (v.fgen !== undefined) t.fgen = v.fgen;
        t.writers[writer] = w;
        bytes += cost;
      }
      if (Object.keys(v.writers).length === 0) {
        const t = (current[topic] ??= { writers: {} });
        if (v.fgen !== undefined) t.fgen = v.fgen;
      }
    }
    pages.push(current); // always ≥1 page, even when empty
    return pages.map((p, i) => ({ t: "HAVE", req, page: i + 1, of: pages.length, vectors: p }));
  }

  // ---- WANT (responder side) ----

  private serveWant(ps: PeerState, m: MsgWant): void {
    if (!ps.session.mutualFull(m.topic)) {
      ps.session.sendControl({ t: "ERR", code: "ERR_ACL_DENIED", ref: m.topic });
      return;
    }
    if (this.snapshotHub?.maybeOffer(ps.session, m.topic, m.writer, m.fromSeq)) {
      // the WANT itself completes empty — the snapshot path supplies the basis,
      // and the requester re-WANTs above the cut after adoption
      const contig = this.o.core.getStream(m.topic, m.writer).contigSeq;
      ps.session.sendData((mid) => ({
        t: "ENTRIES",
        mid,
        req: m.req,
        topic: m.topic,
        writer: m.writer,
        fromSeq: m.fromSeq,
        toSeq: contig,
        entries: [],
        done: true,
      }));
      return;
    }
    const head = this.o.core.getStream(m.topic, m.writer);
    const toSeq = head.contigSeq; // captured at processing — stable completion point
    if (m.fromSeq > toSeq) {
      ps.session.sendData((mid) => ({
        t: "ENTRIES",
        mid,
        req: m.req,
        topic: m.topic,
        writer: m.writer,
        fromSeq: m.fromSeq,
        toSeq,
        entries: [],
        done: true,
      }));
      return;
    }
    const budget = Math.floor(this.o.constants.MAX_FRAME_BYTES / 2);
    let from = m.fromSeq;
    while (from <= toSeq) {
      const batch: LogEntry[] = [];
      let bytes = 0;
      const rows = this.o.store.entriesRange(
        m.topic,
        m.writer,
        from,
        Math.min(toSeq, from + ENTRIES_BATCH_MAX - 1),
      );
      if (rows.length === 0) break; // below retention — snapshot path (milestone ③)
      for (const { entry } of rows) {
        const cost = utf8ByteLength(JSON.stringify(entry));
        if (batch.length > 0 && bytes + cost > budget) break;
        batch.push(entry);
        bytes += cost;
      }
      const last = batch[batch.length - 1];
      if (!last) break;
      const done = last.seq >= toSeq;
      const fromSeq = from;
      const accepted = ps.session.sendData((mid) => ({
        t: "ENTRIES",
        mid,
        req: m.req,
        topic: m.topic,
        writer: m.writer,
        fromSeq,
        toSeq,
        entries: batch,
        done,
      }));
      if (!accepted) return; // tail-dropped — the requester's WANT retry recovers
      from = last.seq + 1;
    }
  }

  // ---- data dispatch ----

  private onData(s: Session, m: { t: string }): void {
    const ps = this.peers.get(s);
    if (!ps) return;
    ps.lastActivity = this.o.clock();
    if (m.t === "ENTRIES") this.onEntries(ps, m as MsgEntries);
    else if (m.t === "SNAP") this.subHub?.handleSnap(ps.session, m as MsgSnap);
    else if (m.t === "DELTA") this.subHub?.handleDelta(ps.session, m as MsgDelta);
    else if (m.t === "SNAPSHOT") this.snapshotHub?.handleChunk(ps.session, m as MsgSnapshot);
  }

  private onEntries(ps: PeerState, m: MsgEntries): void {
    if (!ps.session.mutualFull(m.topic)) return; // ACL re-verified on receive (§5.4)
    const key = `${m.topic}\u0000${m.writer}`;
    if ((ps.known.get(key) ?? 0) < m.toSeq) ps.known.set(key, m.toSeq); // the sender holds ≥ toSeq
    const applies: Promise<unknown>[] = [];
    for (const raw of m.entries) {
      let entry: LogEntry;
      try {
        entry = validateEntry(raw, this.o.constants);
      } catch (e) {
        const code = e instanceof SeqscribeError ? e.code : "ERR_ENTRY_ENCODING";
        ps.session.sendControl({ t: "ERR", code, ref: `${m.topic}/${m.writer}` });
        continue;
      }
      if (entry.topic !== m.topic || entry.writer !== m.writer) continue;
      applies.push(this.o.core.applyExternal(entry, ps.session.peerId).catch(() => undefined));
    }
    void Promise.all(applies).then(() => {
      if (m.req !== undefined && m.done) {
        const want = ps.activeWants.get(m.req);
        this.finishWant(ps, m.req);
        // the responder's captured toSeq may already be stale — re-check
        if (want) {
          const head = this.o.core.getStream(m.topic, m.writer);
          const target = this.o.core.recoveryTarget(m.topic, m.writer);
          if (target) {
            // absorbing-state detection: a round that made no progress means this
            // peer cannot supply the canonical range (§12 step 3)
            const key = `${m.topic} ${m.writer}`;
            const before = ps.recoveryProgress.get(key);
            ps.recoveryProgress.set(key, head.contigSeq);
            if (before !== undefined && before >= head.contigSeq)
              this.o.core.reportCanonicalUnavailable(m.topic, m.writer);
            else this.queueWant(ps, m.topic, m.writer); // keep pulling toward finalSeq
          } else if (head.sealReason === null && m.toSeq > head.contigSeq) {
            // below-cut deficits ride the snapshot path, not WANT retries —
            // adoption restarts HAVE rounds and re-WANTs above the cut (§7.8)
            const cert = this.o.core.getCert(m.topic);
            const awaitingSnapshot =
              cert !== null && (cert.cut[m.writer]?.seq ?? 0) > head.contigSeq;
            if (!awaitingSnapshot) this.queueWant(ps, m.topic, m.writer);
          }
        }
      }
    });
  }

  // ---- eager push & knowledge-based relay (§6.3, extended per proposals-v3.4 P1) ----
  //
  // Own appends push to the K most-recently-active peers (spec §6.3). External
  // applies mark ALL other mutual-full peers dirty and let knowledge targeting
  // suppress sends the peer doesn't need — replacing the hlc-window relay,
  // whose recency criterion cannot gossip partition-aged backlogs (the P7
  // finding: repair degenerated to one hop per anti-entropy round).

  handleApplied(e: LogEntry, via: string | undefined): void {
    const own = via === undefined && e.writer === this.o.writerId;
    if (own) {
      const throttle = this.o.topics.get(e.topic).policy.flushThrottleMs ?? 0;
      const candidates = [...this.peers.values()]
        .filter(
          (ps) => ps.session.state() === "ready" && ps.session.mutualFull(e.topic),
        )
        .sort((a, b) =>
          b.lastActivity !== a.lastActivity
            ? b.lastActivity - a.lastActivity
            : a.session.peerId < b.session.peerId
              ? -1
              : 1,
        )
        .slice(0, this.o.constants.EAGER_PUSH_K);
      for (const ps of candidates) this.markDirty(ps, e.topic, e.writer, throttle);
      return;
    }
    if (via === undefined) return; // import/local replay: HAVE rounds cover it
    for (const ps of this.peers.values()) {
      if (ps.session.peerId === via) continue;
      this.markDirty(ps, e.topic, e.writer, 0);
    }
  }

  // ---- PROBE (§6.4 evidence) ----

  private serveProbe(ps: PeerState, m: MsgProbe): void {
    const points: { seq: Seq; chain: string }[] = [];
    let minMissing: Seq | null = null;
    const head = this.o.core.getStream(m.topic, m.writer);
    for (const seq of m.seqs) {
      const e = this.o.store.getEntry(m.topic, m.writer, seq);
      if (e) points.push({ seq, chain: e.chain });
      else if (seq <= head.contigSeq && (minMissing === null || seq < minMissing)) minMissing = seq;
    }
    const res: MsgProbeRes = { t: "PROBE_RES", topic: m.topic, writer: m.writer, points };
    if (minMissing !== null) res.unavailable = { belowSeq: minMissing + 1 };
    ps.session.sendControl(res);
  }

  // On fork detection: one log-spaced PROBE sweep to bound the last common
  // point; evidence lands in sq_meta for the host's §12 adjudication.
  private locateFork(ps: PeerState, topic: Topic, writer: WriterId, upTo: Seq): void {
    const seqs: Seq[] = [];
    for (let d = 0; ; d = d === 0 ? 1 : d * 2) {
      const seq = upTo - d;
      if (seq < 1) break;
      seqs.push(seq);
      if (seqs.length >= 32) break;
    }
    if (seqs.length === 0) return;
    const key = `${topic} ${writer}`;
    ps.probes.set(key, (res) => {
      ps.probes.delete(key);
      ps.session.satisfyRequest(`PROBE:${key}`);
      let lastCommon: Seq = 0;
      for (const p of res.points) {
        const mine = this.o.store.getEntry(topic, writer, p.seq);
        if (mine && mine.chain === p.chain && p.seq > lastCommon) lastCommon = p.seq;
      }
      this.o.store.metaSet(
        `fork_evidence:${topic}:${writer}:${ps.session.peerId}`,
        JSON.stringify({ lastCommon, upTo, at: this.o.clock() }),
      );
    });
    ps.session.request(`PROBE:${key}`, (): MsgProbe => ({ t: "PROBE", topic, writer, seqs }));
  }
}
