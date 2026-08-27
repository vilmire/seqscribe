// SPEC §5.1–5.3, §18 PeerSession — one Channel: HELLO negotiation, the control
// lane (immediate send, request retries), and the data lane (mids assigned at
// transmission, ACK-to-contiguous, credits, stall detection, tail-drop queue).

import { misuse, SeqscribeError } from "./errors.js";
import {
  DATA_TYPES,
  parseMsg,
  serializeMsg,
  type ControlMsg,
  type DataMsg,
  type MsgHello,
  type WireMsg,
} from "./messages.js";
import type { TopicRegistry } from "./topics.js";
import type {
  Channel,
  Constants,
  PeerHandle,
  Timers,
  Topic,
  Unsub,
  WriterId,
} from "./types.js";

export const PROTO_MIN = 1;
export const PROTO_MAX = 1;

export type SessionState = "attached" | "ready" | "closed";

// Reasoned close causes (extension per proposals-v3.5 P10/P16). Every close
// path names why, so hosts can distinguish a non-speaking endpoint
// ("hello_timeout") from transport loss, protocol violations, stall, and
// deliberate detach — previously all collapsed into one reasonless close().
export type SessionCloseReason =
  | "hello_timeout" // no HELLO within HELLO_TIMEOUT_MS — the peer is reachable but not speaking seqscribe
  | "transport" // the host's Channel closed under us
  | "protocol" // protocol violation: version mismatch, credit-window abuse, reassembly overflow
  | "stall" // unACKed data older than CHANNEL_STALL_MS (§5.2)
  | "detach" // the host called PeerHandle.detach()
  | "node_closed"; // node.close() tore the session down

// The complete lifecycle feed (proposals-v3.5 P16): unlike onStateChange, a
// subscriber registered after attach still sees every transition (events are
// replayed on subscribe), and "closed" carries its reason.
export interface PeerLifecycleEvent {
  peerId: string;
  event: "attached" | "ready" | "closed";
  reason?: SessionCloseReason; // present iff event === "closed"
}

// The handle node.attach actually returns (extension beyond SPEC §14
// PeerHandle — proposals-v3.5 P15/P16): reasoned lifecycle plus runtime grant
// re-advertisement on the established session.
export interface PeerHandleExt extends PeerHandle {
  closeReason(): SessionCloseReason | null;
  onLifecycle(cb: (e: PeerLifecycleEvent) => void): Unsub;
  updateGrants(grants: Record<Topic, "full" | "serve" | "none">): void;
}

export interface SessionOpts {
  channel: Channel;
  peerId: string;
  peerClass: "content" | "metadata";
  grants: Record<Topic, "full" | "serve" | "none">;
  localNode: WriterId;
  topics: TopicRegistry;
  constants: Constants;
  timers: Timers;
  clock: () => number;
  onReady: (s: Session) => void;
  onClose: (s: Session) => void;
  onControl: (s: Session, m: ControlMsg) => void;
  onData: (s: Session, m: DataMsg) => void;
  onCapacity?: ((s: Session) => void) | undefined; // data queue drained below cap
  onGrantsUpdated?: ((s: Session) => void) | undefined; // either side's grants changed post-ready (P15)
}

interface PendingRequest {
  make: () => ControlMsg;
  timer: unknown;
}

export class Session {
  readonly peerId: string;
  readonly peerClass: "content" | "metadata";
  private grants: Record<Topic, "full" | "serve" | "none">;

  private readonly o: SessionOpts;
  private readonly c: Constants;
  private stateNow: SessionState = "attached";
  private closeReasonNow: SessionCloseReason | null = null;
  private readonly stateListeners = new Set<(s: SessionState) => void>();
  private readonly lifecycleHistory: PeerLifecycleEvent[] = [];
  private readonly lifecycleListeners = new Set<(e: PeerLifecycleEvent) => void>();

  private peerGrants: MsgHello["grants"] = {};
  // Grant generations (P15): each local updateGrants bumps grantsGen and
  // re-advertises via HELLO; peerGrantsGen dedupes the peer's re-advertisements
  // (retries and stale frames carry an already-seen gen and are no-ops).
  private grantsGen = 0;
  private peerGrantsGen = 0;
  private readonly refusedTopics = new Set<Topic>();

  // data lane — sender
  private nextMid = 1;
  private readonly unacked: { mid: number; at: number; frame: string; sentAt: number }[] = [];
  private readonly outQueue: ((mid: number) => DataMsg)[] = [];
  private retryTimer: unknown = null;
  // data lane — receiver
  private recvContigMid = 0;
  private readonly recvBuffer = new Map<number, DataMsg>();

  private readonly preReadyControl: ControlMsg[] = [];
  private readonly requests = new Map<string, PendingRequest>();
  private helloTimer: unknown = null;
  private stallTimer: unknown = null;

  constructor(opts: SessionOpts) {
    this.o = opts;
    this.c = opts.constants;
    this.peerId = opts.peerId;
    this.peerClass = opts.peerClass;
    this.assertGrants(opts.grants);
    // a private copy — mutating the host's original object is an apparent but
    // ineffective update path (P15); the supported one is updateGrants()
    this.grants = { ...opts.grants };
    this.emitLifecycle({ peerId: this.peerId, event: "attached" });

    opts.channel.onMessage((raw) => this.onRaw(raw));
    opts.channel.onClose(() => this.close("transport"));

    this.request("HELLO", () => this.helloMsg());
    this.helloTimer = this.o.timers.setTimeout(
      () => this.close("hello_timeout"),
      this.c.HELLO_TIMEOUT_MS,
    );
    this.stallTimer = this.o.timers.setTimeout(
      () => this.stallCheck(),
      Math.ceil(this.c.CHANNEL_STALL_MS / 2),
    );
  }

  // §14 attach ACL guard: metadata peers can never be granted content topics;
  // a "full" grant on a subscribe-only topic would negotiate mutual full-sync
  // against a topic that has no durable log to sync (proposals-v3.5). Also
  // enforces defined-before-grant, for updateGrants exactly as for attach.
  private assertGrants(grants: Record<Topic, "full" | "serve" | "none">): void {
    for (const [topic, mode] of Object.entries(grants)) {
      const policy = this.o.topics.get(topic).policy;
      if (mode === "none") continue;
      if (this.peerClass === "metadata" && policy.access === "content")
        throw misuse(`metadata-class peer granted content topic ${topic}`);
      if (mode === "full" && policy.replication === "subscribe-only")
        throw misuse(`"full" grant on subscribe-only topic ${topic} — grant "serve" instead`);
    }
  }

  state(): SessionState {
    return this.stateNow;
  }

  closeReason(): SessionCloseReason | null {
    return this.closeReasonNow;
  }

  onStateChange(cb: (s: SessionState) => void): () => void {
    this.stateListeners.add(cb);
    return () => this.stateListeners.delete(cb);
  }

  // P16 lifecycle feed: replays history on subscribe, so a listener registered
  // after attach (the only kind attach can return) still records "attached".
  onLifecycle(cb: (e: PeerLifecycleEvent) => void): () => void {
    for (const e of this.lifecycleHistory) cb(e);
    this.lifecycleListeners.add(cb);
    return () => this.lifecycleListeners.delete(cb);
  }

  private emitLifecycle(e: PeerLifecycleEvent): void {
    this.lifecycleHistory.push(e);
    for (const cb of this.lifecycleListeners) cb(e);
  }

  // P15: replace this session's grants and re-advertise them to the peer. The
  // HELLO re-advertisement rides the §5.3 request machinery — retried every
  // CONTROL_RETRY_MS until the peer answers with ackGen === grantsGen — so a
  // dropped frame cannot silently strand the update. Newly-defined topics must
  // be defined on BOTH endpoints (and granted on both) before they become
  // mutual; the peer's own updateGrants announces its half.
  updateGrants(grants: Record<Topic, "full" | "serve" | "none">): void {
    if (this.stateNow === "closed") throw misuse("updateGrants on a closed session");
    this.assertGrants(grants);
    this.grants = { ...grants };
    this.grantsGen++;
    this.recomputeRefusals();
    this.request("HELLO", () => this.helloMsg());
    if (this.stateNow === "ready") this.o.onGrantsUpdated?.(this);
  }

  // Effective per-topic relations after HELLO (§5.4)
  mutualFull(topic: Topic): boolean {
    return (
      this.stateNow === "ready" &&
      !this.refusedTopics.has(topic) &&
      this.grants[topic] === "full" &&
      this.peerGrants[topic]?.mode === "full"
    );
  }

  peerMaySub(topic: Topic): boolean {
    const mine = this.grants[topic];
    return this.stateNow === "ready" && (mine === "full" || mine === "serve");
  }

  refused(topic: Topic): boolean {
    return this.refusedTopics.has(topic);
  }

  // ---- control lane ----

  sendControl(m: ControlMsg): void {
    if (this.stateNow === "closed") return;
    this.o.channel.send(serializeMsg(m, this.c));
  }

  // Requests expecting responses retry every CONTROL_RETRY_MS until satisfied
  // or close (§5.3).
  request(key: string, make: () => ControlMsg): void {
    if (this.stateNow === "closed") return;
    this.cancelRequest(key);
    const fire = () => {
      this.sendControl(make());
      const pr = this.requests.get(key);
      if (pr) pr.timer = this.o.timers.setTimeout(fire, this.c.CONTROL_RETRY_MS);
    };
    this.requests.set(key, { make, timer: null });
    fire();
  }

  satisfyRequest(key: string): void {
    this.cancelRequest(key);
  }

  private cancelRequest(key: string): void {
    const pr = this.requests.get(key);
    if (pr) {
      if (pr.timer !== null) this.o.timers.clearTimeout(pr.timer);
      this.requests.delete(key);
    }
  }

  // ---- data lane ----

  // Returns false when the message was tail-dropped (SEND_QUEUE_CAP). Dropped
  // builders never consumed a mid — drops create no mid gaps (§5.2).
  sendData(make: (mid: number) => DataMsg): boolean {
    if (this.stateNow === "closed") return false;
    if (this.outQueue.length >= this.c.SEND_QUEUE_CAP) return false;
    this.outQueue.push(make);
    this.pumpData();
    return true;
  }

  hasSendCapacity(): boolean {
    return this.stateNow === "ready" && this.outQueue.length < this.c.SEND_QUEUE_CAP;
  }

  queuedData(): number {
    return this.outQueue.length + this.unacked.length;
  }

  private pumpData(): void {
    while (
      this.stateNow !== "closed" &&
      this.outQueue.length > 0 &&
      this.unacked.length < this.c.INFLIGHT_CREDITS
    ) {
      const make = this.outQueue.shift();
      if (!make) return;
      const mid = this.nextMid++;
      const frame = serializeMsg(make(mid), this.c);
      this.unacked.push({ mid, at: this.o.clock(), frame, sentAt: this.o.clock() });
      this.o.channel.send(frame);
    }
    if (this.unacked.length > 0 && this.retryTimer === null) {
      this.retryTimer = this.o.timers.setTimeout(
        () => this.retryUnacked(),
        this.c.CONTROL_RETRY_MS,
      );
    }
  }

  // Data-lane ARQ (proposals-v3.4 P2): a lost frame otherwise head-of-line
  // blocks the mid-contiguous receiver until the CHANNEL_STALL_MS close —
  // retransmitting unACKed frames (same mid) bounds loss recovery to the retry
  // cadence instead. Receivers dedupe by mid, so retransmits are harmless.
  private retryUnacked(): void {
    this.retryTimer = null;
    if (this.stateNow === "closed") return;
    const now = this.o.clock();
    for (const u of this.unacked) {
      if (now - u.sentAt >= this.c.CONTROL_RETRY_MS) {
        u.sentAt = now;
        this.o.channel.send(u.frame);
      }
    }
    if (this.unacked.length > 0)
      this.retryTimer = this.o.timers.setTimeout(
        () => this.retryUnacked(),
        this.c.CONTROL_RETRY_MS,
      );
  }

  private stallCheck(): void {
    if (this.stateNow === "closed") return;
    const oldest = this.unacked[0];
    if (oldest && this.o.clock() - oldest.at > this.c.CHANNEL_STALL_MS) {
      this.close("stall");
      return;
    }
    this.stallTimer = this.o.timers.setTimeout(
      () => this.stallCheck(),
      Math.ceil(this.c.CHANNEL_STALL_MS / 2),
    );
  }

  // ---- lifecycle ----

  close(reason: SessionCloseReason = "detach"): void {
    if (this.stateNow === "closed") return;
    this.stateNow = "closed";
    this.closeReasonNow = reason;
    for (const key of [...this.requests.keys()]) this.cancelRequest(key);
    if (this.helloTimer !== null) this.o.timers.clearTimeout(this.helloTimer);
    if (this.stallTimer !== null) this.o.timers.clearTimeout(this.stallTimer);
    if (this.retryTimer !== null) this.o.timers.clearTimeout(this.retryTimer);
    this.o.channel.close();
    for (const cb of this.stateListeners) cb("closed");
    this.emitLifecycle({ peerId: this.peerId, event: "closed", reason });
    this.o.onClose(this);
  }

  // ---- inbound ----

  private helloMsg(): MsgHello {
    const grants: MsgHello["grants"] = {};
    for (const [topic, mode] of Object.entries(this.grants)) {
      grants[topic] = { mode, schemaHash: this.o.topics.get(topic).schemaHash };
    }
    return {
      t: "HELLO",
      protoMin: PROTO_MIN,
      protoMax: PROTO_MAX,
      node: this.o.localNode,
      grants,
      grantsGen: this.grantsGen,
    };
  }

  // Effective refusal set (§5.4 schema negotiation), recomputed from the
  // CURRENT grant pair — at initial HELLO and again on every P15 grant update
  // (a redefinition-free mismatch can appear or vanish as topics are granted).
  // Only newly refused topics get an ERR, matching the original negotiation.
  private recomputeRefusals(): void {
    const next = new Set<Topic>();
    for (const [topic, mine] of Object.entries(this.grants)) {
      if (mine !== "full") continue;
      const theirs = this.peerGrants[topic];
      if (theirs?.mode !== "full") continue;
      if (theirs.schemaHash !== this.o.topics.get(topic).schemaHash) next.add(topic);
    }
    for (const topic of next) {
      if (!this.refusedTopics.has(topic))
        this.sendControl({ t: "ERR", code: "ERR_SCHEMA_MISMATCH", ref: topic });
    }
    this.refusedTopics.clear();
    for (const topic of next) this.refusedTopics.add(topic);
  }

  private onRaw(raw: string): void {
    if (this.stateNow === "closed") return;
    let m: WireMsg;
    try {
      m = parseMsg(raw, this.c);
    } catch (e) {
      const code = e instanceof SeqscribeError ? e.code : "ERR_ENTRY_ENCODING";
      this.sendControl({ t: "ERR", code, detail: "unparseable frame" });
      return;
    }
    // A handler failure is a protocol error, not a transport fault: it gets the
    // same ERR-and-drop discipline as an unparseable frame. Nothing may throw
    // into the host's Channel.onMessage callback (§5.1 — the Channel is the
    // host's transport; its callbacks are not our error path). Dropped frames
    // recover via the §5.2/§5.3 retry machinery, exactly like frame loss.
    try {
      this.dispatch(m);
    } catch (e) {
      const code = e instanceof SeqscribeError ? e.code : "ERR_ENTRY_ENCODING";
      this.sendControl({ t: "ERR", code, detail: `${m.t} handler failed` });
    }
  }

  private dispatch(m: WireMsg): void {
    if (m.t === "HELLO") {
      this.onHello(m);
      return;
    }
    if (m.t === "ACK") {
      let i = 0;
      while (i < this.unacked.length && this.unacked[i]!.mid <= m.upTo) i++;
      this.unacked.splice(0, i);
      this.pumpData();
      if (this.hasSendCapacity()) this.o.onCapacity?.(this);
      return;
    }
    if (DATA_TYPES.has(m.t)) {
      this.onDataMsg(m as DataMsg);
      return;
    }
    if (this.stateNow !== "ready") {
      this.preReadyControl.push(m as ControlMsg);
      return;
    }
    this.o.onControl(this, m as ControlMsg);
  }

  private onHello(m: MsgHello): void {
    if (this.stateNow === "ready") {
      this.onHelloUpdate(m); // P15 grant re-advertisement lane
      return;
    }
    if (this.stateNow !== "attached") return;
    const proto = Math.min(PROTO_MAX, m.protoMax);
    if (proto < Math.max(PROTO_MIN, m.protoMin)) {
      this.sendControl({ t: "ERR", code: "ERR_PROTO_VERSION" });
      this.close("protocol");
      return;
    }
    this.peerGrants = m.grants;
    this.peerGrantsGen = m.grantsGen ?? 0;
    this.recomputeRefusals();
    this.satisfyRequest("HELLO");
    if (this.helloTimer !== null) {
      this.o.timers.clearTimeout(this.helloTimer);
      this.helloTimer = null;
    }
    this.stateNow = "ready";
    for (const cb of this.stateListeners) cb("ready");
    this.emitLifecycle({ peerId: this.peerId, event: "ready" });
    // an updateGrants that raced the handshake: its request was just satisfied
    // by the peer's INITIAL hello — re-issue until the peer acks the update gen
    if (this.grantsGen > 0) this.request("HELLO", () => this.helloMsg());
    this.o.onReady(this);
    const held = this.preReadyControl.splice(0);
    for (const c of held) this.o.onControl(this, c);
    this.drainData();
  }

  // Post-ready HELLO (P15). A frame WITHOUT ackGen is a peer-initiated
  // re-advertisement — or a handshake retry, which gen-dedup turns into a
  // no-op — and is always answered with our current HELLO carrying ackGen so
  // the peer's update request can settle. A frame WITH ackGen is that answer
  // and is never answered back (no ping-pong); it still carries the peer's
  // current grants, applied above when newer.
  private onHelloUpdate(m: MsgHello): void {
    const gen = m.grantsGen ?? 0;
    if (gen > this.peerGrantsGen) {
      this.peerGrants = m.grants;
      this.peerGrantsGen = gen;
      this.recomputeRefusals();
      this.o.onGrantsUpdated?.(this);
    }
    if (m.ackGen !== undefined) {
      if (m.ackGen === this.grantsGen) this.satisfyRequest("HELLO");
      return; // a stale ack (retry racing a newer local update) — keep retrying
    }
    this.sendControl({ ...this.helloMsg(), ackGen: gen });
  }

  private onDataMsg(m: DataMsg): void {
    if (m.mid <= this.recvContigMid) {
      this.sendControl({ t: "ACK", upTo: this.recvContigMid }); // duplicate — re-ACK
      return;
    }
    // §5.2 credit-window bound: a conforming sender keeps unACKed ≤
    // INFLIGHT_CREDITS and ACK advances only to the contiguous mid, so every
    // legitimate data frame satisfies mid ≤ recvContigMid + INFLIGHT_CREDITS
    // (mids are consecutive; the sender's window starts at our last ACK, which
    // is never ahead of recvContigMid). Beyond that is credit abuse or a
    // desynced peer — either way recvBuffer would grow without bound. §5 names
    // no lighter response, so the conservative one: ERR + close (the host
    // redials; mids/credits reset per §5.2).
    if (m.mid > this.recvContigMid + this.c.INFLIGHT_CREDITS) {
      this.sendControl({
        t: "ERR",
        code: "ERR_ENTRY_ENCODING",
        detail: `data mid ${m.mid} beyond credit window`,
      });
      this.close("protocol");
      return;
    }
    this.recvBuffer.set(m.mid, m);
    if (this.stateNow === "ready") this.drainData();
  }

  private drainData(): void {
    let advanced = false;
    for (;;) {
      const next = this.recvBuffer.get(this.recvContigMid + 1);
      if (!next) break;
      this.recvBuffer.delete(next.mid);
      this.recvContigMid = next.mid;
      advanced = true;
      this.o.onData(this, next);
    }
    if (advanced) this.sendControl({ t: "ACK", upTo: this.recvContigMid });
  }
}
