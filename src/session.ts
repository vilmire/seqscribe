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
import type { Channel, Constants, Timers, Topic, WriterId } from "./types.js";

export const PROTO_MIN = 1;
export const PROTO_MAX = 1;

export type SessionState = "attached" | "ready" | "closed";

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
}

interface PendingRequest {
  make: () => ControlMsg;
  timer: unknown;
}

export class Session {
  readonly peerId: string;
  readonly peerClass: "content" | "metadata";
  readonly grants: Record<Topic, "full" | "serve" | "none">;

  private readonly o: SessionOpts;
  private readonly c: Constants;
  private stateNow: SessionState = "attached";
  private readonly stateListeners = new Set<(s: SessionState) => void>();

  private peerGrants: MsgHello["grants"] = {};
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
    this.grants = opts.grants;

    // §14 attach ACL guard: metadata peers can never be granted content topics;
    // a "full" grant on a subscribe-only topic would negotiate mutual full-sync
    // against a topic that has no durable log to sync (proposals-v3.5)
    for (const [topic, mode] of Object.entries(opts.grants)) {
      if (mode === "none") continue;
      const policy = opts.topics.get(topic).policy;
      if (opts.peerClass === "metadata" && policy.access === "content")
        throw misuse(`metadata-class peer granted content topic ${topic}`);
      if (mode === "full" && policy.replication === "subscribe-only")
        throw misuse(`"full" grant on subscribe-only topic ${topic} — grant "serve" instead`);
    }

    opts.channel.onMessage((raw) => this.onRaw(raw));
    opts.channel.onClose(() => this.close());

    this.request("HELLO", () => this.helloMsg());
    this.helloTimer = this.o.timers.setTimeout(() => this.close(), this.c.HELLO_TIMEOUT_MS);
    this.stallTimer = this.o.timers.setTimeout(
      () => this.stallCheck(),
      Math.ceil(this.c.CHANNEL_STALL_MS / 2),
    );
  }

  state(): SessionState {
    return this.stateNow;
  }

  onStateChange(cb: (s: SessionState) => void): () => void {
    this.stateListeners.add(cb);
    return () => this.stateListeners.delete(cb);
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
      this.close();
      return;
    }
    this.stallTimer = this.o.timers.setTimeout(
      () => this.stallCheck(),
      Math.ceil(this.c.CHANNEL_STALL_MS / 2),
    );
  }

  // ---- lifecycle ----

  close(): void {
    if (this.stateNow === "closed") return;
    this.stateNow = "closed";
    for (const key of [...this.requests.keys()]) this.cancelRequest(key);
    if (this.helloTimer !== null) this.o.timers.clearTimeout(this.helloTimer);
    if (this.stallTimer !== null) this.o.timers.clearTimeout(this.stallTimer);
    if (this.retryTimer !== null) this.o.timers.clearTimeout(this.retryTimer);
    this.o.channel.close();
    for (const cb of this.stateListeners) cb("closed");
    this.o.onClose(this);
  }

  // ---- inbound ----

  private helloMsg(): MsgHello {
    const grants: MsgHello["grants"] = {};
    for (const [topic, mode] of Object.entries(this.grants)) {
      grants[topic] = { mode, schemaHash: this.o.topics.get(topic).schemaHash };
    }
    return { t: "HELLO", protoMin: PROTO_MIN, protoMax: PROTO_MAX, node: this.o.localNode, grants };
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
    if (this.stateNow !== "attached") return; // duplicate HELLO (retry) — already negotiated
    const proto = Math.min(PROTO_MAX, m.protoMax);
    if (proto < Math.max(PROTO_MIN, m.protoMin)) {
      this.sendControl({ t: "ERR", code: "ERR_PROTO_VERSION" });
      this.close();
      return;
    }
    this.peerGrants = m.grants;
    for (const [topic, mine] of Object.entries(this.grants)) {
      const theirs = m.grants[topic];
      if (mine === "full" && theirs?.mode === "full") {
        if (theirs.schemaHash !== this.o.topics.get(topic).schemaHash) {
          this.refusedTopics.add(topic);
          this.sendControl({ t: "ERR", code: "ERR_SCHEMA_MISMATCH", ref: topic });
        }
      }
    }
    this.satisfyRequest("HELLO");
    if (this.helloTimer !== null) {
      this.o.timers.clearTimeout(this.helloTimer);
      this.helloTimer = null;
    }
    this.stateNow = "ready";
    for (const cb of this.stateListeners) cb("ready");
    this.o.onReady(this);
    const held = this.preReadyControl.splice(0);
    for (const c of held) this.o.onControl(this, c);
    this.drainData();
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
      this.close();
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
