// proposals-v3.5 P10–P16 — host ergonomics & operational observability:
// reasoned session lifecycle (P16), HELLO-unresponsive signal (P10), the
// one-asynchronous-failure append contract (P11), sanitizeJson (P12),
// estimateEntryBytes (P13), and runtime topic activation via post-ready grant
// re-advertisement (P14/P15).

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import {
  assertJsonValue,
  chainOf,
  coreOf,
  createSeqscribe,
  estimateEntryBytes,
  jcs,
  manageReconnect,
  sanitizeJson,
  seedOf,
  SeqscribeError,
  utf8ByteLength,
} from "../src/index.js";
import type {
  Channel,
  Constants,
  LogEntry,
  PeerLifecycleEvent,
  SeqscribeNodeExt,
  TopicPolicy,
} from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

const TEST_CONSTANTS: Partial<Constants> = {
  ANTI_ENTROPY_MS: 2_000,
  CONTROL_RETRY_MS: 200,
  CHANNEL_STALL_MS: 3_000,
  HELLO_TIMEOUT_MS: 1_000,
};

interface TestNode {
  node: SeqscribeNodeExt;
  writerId: string;
}

function makeNode(sched: Scheduler, writerId: string, topics: string[] = [T]): TestNode {
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    constants: TEST_CONSTANTS,
  });
  for (const t of topics) node.defineTopic(t, FULL);
  return { node, writerId };
}

function contigOf(n: TestNode, writer: string, topic = T): number {
  return coreOf(n.node).getStream(topic, writer).contigSeq;
}

// a channel that never delivers anything — a reachable endpoint that does not
// speak seqscribe (the P10 misclassified-dashboard shape)
function silentChannel(): Channel {
  return { send: () => {}, onMessage: () => {}, onClose: () => {}, close: () => {} };
}

describe("P16 — reasoned session lifecycle", () => {
  it("onLifecycle replays attach for a return-time subscriber and reasons the close", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "wA");
    const h = node.attach(silentChannel(), {
      peerId: "wPeer",
      peerClass: "content",
      grants: { [T]: "full" },
    });
    const events: PeerLifecycleEvent[] = [];
    h.onLifecycle((e) => events.push(e));
    // registration happened AFTER attach — the attached transition is replayed
    expect(events).toEqual([{ peerId: "wPeer", event: "attached" }]);

    h.detach();
    expect(events[1]).toEqual({ peerId: "wPeer", event: "closed", reason: "detach" });
    expect(h.closeReason()).toBe("detach");

    // a subscriber arriving after close still gets the full history
    const late: PeerLifecycleEvent[] = [];
    h.onLifecycle((e) => late.push(e));
    expect(late).toEqual(events);
    await node.close();
  });

  it("distinguishes hello_timeout, transport, and node_closed", async () => {
    const sched = new Scheduler(0);

    // hello_timeout: nothing ever arrives on the channel
    const a = makeNode(sched, "wA");
    const hTimeout = a.node.attach(silentChannel(), {
      peerId: "wSilent",
      peerClass: "content",
      grants: { [T]: "full" },
    });
    await sched.run({ untilMs: 1_500 }); // > HELLO_TIMEOUT_MS
    expect(hTimeout.state()).toBe("closed");
    expect(hTimeout.closeReason()).toBe("hello_timeout");

    // transport: the peer side of a live link goes away
    const rng = new SeededRng(11);
    const b = makeNode(sched, "wB");
    const c = makeNode(sched, "wC");
    const link = new VirtualLink(sched, rng);
    const hb = b.node.attach(link.a, { peerId: "wC", peerClass: "content", grants: { [T]: "full" } });
    const hc = c.node.attach(link.b, { peerId: "wB", peerClass: "content", grants: { [T]: "full" } });
    await sched.run({ untilMs: 2_000 });
    expect(hb.state()).toBe("ready");
    hc.detach(); // closes the far side → our channel onClose fires
    await sched.run({ untilMs: 2_100 });
    expect(hb.closeReason()).toBe("transport");
    expect(hc.closeReason()).toBe("detach");

    // node_closed: node.close tears the session down
    const d = makeNode(sched, "wD");
    const e = makeNode(sched, "wE");
    const link2 = new VirtualLink(sched, rng);
    const hd = d.node.attach(link2.a, { peerId: "wE", peerClass: "content", grants: { [T]: "full" } });
    e.node.attach(link2.b, { peerId: "wD", peerClass: "content", grants: { [T]: "full" } });
    await sched.run({ untilMs: 4_000 });
    expect(hd.state()).toBe("ready");
    await d.node.close();
    expect(hd.closeReason()).toBe("node_closed");

    await Promise.all([a.node.close(), b.node.close(), c.node.close(), e.node.close()]);
  });
});

describe("P10 — HELLO-unresponsive signal in manageReconnect", () => {
  it("fires onPeerUnresponsive after N consecutive timeouts; 'stop' suppresses redialing", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "wA");
    let dials = 0;
    const unresponsive: number[] = [];
    const events: (PeerLifecycleEvent & { attempt: number })[] = [];
    manageReconnect(node, {
      peerId: "wDash",
      peerClass: "content",
      grants: { [T]: "full" },
      dial: () => {
        dials++;
        return silentChannel();
      },
      timers: sched.timers(),
      rng: () => 0, // no jitter — deterministic timeline
      unresponsiveAfter: 3,
      onPeerUnresponsive: (info) => {
        unresponsive.push(info.consecutiveHelloTimeouts);
        return "stop";
      },
      onEvent: (e) => events.push(e),
    });
    await sched.run({ untilMs: 60_000 });

    expect(dials).toBe(3); // attempt 4 was suppressed by "stop"
    expect(unresponsive).toEqual([3]);
    // every attempt reported attached then closed(hello_timeout), numbered
    expect(events.filter((e) => e.event === "attached").map((e) => e.attempt)).toEqual([1, 2, 3]);
    for (const e of events.filter((ev) => ev.event === "closed"))
      expect(e.reason).toBe("hello_timeout");
    await node.close();
  });

  it("a ready session resets the consecutive count", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(21);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    let dials = 0;
    const unresponsive: number[] = [];
    manageReconnect(a.node, {
      peerId: "wB",
      peerClass: "content",
      grants: { [T]: "full" },
      // dials 1–2 hit a silent endpoint; dial 3 reaches the real peer
      dial: () => {
        dials++;
        if (dials < 3) return silentChannel();
        const link = new VirtualLink(sched, rng);
        b.node.attach(link.b, { peerId: "wA", peerClass: "content", grants: { [T]: "full" } });
        return link.a;
      },
      timers: sched.timers(),
      rng: () => 0,
      unresponsiveAfter: 3,
      onPeerUnresponsive: (info) => void unresponsive.push(info.consecutiveHelloTimeouts),
    });
    await sched.run({ untilMs: 20_000 });

    expect(dials).toBe(3);
    expect(unresponsive).toEqual([]); // threshold never reached — ready reset it
    await Promise.all([a.node.close(), b.node.close()]);
  });
});

describe("P11 — one asynchronous append failure contract", () => {
  it("unknown topic and encoding failures reject instead of throwing", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "wA");

    let p1: Promise<unknown> | undefined;
    expect(() => {
      p1 = node.log("t.unknown").append("e", { a: 1 });
    }).not.toThrow();
    await expect(p1).rejects.toMatchObject({ code: "ERR_UNKNOWN_TOPIC" });

    let p2: Promise<unknown> | undefined;
    expect(() => {
      p2 = node.log(T).append("e", { a: undefined } as never);
    }).not.toThrow();
    await expect(p2).rejects.toMatchObject({ code: "ERR_ENTRY_ENCODING" });

    await node.close();
  });

  it("register raw append remains the one synchronous throw (SPEC §11.1)", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "wA");
    node.defineTopic("t.reg", { ...FULL, kind: "register", conflict: { default: "lww" } });
    expect(() => node.log("t.reg").append("set", { v: 1 })).toThrowError(/ERR_MISUSE/);
    await node.close();
  });
});

describe("P12 — sanitizeJson", () => {
  it("drops undefined object properties recursively, non-mutating, JCS-clean", () => {
    const input = {
      keep: 1,
      drop: undefined,
      nested: { a: undefined, b: [{ c: undefined, d: "x" }] },
    };
    const out = sanitizeJson(input);
    expect(out).toEqual({ keep: 1, nested: { b: [{ d: "x" }] } });
    expect(() => assertJsonValue(out)).not.toThrow();
    expect("drop" in input).toBe(true); // the original is untouched
    expect(jcs(out)).toBe('{"keep":1,"nested":{"b":[{"d":"x"}]}}');
  });

  it("rejects undefined array elements rather than shifting positions", () => {
    expect(() => sanitizeJson({ a: [1, undefined, 3] })).toThrowError(/ERR_ENTRY_ENCODING/);
    expect(() => sanitizeJson(Number.NaN)).toThrowError(/non-finite/);
  });

  it("keeps an own __proto__ key as data, never as a prototype set", () => {
    const hostile = JSON.parse('{"__proto__":{"polluted":1},"ok":2}') as unknown;
    const out = sanitizeJson(hostile) as Record<string, unknown>;
    expect(Object.getPrototypeOf(out)).toBe(Object.prototype);
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    expect(Object.getOwnPropertyNames(out)).toContain("__proto__");
  });

  it("sanitized payloads append end-to-end", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "wA");
    const loose = { v: 1, junk: undefined };
    await expect(node.log(T).append("e", loose as never)).rejects.toMatchObject({
      code: "ERR_ENTRY_ENCODING",
    });
    const p = node.log(T).append("e", sanitizeJson(loose));
    await sched.run({ untilMs: 100 }); // drive the group-commit timer
    await expect(p).resolves.toEqual([T, "wA", 1]);
    await node.close();
  });
});

describe("P13 — estimateEntryBytes", () => {
  const shape = { topic: T, kind: "note", payload: { a: 1, text: "hello" } };
  const hlc = { l: 1_724_726_400_000, c: 3 };
  const entry: LogEntry = {
    topic: T,
    writer: "wA",
    seq: 7,
    hlc,
    kind: "note",
    payload: shape.payload,
    chain: "",
  };
  entry.chain = chainOf(seedOf(T, "wA"), entry);
  const actual = utf8ByteLength(jcs(entry as never));

  it("is exact with full next-entry context", () => {
    expect(estimateEntryBytes(shape, { writer: "wA", seq: 7, hlc })).toBe(actual);
  });

  it("is a conservative upper bound without context", () => {
    expect(estimateEntryBytes(shape)).toBeGreaterThanOrEqual(actual);
    // partial context tightens but stays a bound
    expect(estimateEntryBytes(shape, { writer: "wA" })).toBeGreaterThanOrEqual(actual);
  });

  it("flags an over-limit payload before append does", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "wA");
    const big = { blob: "x".repeat(70_000) };
    expect(estimateEntryBytes({ topic: T, kind: "e", payload: big })).toBeGreaterThan(65_536);
    const p = node.log(T).append("e", big);
    p.catch(() => {}); // settled below — keep the pump free of unhandled noise
    await sched.run({ untilMs: 100 }); // size enforcement runs at commit time
    await expect(p).rejects.toMatchObject({ code: "ERR_ENTRY_TOO_LARGE" });
    await node.close();
  });
});

describe("P14/P15 — runtime topic activation on an established session", () => {
  it("defineTopic + updateGrants on both ends makes the topic mutual and syncs it", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(31);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    const link = new VirtualLink(sched, rng);
    const ha = a.node.attach(link.a, { peerId: "wB", peerClass: "content", grants: { [T]: "full" } });
    const hb = b.node.attach(link.b, { peerId: "wA", peerClass: "content", grants: { [T]: "full" } });
    await sched.run({ untilMs: 500 });
    expect(ha.state()).toBe("ready");

    // a topic discovered after boot, on both ends — no detach/reattach
    const T2 = "t.runtime";
    a.node.defineTopic(T2, FULL);
    b.node.defineTopic(T2, FULL);
    ha.updateGrants({ [T]: "full", [T2]: "full" });
    hb.updateGrants({ [T]: "full", [T2]: "full" });

    void a.node.log(T2).append("e", { n: 1 });
    void b.node.log(T).append("e", { n: 2 }); // the original topic keeps working
    await sched.run({ untilMs: 1_800 }); // < ANTI_ENTROPY_MS — driven by the update itself

    expect(ha.state()).toBe("ready"); // no redial happened
    expect(contigOf(b, "wA", T2)).toBe(1);
    expect(contigOf(a, "wB", T)).toBe(1);
    await Promise.all([a.node.close(), b.node.close()]);
  });

  it("a pre-connect backlog on the new topic converges via the triggered HAVE round", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(32);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    const T2 = "t.runtime";
    a.node.defineTopic(T2, FULL);
    for (let i = 0; i < 10; i++) void a.node.log(T2).append("e", { i });

    const link = new VirtualLink(sched, rng);
    const ha = a.node.attach(link.a, { peerId: "wB", peerClass: "content", grants: { [T]: "full" } });
    const hb = b.node.attach(link.b, { peerId: "wA", peerClass: "content", grants: { [T]: "full" } });
    await sched.run({ untilMs: 500 });

    b.node.defineTopic(T2, FULL);
    ha.updateGrants({ [T]: "full", [T2]: "full" });
    hb.updateGrants({ [T]: "full", [T2]: "full" });
    await sched.run({ untilMs: 1_900 }); // < ANTI_ENTROPY_MS
    expect(contigOf(b, "wA", T2)).toBe(10);
    await Promise.all([a.node.close(), b.node.close()]);
  });

  it("update-time schema mismatch refuses the topic without closing the session", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(33);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    const T2 = "t.mismatch";
    a.node.defineTopic(T2, FULL);
    b.node.defineTopic(T2, { ...FULL, kind: "register", conflict: { default: "fww" } });

    const link = new VirtualLink(sched, rng);
    const ha = a.node.attach(link.a, { peerId: "wB", peerClass: "content", grants: { [T]: "full" } });
    const hb = b.node.attach(link.b, { peerId: "wA", peerClass: "content", grants: { [T]: "full" } });
    await sched.run({ untilMs: 500 });

    ha.updateGrants({ [T]: "full", [T2]: "full" });
    hb.updateGrants({ [T]: "full", [T2]: "full" });
    void a.node.log(T2).append("e", { n: 1 });
    await sched.run({ untilMs: 2_500 });

    expect(ha.state()).toBe("ready"); // mismatch refuses the topic, not the session
    expect(contigOf(b, "wA", T2)).toBe(0); // refused — never synced
    expect(contigOf(b, "wA", T)).toBe(0); // and the healthy topic is unaffected
    void a.node.log(T).append("e", { n: 2 });
    await sched.run({ untilMs: 3_500 });
    expect(contigOf(b, "wA", T)).toBe(1);
    await Promise.all([a.node.close(), b.node.close()]);
  });

  it("updateGrants revalidates the §14 attach ACL guard", async () => {
    const sched = new Scheduler(0);
    const { node } = makeNode(sched, "wA");
    node.defineTopic("t.ring", {
      kind: "append",
      retention: { mode: "ring", size: 10 },
      replication: "subscribe-only",
      access: "content",
    });
    const h = node.attach(silentChannel(), {
      peerId: "wPeer",
      peerClass: "content",
      grants: { [T]: "full" },
    });
    expect(() => h.updateGrants({ [T]: "full", "t.ring": "full" })).toThrowError(/subscribe-only/);
    expect(() => h.updateGrants({ [T]: "full", "t.nope": "serve" })).toThrowError(
      /ERR_UNKNOWN_TOPIC/,
    );
    await node.close();
  });
});
