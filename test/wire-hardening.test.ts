// Wire/ingress hardening: parseMsg structural validation for every handled
// message type (§5.4), the ERR-and-drop discipline for handler failures
// (nothing throws into the host's Channel.onMessage callback), the §5.2
// credit-window bound on recvBuffer, and hostile HAVE vectors.

import { describe, expect, it } from "vitest";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { b64encode, createSeqscribe, parseMsg, resolveConstants, serializeMsg } from "../src/index.js";
import type {
  Anomaly,
  Channel,
  Constants,
  PeerHandle,
  SeqscribeNode,
  TopicPolicy,
  WireMsg,
} from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
const C = resolveConstants();
const HEX64 = "ab".repeat(32);

// ---- parseMsg unit level: schema derived from what the emit sites produce ----

describe("parseMsg round-trips every frame this implementation emits", () => {
  const valid: WireMsg[] = [
    {
      t: "HELLO",
      protoMin: 1,
      protoMax: 1,
      node: "wA",
      grants: { [T]: { mode: "full", schemaHash: HEX64 } },
    },
    { t: "HAVE_GET", req: 1 },
    // paginateHave: empty-writers page carrying only fgen; live and retired heads
    {
      t: "HAVE",
      req: 1,
      page: 1,
      of: 2,
      vectors: {
        [T]: {
          fgen: 3,
          writers: {
            wA: { contig: 0, chain: HEX64 }, // fresh stream advertises its seed
            wB: { contig: 7, chain: HEX64, rgen: 1 },
            wC: { retired: true, finalSeq: 9, finalChain: HEX64, rgen: 2 },
          },
        },
        "t.empty": { writers: {} },
      },
    },
    { t: "WANT", req: 2, topic: T, writer: "wA", fromSeq: 1 },
    // serveWant empty completion (toSeq 0 below fromSeq) and pumpDirty batches
    { t: "ENTRIES", mid: 1, req: 2, topic: T, writer: "wA", fromSeq: 1, toSeq: 0, entries: [], done: true },
    { t: "ENTRIES", mid: 2, topic: T, writer: "wA", fromSeq: 3, toSeq: 5, entries: [], done: false },
    { t: "PROBE", topic: T, writer: "wA", seqs: [8, 7, 6, 4] },
    {
      t: "PROBE_RES",
      topic: T,
      writer: "wA",
      points: [{ seq: 4, chain: HEX64 }],
      unavailable: { belowSeq: 2 },
    },
    {
      t: "FINALITY",
      topic: T,
      cert: {
        topic: T,
        order: { l: 5, c: 0, writer: "wA", seq: 3 },
        cut: { wA: { seq: 3, chain: HEX64 } },
        generation: 1,
        authority: "test:authority",
        sig: "valid-sig",
      },
    },
    {
      t: "WRITER_DIRECTIVE",
      directive: {
        topic: T,
        writer: "wA",
        state: "retired",
        rgen: 1,
        finalSeq: 3,
        finalChain: HEX64,
        authority: "test:authority",
        sig: "valid-sig",
      },
    },
    {
      t: "SNAPSHOT_OFFER",
      topic: T,
      snapshotId: HEX64,
      order: { l: 5, c: 0, writer: "wA", seq: 3 },
      cut: { wA: { seq: 3, chain: HEX64 } },
      certHash: HEX64,
    },
    { t: "SNAPSHOT_GET", topic: T, snapshotId: HEX64, wants: [{ name: "v", version: "1" }] },
    { t: "SNAPSHOT", mid: 3, topic: T, snapshotId: HEX64, chunk: 1, of: 2, data: "AA==", totalHash: HEX64 },
    { t: "SUB", subId: 1, view: "tail", params: { topic: T }, fromCursor: '{"e":"x","d":0}' },
    { t: "SNAP", mid: 4, subId: 1, chunk: 1, of: 1, data: "AA==", cursor: "c", reset: true },
    { t: "DELTA", mid: 5, subId: 1, changes: { upserts: [{ key: "k", n: 1 }], deletes: ["x"] }, cursor: "c" },
    { t: "UNSUB", subId: 1 },
    { t: "SUB_ERR", subId: 1, code: "ERR_FUTURE_CURSOR" },
    { t: "ACK", upTo: 0 },
    { t: "ERR", code: "ERR_SCHEMA_MISMATCH", ref: T, detail: "d" },
  ];

  for (const m of valid) {
    it(`accepts emitted-shape ${m.t}`, () => {
      expect(parseMsg(serializeMsg(m, C), C)).toEqual(m);
    });
  }

  it("accepts SUB with absent params (JSON.stringify drops undefined)", () => {
    expect(() => parseMsg('{"t":"SUB","subId":1,"view":"v"}', C)).not.toThrow();
  });
});

describe("parseMsg rejects malformed frames of every handled type", () => {
  const bad: [string, string][] = [
    ["junk JSON", "not json {"],
    ["non-object frame", "[1,2,3]"],
    ["missing type", '{"req":1}'],
    ["unknown type", '{"t":"NOPE"}'],
    ["data frame without mid", `{"t":"ENTRIES","topic":"${T}","writer":"wA"}`],
    ["HELLO without grants", '{"t":"HELLO","protoMin":1,"protoMax":1,"node":"wA"}'],
    ["HELLO bad grant mode", `{"t":"HELLO","protoMin":1,"protoMax":1,"node":"wA","grants":{"${T}":{"mode":"root","schemaHash":"x"}}}`],
    ["HELLO non-writer node", '{"t":"HELLO","protoMin":1,"protoMax":1,"node":"no spaces","grants":{}}'],
    ["HAVE_GET string req", '{"t":"HAVE_GET","req":"1"}'],
    ["HAVE without vectors (the review reproducer)", '{"t":"HAVE","req":1,"page":1,"of":1}'],
    ["HAVE page beyond of", `{"t":"HAVE","req":1,"page":3,"of":2,"vectors":{}}`],
    ["HAVE writer head missing chain", `{"t":"HAVE","req":1,"page":1,"of":1,"vectors":{"${T}":{"writers":{"wA":{"contig":1}}}}}`],
    ["HAVE invalid writer name", `{"t":"HAVE","req":1,"page":1,"of":1,"vectors":{"${T}":{"writers":{"bad writer":{"contig":1,"chain":"${HEX64}"}}}}}`],
    ["HAVE __proto__ writer key", `{"t":"HAVE","req":1,"page":1,"of":1,"vectors":{"${T}":{"writers":{"__proto__":{"contig":1,"chain":"${HEX64}"}}}}}`],
    ["WANT without topic", '{"t":"WANT","req":1,"writer":"wA","fromSeq":1}'],
    ["WANT fromSeq 0", `{"t":"WANT","req":1,"topic":"${T}","writer":"wA","fromSeq":0}`],
    ["ENTRIES entries not an array", `{"t":"ENTRIES","mid":1,"topic":"${T}","writer":"wA","fromSeq":1,"toSeq":1,"entries":{},"done":true}`],
    ["ENTRIES done not boolean", `{"t":"ENTRIES","mid":1,"topic":"${T}","writer":"wA","fromSeq":1,"toSeq":1,"entries":[],"done":"yes"}`],
    ["PROBE seq 0", `{"t":"PROBE","topic":"${T}","writer":"wA","seqs":[0]}`],
    ["PROBE_RES point without chain", `{"t":"PROBE_RES","topic":"${T}","writer":"wA","points":[{"seq":1}]}`],
    ["FINALITY null cert", `{"t":"FINALITY","topic":"${T}","cert":null}`],
    ["FINALITY cert without authority", `{"t":"FINALITY","topic":"${T}","cert":{"topic":"${T}","order":{},"cut":{},"generation":1,"sig":"s"}}`],
    ["WRITER_DIRECTIVE bare directive", `{"t":"WRITER_DIRECTIVE","directive":{"topic":"${T}"}}`],
    ["SNAPSHOT_OFFER without cut", `{"t":"SNAPSHOT_OFFER","topic":"${T}","snapshotId":"${HEX64}","order":{},"certHash":"${HEX64}"}`],
    ["SNAPSHOT_GET non-hex snapshotId", `{"t":"SNAPSHOT_GET","topic":"${T}","snapshotId":"xyz","wants":[]}`],
    ["SNAPSHOT chunk beyond of", `{"t":"SNAPSHOT","mid":1,"topic":"${T}","snapshotId":"${HEX64}","chunk":9,"of":2,"data":""}`],
    ["SUB numeric view", '{"t":"SUB","subId":1,"view":7}'],
    ["SNAP chunk beyond of", '{"t":"SNAP","mid":1,"subId":1,"chunk":2,"of":1,"data":"","cursor":"c","reset":false}'],
    ["SNAP without cursor", '{"t":"SNAP","mid":1,"subId":1,"chunk":1,"of":1,"data":"","reset":false}'],
    ["DELTA upserts not rows", '{"t":"DELTA","mid":1,"subId":1,"changes":{"upserts":[1],"deletes":[]},"cursor":"c"}'],
    ["DELTA deletes not strings", '{"t":"DELTA","mid":1,"subId":1,"changes":{"upserts":[],"deletes":[1]},"cursor":"c"}'],
    ["UNSUB without subId", '{"t":"UNSUB"}'],
    ["SUB_ERR numeric code", '{"t":"SUB_ERR","subId":1,"code":123}'],
    ["ACK string upTo", '{"t":"ACK","upTo":"9"}'],
    ["ACK negative upTo", '{"t":"ACK","upTo":-1}'],
    ["ERR empty code", '{"t":"ERR","code":""}'],
  ];

  for (const [name, raw] of bad) {
    it(`rejects ${name}`, () => {
      expect(() => parseMsg(raw, C)).toThrowError(/ERR_ENTRY_ENCODING/);
    });
  }

  it("rejects oversized frames", () => {
    const raw = `{"t":"ACK","upTo":1,"pad":"${"x".repeat(C.MAX_FRAME_BYTES)}"}`;
    expect(() => parseMsg(raw, C)).toThrowError(/MAX_FRAME_BYTES/);
  });
});

// ---- session level: the host's transport callback never sees an exception ----

interface Harness {
  node: SeqscribeNode;
  handle: PeerHandle;
  anomalies: Anomaly[];
  sent: string[];
  frames: () => { t: string; code?: string }[];
  inject: (raw: string) => void; // the host's onMessage callback, verbatim
  ready: () => void; // answer the session's HELLO so it negotiates to ready
  close: () => void;
}

function makeHarness(constants: Partial<Constants> = {}): Harness {
  const sched = new Scheduler(1_000_000);
  const anomalies: Anomaly[] = [];
  const node = createSeqscribe({
    writerId: "wLocal",
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    constants,
  });
  node.onAnomaly((a) => anomalies.push(a));
  node.defineTopic(T, FULL);

  const sent: string[] = [];
  let onMsg: ((m: string) => void) | null = null;
  const ch: Channel = {
    send: (m) => sent.push(m),
    onMessage: (cb) => (onMsg = cb),
    onClose: () => {},
    close: () => {},
  };
  const handle = node.attach(ch, { peerId: "wPeer", peerClass: "content", grants: { [T]: "full" } });
  const frames = () => sent.map((s) => JSON.parse(s) as { t: string; code?: string });
  return {
    node,
    handle,
    anomalies,
    sent,
    frames,
    inject: (raw) => onMsg!(raw),
    ready: () => {
      // echo our own HELLO's grants back (identical schemaHash) as the peer
      const hello = frames().find((f) => f.t === "HELLO") as unknown as {
        grants: Record<string, { mode: string; schemaHash: string }>;
      };
      onMsg!(
        JSON.stringify({ t: "HELLO", protoMin: 1, protoMax: 1, node: "wPeer", grants: hello.grants }),
      );
    },
    close: () => void node.close(),
  };
}

function errsAfter(h: Harness, before: number): { t: string; code?: string }[] {
  return h.frames().slice(before).filter((f) => f.t === "ERR");
}

describe("Session ERR-and-drop discipline (no throw into the transport callback)", () => {
  it("answers each malformed frame with ERR and stays ready", () => {
    const h = makeHarness();
    h.ready();
    expect(h.handle.state()).toBe("ready");
    const malformed = [
      "junk {",
      '{"t":"NOPE"}',
      '{"t":"HAVE","req":1}', // the review reproducer — used to TypeError in onHavePage
      '{"t":"ACK","upTo":"nope"}',
      '{"t":"WANT","req":1}',
      `{"t":"ENTRIES","mid":1,"topic":"${T}","writer":"wPeer","fromSeq":1,"toSeq":1,"entries":{},"done":true}`,
      '{"t":"SNAP","mid":1,"subId":1,"chunk":9,"of":2,"data":"","cursor":"c","reset":false}',
      `{"t":"FINALITY","topic":"${T}","cert":null}`,
      '{"t":"WRITER_DIRECTIVE","directive":{"topic":"zzz"}}',
      '{"t":"SUB","subId":"one","view":"tail"}',
    ];
    for (const raw of malformed) {
      const before = h.sent.length;
      expect(() => h.inject(raw)).not.toThrow();
      expect(errsAfter(h, before).length).toBe(1);
      expect(errsAfter(h, before)[0]!.code).toBe("ERR_ENTRY_ENCODING");
      expect(h.handle.state()).toBe("ready"); // drop, not close — same as parse errors
    }
    h.close();
  });

  it("contains a handler exception from a hostile SNAP body (ERR, not a crash)", () => {
    const h = makeHarness();
    h.ready();
    const sub = h.node.subscribe(h.handle, { view: "tail", params: { topic: T } });
    let snapshots = 0;
    sub.onSnapshot(() => snapshots++);

    // structurally valid SNAP whose reassembled body is not JSON at all
    const junk = b64encode(new TextEncoder().encode("!not json!"));
    let before = h.sent.length;
    expect(() =>
      h.inject(`{"t":"SNAP","mid":1,"subId":1,"chunk":1,"of":1,"data":"${junk}","cursor":"c","reset":true}`),
    ).not.toThrow();
    expect(errsAfter(h, before).length).toBe(1);
    expect(h.handle.state()).toBe("ready");
    expect(snapshots).toBe(0);

    // valid JSON but not a row array — rejected before subscriber callbacks
    const obj = b64encode(new TextEncoder().encode('{"a":1}'));
    before = h.sent.length;
    expect(() =>
      h.inject(`{"t":"SNAP","mid":2,"subId":1,"chunk":1,"of":1,"data":"${obj}","cursor":"c","reset":true}`),
    ).not.toThrow();
    expect(errsAfter(h, before)[0]!.code).toBe("ERR_ENTRY_ENCODING");
    expect(snapshots).toBe(0);

    // a well-formed body still flows to the callback (the guard is not a filter)
    const rows = b64encode(new TextEncoder().encode('[{"key":"k","n":1}]'));
    h.inject(`{"t":"SNAP","mid":3,"subId":1,"chunk":1,"of":1,"data":"${rows}","cursor":"c2","reset":true}`);
    expect(snapshots).toBe(1);
    expect(sub.cursor).toBe("c2");
    h.close();
  });
});

describe("recvBuffer credit-window bound (§5.2)", () => {
  const entriesFrame = (mid: number) =>
    `{"t":"ENTRIES","mid":${mid},"topic":"${T}","writer":"wPeer","fromSeq":1,"toSeq":0,"entries":[],"done":true}`;

  it("buffers out-of-order mids up to INFLIGHT_CREDITS above contig", () => {
    const h = makeHarness();
    h.ready();
    // contig 0: mids 2..4 buffer (nothing contiguous yet), mid 1 releases all
    for (const mid of [2, 3, 4]) expect(() => h.inject(entriesFrame(mid))).not.toThrow();
    expect(h.handle.state()).toBe("ready");
    const before = h.sent.length;
    h.inject(entriesFrame(1));
    const acks = h.frames().slice(before).filter((f) => f.t === "ACK") as unknown as { upTo: number }[];
    expect(acks.length).toBe(1);
    expect(acks[0]!.upTo).toBe(4); // drained to contiguous
    h.close();
  });

  it("closes the session on a mid beyond the credit window", () => {
    const h = makeHarness();
    h.ready();
    const before = h.sent.length;
    expect(() => h.inject(entriesFrame(5))).not.toThrow(); // contig 0 + credits 4 < 5
    expect(errsAfter(h, before).length).toBe(1);
    expect(errsAfter(h, before)[0]!.code).toBe("ERR_ENTRY_ENCODING");
    expect(h.handle.state()).toBe("closed"); // conservative: misbehaving peer, host redials
    h.close();
  });

  it("keeps honoring the window as contig advances", () => {
    const h = makeHarness();
    h.ready();
    for (const mid of [1, 2, 3]) h.inject(entriesFrame(mid)); // contig now 3
    expect(() => h.inject(entriesFrame(7))).not.toThrow(); // 3 + 4 — at the edge, buffered
    expect(h.handle.state()).toBe("ready");
    h.inject(entriesFrame(8)); // 3 + 4 < 8 — over
    expect(h.handle.state()).toBe("closed");
    h.close();
  });
});

describe("hostile HAVE vectors", () => {
  it("valid unknown writers are trusted census (no ERR, no crash), fed to sync", () => {
    const h = makeHarness();
    h.ready();
    const req = (h.frames().find((f) => f.t === "HAVE_GET") as unknown as { req: number }).req;
    const before = h.sent.length;
    const vectors = {
      [T]: {
        writers: {
          wGhost1: { contig: 2, chain: HEX64 },
          wGhost2: { contig: 5, chain: HEX64 },
        },
      },
    };
    expect(() =>
      h.inject(JSON.stringify({ t: "HAVE", req, page: 1, of: 1, vectors })),
    ).not.toThrow();
    expect(errsAfter(h, before).length).toBe(0);
    expect(h.handle.state()).toBe("ready");
    // deficits turned into WANTs — the vector was processed, not dropped
    const wants = h.frames().slice(before).filter((f) => f.t === "WANT");
    expect(wants.length).toBeGreaterThan(0);
    h.close();
  });

  it("rejects charter-violating and __proto__ writer names without pollution", () => {
    const h = makeHarness();
    h.ready();
    const req = (h.frames().find((f) => f.t === "HAVE_GET") as unknown as { req: number }).req;
    const hostile = [
      `{"t":"HAVE","req":${req},"page":1,"of":1,"vectors":{"${T}":{"writers":{"evil writer!":{"contig":1,"chain":"${HEX64}"}}}}}`,
      `{"t":"HAVE","req":${req},"page":1,"of":1,"vectors":{"${T}":{"writers":{"__proto__":{"contig":1,"chain":"${HEX64}"}}}}}`,
      `{"t":"HAVE","req":${req},"page":1,"of":1,"vectors":{"__proto__":{"writers":{}}}}`,
      `{"t":"HAVE","req":${req},"page":1,"of":1,"vectors":{"${T}":{"writers":{"wX":{"contig":1,"chain":"deadbeef"}}}}}`, // short chain
    ];
    for (const raw of hostile) {
      const before = h.sent.length;
      expect(() => h.inject(raw)).not.toThrow();
      expect(errsAfter(h, before).length).toBe(1);
      expect(h.handle.state()).toBe("ready");
    }
    expect(({} as { contig?: unknown }).contig).toBeUndefined(); // Object.prototype untouched
    h.close();
  });
});
