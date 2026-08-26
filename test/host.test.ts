// Host-side helpers: hmac authority (sign/verify/role binding), finality loop,
// reconnect manager, writerId persistence, legacy JSONL migration — plus the
// new attach grant validation, real owner lock, and stats() surface.

import Database from "better-sqlite3";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { fileHandle, memoryHandle } from "../harness/sqlite.js";
import {
  coreOf,
  createSeqscribe,
  hmacAuthority,
  loadOrCreateWriterId,
  manageReconnect,
  migrateLegacyJsonl,
  SeqscribeError,
  startFinalityLoop,
} from "../src/index.js";
import type {
  Channel,
  Constants,
  SeqscribeNodeExt,
  TopicPolicy,
  WriterDirective,
} from "../src/index.js";

const T = "t.ledger";
const AUTH = "fleet:authority";
const POLICY: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
  finalityAuthority: AUTH,
};
const TEST_CONSTANTS: Partial<Constants> = {
  FINALITY_WINDOW_MS: 50_000,
  ANTI_ENTROPY_MS: 1_000,
  CONTROL_RETRY_MS: 200,
};

const dir = mkdtempSync(join(tmpdir(), "seqscribe-host-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

function makeNode(
  sched: Scheduler,
  writerId: string,
  authority = hmacAuthority({ authorityId: AUTH, secret: "fleet-secret" }),
): SeqscribeNodeExt {
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.3,
    constants: TEST_CONSTANTS,
    authority,
  });
  node.defineTopic(T, POLICY);
  return node;
}

function connect(sched: Scheduler, rng: SeededRng, a: SeqscribeNodeExt, b: SeqscribeNodeExt) {
  const link = new VirtualLink(sched, rng);
  a.attach(link.a, { peerId: "pB", peerClass: "content", grants: { [T]: "full" } });
  b.attach(link.b, { peerId: "pA", peerClass: "content", grants: { [T]: "full" } });
}

describe("hmac authority + finality loop", () => {
  it("signs, verifies, propagates, and rejects tampering — end to end", async () => {
    const sched = new Scheduler(1_000_000);
    const rng = new SeededRng(91);
    const authority = hmacAuthority({ authorityId: AUTH, secret: "fleet-secret" });
    const a = makeNode(sched, "wA", authority);
    const b = makeNode(sched, "wB", authority);
    connect(sched, rng, a, b);
    for (let i = 0; i < 3; i++) void a.log(T).append("tx", { i });
    await sched.run({ untilMs: 1_001_000 });

    // the loop stays silent while the window is empty…
    const loop = startFinalityLoop(a, {
      topics: [T],
      authority,
      intervalMs: 5_000,
      timers: sched.timers(),
    });
    await sched.run({ untilMs: 1_020_000 });
    expect(a.finality(T)).toBeNull();

    // …and issues + propagates once entries age past the window
    await sched.run({ untilMs: 1_120_000 });
    expect(a.finality(T)?.generation).toBe(1);
    expect(b.finality(T)).toEqual(a.finality(T)); // propagated and hmac-verified on B
    loop.stop();

    // tampered cert is rejected by signature verification
    const forged = { ...a.finality(T)!, generation: 99 };
    await expect(b.ingestFinality(forged)).rejects.toThrow();
  });

  it("retires via the issuer hook and enforces role binding", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(92);
    // this authority governs only wA — retiring wB must fail verification
    const authority = hmacAuthority({
      authorityId: AUTH,
      secret: "fleet-secret",
      governs: (_t, w) => w === "wA",
    });
    const a = makeNode(sched, "wA", authority);
    const b = makeNode(sched, "wB", authority);
    connect(sched, rng, a, b);
    void a.log(T).append("tx", {});
    await sched.run({ untilMs: 3_000 });

    const p = a.retire("wA");
    p.catch(() => {});
    await sched.run({ untilMs: sched.now() + 3_000 });
    await p;
    expect(coreOf(a).getStream(T, "wA").sealReason).toBe("retired");
    expect(coreOf(b).getStream(T, "wA").sealReason).toBe("retired"); // directive verified on B

    // out-of-role directive: correctly signed but not governed → refused
    const rogue: WriterDirective = authority.signDirective({
      topic: T,
      writer: "wB",
      state: "retired",
      rgen: 1,
      finalSeq: 0,
      finalChain: "0".repeat(64),
    });
    const p2 = b.publishWriterDirective(rogue);
    p2.catch(() => {});
    await sched.run({ untilMs: sched.now() + 1_000 });
    await expect(p2).rejects.toThrow(/verification/);
  });
});

describe("reconnect manager", () => {
  it("redials with backoff after close and resets on healthy sessions", async () => {
    const sched = new Scheduler(0);
    const rng = new SeededRng(93);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");

    let dials = 0;
    let lastLink: VirtualLink | null = null;
    const dial = (): Channel => {
      dials++;
      const link = new VirtualLink(sched, rng.substream(`d${dials}`));
      lastLink = link;
      b.attach(link.b, { peerId: "pA", peerClass: "content", grants: { [T]: "full" } });
      return link.a;
    };
    const h = manageReconnect(a, {
      peerId: "pB",
      peerClass: "content",
      grants: { [T]: "full" },
      dial,
      backoff: { minMs: 300, maxMs: 5_000 },
      timers: sched.timers(),
      rng: rng.substream("jitter").fn(),
    });
    await sched.run({ untilMs: 500 });
    expect(h.current()?.state()).toBe("ready");
    expect(dials).toBe(1);

    // transport dies → manager redials; entries still converge afterwards
    lastLink!.a.close();
    await sched.run({ untilMs: 3_000 });
    expect(dials).toBeGreaterThanOrEqual(2);
    expect(h.current()?.state()).toBe("ready");
    void a.log(T).append("tx", { after: "reconnect" });
    await sched.run({ untilMs: 6_000 });
    expect(coreOf(b).getStream(T, "wA").contigSeq).toBe(1);

    h.stop();
    const before = dials;
    lastLink!.a.close();
    await sched.run({ untilMs: 20_000 });
    expect(dials).toBe(before); // stopped — no further dials
  });
});

describe("writerId persistence + legacy migration", () => {
  it("creates once and returns the same id thereafter", () => {
    const storage = memoryHandle();
    const id1 = loadOrCreateWriterId(storage, { prefix: "adhdev" });
    const id2 = loadOrCreateWriterId(storage);
    expect(id1).toMatch(/^adhdev-[0-9a-f]{16}$/);
    expect(id2).toBe(id1);
  });

  it("migrates legacy JSONL lines as fresh appends", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched, "wA");
    const legacy = [
      JSON.stringify({ type: "task_created", id: 1 }),
      JSON.stringify({ type: "task_done", id: 1 }),
      "",
      JSON.stringify({ type: "note", text: "hello" }),
    ];
    const p = migrateLegacyJsonl(node, T, legacy, {
      kind: (o) => String((o as { type?: string }).type ?? "event"),
    });
    await sched.run({ untilMs: 1_000 });
    expect(await p).toBe(3);
    const entries = coreOf(node).entries(T, "wA", 1, 3);
    expect(entries.map((e) => e.kind)).toEqual(["task_created", "task_done", "note"]);
    expect(entries[2]!.payload).toEqual({ type: "note", text: "hello" });
  });
});

describe("attach grant validation + owner lock + stats", () => {
  it("rejects a full grant on a subscribe-only topic", () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched, "wA");
    node.defineTopic("t.ring", {
      kind: "append",
      retention: { mode: "ring", size: 10 },
      replication: "subscribe-only",
      access: "content",
    });
    const rng = new SeededRng(94);
    const link = new VirtualLink(sched, rng);
    expect(() =>
      node.attach(link.a, { peerId: "p", peerClass: "content", grants: { "t.ring": "full" } }),
    ).toThrowError(SeqscribeError);
    node.attach(link.b, { peerId: "p2", peerClass: "content", grants: { "t.ring": "serve" } }); // fine
  });

  it("enforces cross-process ownership via the lock file", async () => {
    const path = join(dir, "locked.db");
    const first = createSeqscribe({ writerId: "wA", storage: fileHandle(path) });
    // second "process": a fresh connection pair to the same files
    expect(() => createSeqscribe({ writerId: "wA", storage: fileHandle(path) })).toThrowError(
      /ERR_DB_OWNED/,
    );
    await first.close();
    const third = createSeqscribe({ writerId: "wA", storage: fileHandle(path) }); // lock released
    await third.close();
  });

  it("exposes the host-guide baseline metrics via stats()", async () => {
    const sched = new Scheduler(1_000_000);
    const node = makeNode(sched, "wA");
    node.onEntry(T, "worker", () => {});
    for (let i = 0; i < 4; i++) void node.log(T).append("tx", { i });
    await sched.run({ untilMs: 1_001_000 });

    const s = node.stats();
    expect(s.topics[T]).toMatchObject({
      writers: 1,
      logRows: 4,
      pending: 0,
      quarantined: 0,
      archived: 0,
      finalityGeneration: null,
    });
    expect(s.topics[T]!.consumers.worker!.lagRows).toBe(0); // drained
    expect(s.peers).toEqual([]);
  });
});
