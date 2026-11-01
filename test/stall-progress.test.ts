// Stall detection counts ACK progress, not the age of the oldest in-flight
// frame. A slow (relay-like) link serializes the INFLIGHT_CREDITS frames written
// at once, so the last of them is legitimately ACKed several frame-times after
// it was handed to the channel; that must not close a lane that keeps making
// progress. A link that stops delivering must still close it.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe } from "../src/index.js";
import type { Channel, Constants, SeqscribeNode, TopicPolicy } from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
const C: Partial<Constants> = {
  ANTI_ENTROPY_MS: 600_000,
  CONTROL_RETRY_MS: 60_000, // no ARQ noise: this test is about the stall rule
  CHANNEL_STALL_MS: 3_000,
  HELLO_TIMEOUT_MS: 1_000,
};

function node(sched: Scheduler, id: string): SeqscribeNode {
  const n = createSeqscribe({ writerId: id, storage: memoryHandle(), clock: sched.clock(), timers: sched.timers(), constants: C });
  n.defineTopic(T, FULL);
  return n;
}

// Serializes outgoing bytes at `bytesPerSec` (a bandwidth-limited pipe).
function throttled(sched: Scheduler, inner: Channel, bytesPerSec: () => number): Channel {
  let busyUntil = 0;
  return {
    ...inner,
    send: (m) => {
      const start = Math.max(sched.now(), busyUntil);
      busyUntil = start + (m.length / bytesPerSec()) * 1000;
      sched.schedule(Math.ceil(busyUntil), () => inner.send(m));
    },
    onMessage: (cb) => inner.onMessage(cb),
    onClose: (cb) => inner.onClose(cb),
    close: () => inner.close(),
  };
}

const TAIL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "subscribe-only",
  access: "content",
};

// A server with a ~1 MiB tail served to a client over a bandwidth-limited pipe.
async function setup(bw: number) {
  const sched = new Scheduler(0);
  const rng = new SeededRng(21);
  const mk = (id: string) => {
    const n = createSeqscribe({ writerId: id, storage: memoryHandle(), clock: sched.clock(), timers: sched.timers(), constants: C });
    n.defineTopic("t.tail", TAIL);
    return n;
  };
  const server = mk("server");
  const client = mk("client");
  for (let i = 0; i < 16; i += 1) void server.log("t.tail").append("row", { i, pad: "x".repeat(60_000) });
  await sched.run({ untilMs: 200 });
  const link = new VirtualLink(sched, rng, { latency: () => 1 });
  const hs = server.attach(throttled(sched, link.a, () => bw), { peerId: "client", peerClass: "content", grants: { "t.tail": "serve" } });
  const hc = client.attach(link.b, { peerId: "server", peerClass: "content", grants: { "t.tail": "none" } });
  await sched.run({ untilMs: 400 });
  let rows = 0;
  const sub = client.subscribe(hc, { view: "tail", params: { topic: "t.tail" } });
  sub.onSnapshot((snap) => { rows = snap.length; });
  return { sched, link, hs, hc, rowCount: () => rows };
}

describe("stall = no ACK progress (§5.2)", () => {
  it("a chunked SNAP on a slow link that keeps ACKing is not closed although the in-flight window takes > CHANNEL_STALL_MS to drain", async () => {
    // ~1.3 MiB of SNAP chunks at 100 KB/s = ~13 s end to end, 4x CHANNEL_STALL_MS;
    // the 4 in-flight 256 KiB chunks written at once take ~10 s to be ACKed.
    const t = await setup(100_000);
    await t.sched.run({ untilMs: 30_000 });
    expect(t.hs.state()).toBe("ready");
    expect(t.rowCount()).toBe(16);
  });

  it("still closes when the link stops delivering mid-transfer", async () => {
    const t = await setup(100_000);
    await t.sched.run({ untilMs: 2_000 });
    t.link.cut(true);
    await t.sched.run({ untilMs: 20_000 });
    expect(t.hs.state()).toBe("closed");
  });
});
