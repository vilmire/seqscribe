// §9 onEntry: at-least-once, serial, rowid order, cursor persistence, backoff.

import { describe, expect, it } from "vitest";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe, SeqscribeError } from "../src/index.js";
import type { LogEntry, SeqscribeNode, TopicPolicy } from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

function makeNode(sched: Scheduler): SeqscribeNode {
  const node = createSeqscribe({
    writerId: "w1",
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
  });
  node.defineTopic(T, FULL);
  return node;
}

describe("onEntry consumers", () => {
  it("delivers serially in rowid order", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const seen: number[] = [];
    node.onEntry(T, "c1", (e) => {
      seen.push((e.payload as { i: number }).i);
    });
    for (let i = 0; i < 10; i++) void node.log(T).append("note", { i });
    await sched.run();
    expect(seen).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it("does not advance the cursor past a failing callback, retries with backoff", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const seen: number[] = [];
    let failuresLeft = 2;
    node.onEntry(T, "c1", (e) => {
      const i = (e.payload as { i: number }).i;
      if (i === 2 && failuresLeft > 0) {
        failuresLeft--;
        throw new Error("flaky consumer");
      }
      seen.push(i);
    });
    for (let i = 0; i < 5; i++) void node.log(T).append("note", { i });
    await sched.run();
    // at-least-once: 2 was attempted three times, delivered once after retries
    expect(seen).toEqual([0, 1, 2, 3, 4]);
    expect(failuresLeft).toBe(0);
  });

  it("resumes from the persisted cursor after re-registration", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const first: number[] = [];
    const unsub = node.onEntry(T, "c1", (e) => {
      first.push((e.payload as { i: number }).i);
    });
    for (let i = 0; i < 3; i++) void node.log(T).append("note", { i });
    await sched.run();
    unsub();

    for (let i = 3; i < 6; i++) void node.log(T).append("note", { i });
    await sched.run();

    const second: number[] = [];
    node.onEntry(T, "c1", (e) => {
      second.push((e.payload as { i: number }).i);
    });
    await sched.run();
    expect(first).toEqual([0, 1, 2]);
    expect(second).toEqual([3, 4, 5]); // no redelivery of acknowledged entries
  });

  it("rejects registration on non-full retention and duplicate names", () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    node.defineTopic("t.ring", {
      kind: "append",
      retention: { mode: "ring", size: 10 },
      replication: "subscribe-only",
      access: "content",
    });
    expect(() => node.onEntry("t.ring", "c", () => {})).toThrowError(SeqscribeError);
    node.onEntry(T, "dup", () => {});
    expect(() => node.onEntry(T, "dup", () => {})).toThrowError(SeqscribeError);
  });

  it("delivers entries arriving via sync (provisional delivery)", async () => {
    const sched = new Scheduler(0);
    const node = makeNode(sched);
    const seen: LogEntry[] = [];
    node.onEntry(T, "c1", (e) => {
      seen.push(e);
    });
    // externally-applied entry (as if from a peer)
    const other = makeNode(sched);
    void other.log(T).append("note", { ext: true });
    await sched.run();
    // reuse other's entry — but writers must differ; craft via a second writer node
    const otherNode = createSeqscribe({
      writerId: "w2",
      storage: memoryHandle(),
      clock: sched.clock(),
      timers: sched.timers(),
    });
    otherNode.defineTopic(T, FULL);
    void otherNode.log(T).append("note", { ext: true });
    await sched.run();
    const { coreOf } = await import("../src/index.js");
    const e = coreOf(otherNode).entries(T, "w2", 1, 1)[0]!;
    void coreOf(node).applyExternal(e, "peer");
    await sched.run();
    expect(seen.some((x) => x.writer === "w2")).toBe(true);
  });
});
