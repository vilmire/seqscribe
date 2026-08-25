// §5.7 beacon: debounced content-free vector reports + staleness prediction.

import { describe, expect, it } from "vitest";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe } from "../src/index.js";
import type { BeaconReport, BeaconTransport, SeqscribeNode, TopicPolicy } from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

function makeNode(sched: Scheduler, writerId: string): SeqscribeNode {
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
  });
  node.defineTopic(T, FULL);
  return node;
}

function memoryBeacon(): { transport: BeaconTransport; board: Map<string, BeaconReport> } {
  const board = new Map<string, BeaconReport>();
  return {
    board,
    transport: {
      put: async (r) => {
        board.set(r.node, r); // one report per node, overwrite (§14)
      },
      get: async () => [...board.values()],
    },
  };
}

describe("beacon (§5.7)", () => {
  it("debounces reports and computes staleness from known vectors", async () => {
    const sched = new Scheduler(0);
    const { transport, board } = memoryBeacon();
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    a.beacon(transport);
    b.beacon(transport);
    await sched.run({ untilMs: 100 });
    expect(board.size).toBe(2);

    // A appends while B is "offline" from A's perspective (no link at all)
    for (let i = 0; i < 7; i++) void a.log(T).append("note", { i });
    await sched.run({ untilMs: 10_000 }); // past BEACON_DEBOUNCE_MS
    expect(board.get("wA")!.vectors[T]!.writers.wA).toMatchObject({ contig: 7 });

    // B learns the fleet vectors from the beacon and predicts its lag
    b.setKnownVectors([...board.values()]);
    const s = b.staleness(T);
    expect(s.behind.wA).toBe(7);

    // sole-copy awareness inverse: A is behind nobody
    a.setKnownVectors([...board.values()]);
    expect(a.staleness(T).behind.wA ?? 0).toBe(0);
  });

  it("stops cleanly and refuses double start", async () => {
    const sched = new Scheduler(0);
    const { transport } = memoryBeacon();
    const a = makeNode(sched, "wA");
    const h = a.beacon(transport);
    expect(() => a.beacon(transport)).toThrowError(/beacon already started/);
    h.stop();
    await sched.run();
  });
});
