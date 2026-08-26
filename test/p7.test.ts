// SPEC §19 P7 — the catch-up gate: 100 nodes, 200 topics, 10 writes/s
// fleet-wide, 60 s partition, 1% loss → convergence ≤120 s virtual after heal.
// This is a gate on CATCH-UP, not an envelope soak (§19 note 10).
//
// The fleet runs production-shaped constants; the host layer (dialing,
// redial-on-close, partition awareness) is played by the harness. The
// partition is HOST-VISIBLE (channels close; redials fail until the heal) —
// per §5/§14 the transport and reconnection are the host's, and a host that
// owns the channel sees it drop. A SILENT partition (no close signal) heals
// only at the anti-entropy cadence by design — see docs/proposals-v3.4.md.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe, jcs } from "../src/index.js";
import type { Constants, JsonValue, SeqscribeNode, TopicPolicy } from "../src/index.js";

const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

export interface P7Params {
  nodes: number;
  topics: number;
  writesPerSec: number;
  partitionStartMs: number;
  partitionEndMs: number;
  writesEndMs: number;
  gateMs: number; // convergence deadline after heal
  lossP: number;
  seed: number;
  constants: Partial<Constants>;
}

interface P7Result {
  convergedAtMs: number | null; // virtual ms after heal, null = missed the gate window
  writes: number;
  checkedUntil: number;
}

export async function runP7(p: P7Params): Promise<P7Result> {
  const sched = new Scheduler(0);
  const root = new SeededRng(p.seed);
  const topics = Array.from({ length: p.topics }, (_, i) => `p7.t${String(i).padStart(3, "0")}`);
  const grants = Object.fromEntries(topics.map((t) => [t, "full" as const]));

  const nodes: SeqscribeNode[] = [];
  for (let i = 0; i < p.nodes; i++) {
    const node = createSeqscribe({
      writerId: `w${String(i).padStart(2, "0")}`,
      storage: memoryHandle(),
      clock: sched.clock(),
      timers: sched.timers(),
      rng: root.substream(`node${i}`).fn(),
      constants: p.constants,
    });
    for (const t of topics) node.defineTopic(t, FULL);
    nodes.push(node);
  }

  // topology: ring + two chord sets → degree ~6, small diameter
  const pairs: [number, number][] = [];
  const seen = new Set<string>();
  const addPair = (a: number, b: number) => {
    const [x, y] = a < b ? [a, b] : [b, a];
    const k = `${x}-${y}`;
    if (x !== y && !seen.has(k)) {
      seen.add(k);
      pairs.push([x, y]);
    }
  };
  for (let i = 0; i < p.nodes; i++) {
    addPair(i, (i + 1) % p.nodes);
    addPair(i, (i + 7) % p.nodes);
    addPair(i, (i + 19) % p.nodes);
  }

  const half = Math.floor(p.nodes / 2);
  const crosses = (a: number, b: number) => (a < half) !== (b < half);
  let partitioned = false;
  const liveLinks = new Set<{ link: VirtualLink; a: number; b: number }>();

  const dial = (a: number, b: number, gen: number) => {
    const link = new VirtualLink(sched, root.substream(`link${a}-${b}-${gen}`), {
      lossP: p.lossP,
    });
    const rec = { link, a, b };
    liveLinks.add(rec);
    if (partitioned && crosses(a, b)) link.cut(true);
    const h = nodes[a]!.attach(link.a, {
      peerId: `w${String(b).padStart(2, "0")}`,
      peerClass: "content",
      grants,
    });
    nodes[b]!.attach(link.b, {
      peerId: `w${String(a).padStart(2, "0")}`,
      peerClass: "content",
      grants,
    });
    h.onStateChange((s) => {
      if (s === "closed") {
        liveLinks.delete(rec);
        sched.schedule(sched.now() + 5_000, () => dial(a, b, gen + 1)); // host redial backoff
      }
    });
  };
  for (const [a, b] of pairs) dial(a, b, 0);

  if (p.partitionEndMs > p.partitionStartMs) sched.schedule(p.partitionStartMs, () => {
    partitioned = true;
    for (const rec of liveLinks) {
      if (!crosses(rec.a, rec.b)) continue;
      // host-visible partition: the transport layer surfaces the disconnection
      // (both endpoints close; the host redials until the heal lets one through)
      rec.link.a.close();
      rec.link.b.close();
    }
  });
  sched.schedule(p.partitionEndMs, () => {
    partitioned = false;
    for (const rec of liveLinks) rec.link.cut(false);
  });

  // workload: writesPerSec fleet-wide, uniformly random node/topic
  const wl = root.substream("workload");
  let writes = 0;
  const acks: Promise<unknown>[] = [];
  for (let t = 5_000; t < p.writesEndMs; t += Math.round(1_000 / p.writesPerSec)) {
    const at = t;
    const who = wl.int(p.nodes);
    const topic = topics[wl.int(p.topics)]!;
    sched.schedule(at, () => {
      writes++;
      acks.push(nodes[who]!.log(topic).append("w", { at }).catch(() => undefined));
    });
  }

  const fingerprint = (n: SeqscribeNode): string => {
    const map: Record<string, Record<string, number>> = {};
    for (const [topic, v] of Object.entries(n.vectors())) {
      map[topic] = {};
      for (const [w, s] of Object.entries(v.writers))
        map[topic]![w] = "retired" in s ? s.finalSeq : s.contig;
    }
    return jcs(map as unknown as JsonValue);
  };
  const converged = (): boolean => {
    const first = fingerprint(nodes[0]!);
    for (let i = 1; i < p.nodes; i++) if (fingerprint(nodes[i]!) !== first) return false;
    return first !== "{}";
  };

  // sample convergence every 5 s virtual after heal
  const heal = p.partitionEndMs;
  let convergedAtMs: number | null = null;
  for (let t = heal; t <= heal + p.gateMs; t += 5_000) {
    await sched.run({ untilMs: t });
    if (converged()) {
      convergedAtMs = t - heal;
      break;
    }
  }
  await Promise.all(acks);
  const checkedUntil = sched.now();
  for (const n of nodes) await n.close();
  return { convergedAtMs, writes, checkedUntil };
}

export const SPEC_PROFILE: P7Params = {
  nodes: 100,
  topics: 200,
  writesPerSec: 10,
  partitionStartMs: 30_000,
  partitionEndMs: 90_000,
  writesEndMs: 90_000, // writes stop at heal — the gate measures catch-up of the backlog
  gateMs: 120_000,
  lossP: 0.01,
  seed: 20260826,
  constants: {}, // production constants
};

// Non-gating soak (§19 note 10): sustained load, no partition — run with SOAK=1.
const env = (globalThis as unknown as { process?: { env: Record<string, string | undefined> } })
  .process?.env;

describe.skipIf(!env?.SOAK)("soak profile (§19, non-gating)", () => {
  it(
    "sustains 10 writes/s across the fleet for 10 virtual minutes",
    async () => {
      const r = await runP7({
        ...SPEC_PROFILE,
        partitionStartMs: 0,
        partitionEndMs: 0, // no partition; "heal" at t=0 makes the sampler run from the start
        writesEndMs: 600_000,
        gateMs: 660_000,
        seed: 777,
      });
      // eslint-disable-next-line no-console
      console.log(`soak: ${r.writes} writes, converged at +${(r.convergedAtMs ?? -1) / 1000}s`);
      expect(r.convergedAtMs).not.toBeNull();
    },
    1_800_000,
  );
});

describe("P7 catch-up gate (§19)", () => {
  it(
    "100 nodes / 200 topics / 60 s partition / 1% loss converge ≤120 s after heal",
    async () => {
      const r = await runP7(SPEC_PROFILE);
      // eslint-disable-next-line no-console
      console.log(
        `P7: ${r.writes} writes, converged ${r.convergedAtMs === null ? "NOT within gate" : `at heal+${r.convergedAtMs / 1000}s`}`,
      );
      expect(r.convergedAtMs).not.toBeNull();
      expect(r.convergedAtMs!).toBeLessThanOrEqual(120_000);
    },
    600_000,
  );
});
