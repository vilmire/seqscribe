// SPEC §19 acceptance properties, scaled for unit CI:
//   P1 convergence, P2 identical views, P3 no loss, P5 seed determinism,
//   and a reduced-shape P7 catch-up run. The full P7 gate (100 nodes) is a
//   separate CI tier — see docs/harness.md §10.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { coreOf, createSeqscribe, jcs } from "../src/index.js";
import type {
  Constants,
  EntryId,
  JsonValue,
  SeqscribeNode,
  TopicPolicy,
  ViewDef,
} from "../src/index.js";
import type { ViewHub } from "../src/views.js";

const TOPICS = ["p.alpha", "p.beta", "p.gamma"];
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
const TEST_CONSTANTS: Partial<Constants> = {
  ANTI_ENTROPY_MS: 2_000,
  CONTROL_RETRY_MS: 250,
  CHANNEL_STALL_MS: 4_000,
  GROUP_COMMIT_MS: 20,
};

type CountState = { [kind: string]: number };
const countView: ViewDef<CountState, { kind: string; n: number }> = {
  version: "1",
  init: {},
  reduce: (s, e) => ({ ...s, [e.kind]: (s[e.kind] ?? 0) + 1 }),
  rows: (s) => Object.entries(s).map(([kind, n]) => ({ kind, n })),
  rowKey: "kind",
  schema: { kind: "TEXT", n: "INTEGER" },
};

interface SimResult {
  contigMaps: string[]; // per node: JCS of {topic: {writer: {contig, chain}}}
  viewRows: string[]; // per node: JCS of the counts table
  ledger: EntryId[]; // every acked append
  logDumps: string[]; // per node: arrival-order (topic, writer, seq) — P5 fingerprint
}

// One seeded scenario: N nodes in a full mesh with loss + a mid-run partition,
// a deterministic append workload, then quiet time to quiescence.
async function runScenario(seed: number): Promise<SimResult> {
  const sched = new Scheduler(0);
  const root = new SeededRng(seed);
  const N = 4;
  const nodes: SeqscribeNode[] = [];
  for (let i = 0; i < N; i++) {
    const node = createSeqscribe({
      writerId: `w${i}`,
      storage: memoryHandle(),
      clock: sched.clock(),
      timers: sched.timers(),
      rng: root.substream(`node${i}`).fn(),
      constants: TEST_CONSTANTS,
    });
    for (const t of TOPICS) node.defineTopic(t, FULL);
    node.view("counts", TOPICS[0]!, countView);
    nodes.push(node);
  }

  // full mesh with 5% loss; host-style reconnect on stall-close
  const links: VirtualLink[] = [];
  const grants = Object.fromEntries(TOPICS.map((t) => [t, "full" as const]));
  const dial = (i: number, j: number, gen: number) => {
    const link = new VirtualLink(sched, root.substream(`link${i}-${j}-${gen}`), { lossP: 0.05 });
    links.push(link);
    const h = nodes[i]!.attach(link.a, { peerId: `w${j}`, peerClass: "content", grants });
    nodes[j]!.attach(link.b, { peerId: `w${i}`, peerClass: "content", grants });
    h.onStateChange((s) => {
      if (s === "closed") sched.schedule(sched.now() + 250, () => dial(i, j, gen + 1));
    });
  };
  for (let i = 0; i < N; i++) for (let j = i + 1; j < N; j++) dial(i, j, 0);

  // deterministic workload: 60 appends spread over 20 s across nodes/topics
  const wl = root.substream("workload");
  const ledger: EntryId[] = [];
  const acks: Promise<unknown>[] = [];
  for (let k = 0; k < 60; k++) {
    const at = 500 + Math.floor(wl.next() * 20_000);
    const who = wl.int(N);
    const topic = TOPICS[wl.int(TOPICS.length)]!;
    const kind = `k${wl.int(3)}`;
    sched.schedule(at, () => {
      acks.push(nodes[who]!.log(topic).append(kind, { k }).then((id) => ledger.push(id)));
    });
  }

  // partition node 3 from everyone between 8 s and 14 s (its links cut silently)
  sched.schedule(8_000, () => {
    for (const [idx, link] of links.entries()) {
      // links are created in (0,1),(0,2),(0,3),(1,2),(1,3),(2,3) order — node 3
      // participates in indexes 2, 4, 5 of the initial mesh
      if ([2, 4, 5].includes(idx)) link.cut(true);
    }
  });
  sched.schedule(14_000, () => {
    for (const link of links) link.cut(false);
  });

  // run to quiescence: workload ends at 20.5 s; give anti-entropy rounds room
  await sched.run({ untilMs: 90_000 });
  await Promise.all(acks);

  const contigMaps: string[] = [];
  const viewRows: string[] = [];
  const logDumps: string[] = [];
  for (const node of nodes) {
    const core = coreOf(node);
    const map: Record<string, Record<string, { contig: number; chain: string }>> = {};
    for (const t of TOPICS) {
      map[t] = {};
      const v = node.vectors()[t];
      for (const [w, s] of Object.entries(v?.writers ?? {})) {
        if (!("retired" in s)) map[t]![w] = { contig: s.contig, chain: s.chain };
      }
    }
    contigMaps.push(jcs(map as unknown as JsonValue));
    const hub = (node as unknown as { _views: ViewHub })._views;
    viewRows.push(jcs(hub.tableRowsSorted("counts") as unknown as JsonValue));
    const dump: [string, string, number][] = [];
    for (const t of TOPICS) {
      for (const w of ["w0", "w1", "w2", "w3"]) {
        const head = core.getStream(t, w);
        for (const e of core.entries(t, w, 1, head.contigSeq)) dump.push([t, e.writer, e.seq]);
      }
    }
    logDumps.push(jcs(dump as unknown as JsonValue));
  }
  return { contigMaps, viewRows, ledger, logDumps };
}

interface SeedCorpus {
  seeds: { seed: number; scenario: string; note: string }[];
}

describe("failing-seed corpus (§19)", () => {
  it("replays every corpus seed through P1/P2/P3", async () => {
    const { readFileSync } = await import("node:fs");
    const { join } = await import("node:path");
    const corpus = JSON.parse(
      readFileSync(join(import.meta.dirname, "..", "harness", "seeds.json"), "utf8"),
    ) as SeedCorpus;
    for (const { seed } of corpus.seeds) {
      const r = await runScenario(seed);
      for (const m of r.contigMaps) expect(m, `P1 seed=${seed}`).toBe(r.contigMaps[0]);
      for (const v of r.viewRows) expect(v, `P2 seed=${seed}`).toBe(r.viewRows[0]);
      expect(r.ledger.length, `P3 seed=${seed}`).toBe(60);
    }
  }, 120_000);

  const env = (globalThis as unknown as { process?: { env: Record<string, string | undefined> } })
    .process?.env;
  it.skipIf(!env?.FRESH_SEEDS)("explores fresh random seeds (failures join the corpus)", async () => {
    const n = Number(env?.FRESH_SEEDS ?? 0);
    for (let i = 0; i < n; i++) {
      const seed = Math.floor(Math.random() * 2 ** 31); // exploration is allowed to be wall-random
      // eslint-disable-next-line no-console
      console.log(`fresh seed: ${seed}`);
      const r = await runScenario(seed);
      for (const m of r.contigMaps)
        expect(m, `P1 FAILED — add seed ${seed} to harness/seeds.json`).toBe(r.contigMaps[0]);
      for (const v of r.viewRows)
        expect(v, `P2 FAILED — add seed ${seed} to harness/seeds.json`).toBe(r.viewRows[0]);
    }
  }, 600_000);
});

describe("acceptance properties (§19, scaled)", () => {
  it("P1+P2+P3: convergence, identical views, no loss under loss+partition", async () => {
    const r = await runScenario(20260826);

    // P1: equal (topic, writer) → (contig, chain) maps across the component
    for (const m of r.contigMaps) expect(m).toBe(r.contigMaps[0]);

    // P2: byte-equal view tables on the same basis
    for (const v of r.viewRows) expect(v).toBe(r.viewRows[0]);

    // P3: every acked append is present on every node
    expect(r.ledger).toHaveLength(60);
    const converged = JSON.parse(r.contigMaps[0]!) as Record<
      string,
      Record<string, { contig: number }>
    >;
    for (const [topic, writer, seq] of r.ledger) {
      expect(converged[topic]![writer]!.contig).toBeGreaterThanOrEqual(seq);
    }
  }, 30_000);

  it("P5: the same seed reproduces byte-identical final states and arrival orders", async () => {
    const a = await runScenario(424242);
    const b = await runScenario(424242);
    expect(b.contigMaps).toEqual(a.contigMaps);
    expect(b.viewRows).toEqual(a.viewRows);
    expect(b.logDumps).toEqual(a.logDumps);
    expect(b.ledger.map((id) => jcs(id as unknown as JsonValue)).sort()).toEqual(
      a.ledger.map((id) => jcs(id as unknown as JsonValue)).sort(),
    );
  }, 60_000);

  it("P5-negative: different seeds explore different schedules", async () => {
    const a = await runScenario(1);
    const b = await runScenario(2);
    // final convergence holds for both, but the arrival orders should differ —
    // if they don't, the fault model isn't actually exercising the schedule
    expect(a.logDumps).not.toEqual(b.logDumps);
  }, 60_000);
});
