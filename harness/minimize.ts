// Failing-seed minimizer core (docs/harness.md §10 step 4): a parameterized
// scenario runner plus greedy schedule-slicing — drop fault windows / workload
// segments (and shrink what remains) while the target property still fails.
// Consumed by tools/minimize-seed.mjs; properties.test.ts keeps its own inline
// scenario (byte-identical corpus behavior) until it adopts this module.
//
// Determinism: the search order is purely structural (array order, halving),
// and every candidate is re-validated by a fresh seeded run — no wall clock,
// no Math.random anywhere.

import { VirtualLink } from "./bus.js";
import { SeededRng } from "./rng.js";
import { Scheduler } from "./scheduler.js";
import { memoryHandle } from "./sqlite.js";
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

// ---------- scenario parameterization ----------

/** Partition one node from everyone during [fromMs, toMs) — links cut silently. */
export interface FaultWindow {
  fromMs: number;
  toMs: number;
  node: number;
}

/** A batch of appends spread uniformly over [fromMs, toMs). The stable `id`
 * keys the segment's RNG substream, so dropping one segment cannot shift the
 * draws of any other (docs/harness.md §2). */
export interface WorkloadSegment {
  id: string;
  fromMs: number;
  toMs: number;
  count: number;
}

export interface ScenarioParams {
  nodes: number;
  lossP: number;
  faultWindows: FaultWindow[];
  segments: WorkloadSegment[];
}

export type PropertyId = "P1" | "P2" | "P3" | "P5";

/** Run the scenario under `seed` with `params`; return the failing properties
 * (deterministic order P1, P2, P3, P5). `target` — when the minimizer already
 * knows which property it is chasing — lets the check skip the second run that
 * only P5 needs. */
export type RunCheck = (
  seed: number,
  params: ScenarioParams,
  target?: PropertyId,
) => Promise<PropertyId[]>;

export interface ScenarioDef {
  name: string;
  baseline: ScenarioParams;
  /** Node-count slicing floor (a mesh needs 2 to sync at all). */
  minNodes: number;
  check: RunCheck;
}

// ---------- generalized mesh scenario (superset of properties.test.ts) ----------

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
const SETTLE_MS = 70_000; // post-workload quiet time to quiescence

type CountState = { [kind: string]: number };
const countView: ViewDef<CountState, { kind: string; n: number }> = {
  version: "1",
  init: {},
  reduce: (s, e) => ({ ...s, [e.kind]: (s[e.kind] ?? 0) + 1 }),
  rows: (s) => Object.entries(s).map(([kind, n]) => ({ kind, n })),
  rowKey: "kind",
  schema: { kind: "TEXT", n: "INTEGER" },
};

export interface SimResult {
  contigMaps: string[]; // per node: JCS of {topic: {writer: {contig, chain}}}
  viewRows: string[]; // per node: JCS of the counts table
  ledger: EntryId[]; // every acked append
  logDumps: string[]; // per node: arrival-order (topic, writer, seq)
  totalAppends: number; // appends attempted (= sum of segment counts)
}

export async function runScenario(seed: number, params: ScenarioParams): Promise<SimResult> {
  const sched = new Scheduler(0);
  const root = new SeededRng(seed);
  const N = params.nodes;
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

  // full mesh with loss; host-style reconnect on stall-close. Fault windows
  // may overlap, so partitions are reference-counted per node and applied to
  // every live link (including redialed generations).
  const live: { i: number; j: number; link: VirtualLink }[] = [];
  const cutDepth: number[] = new Array<number>(N).fill(0);
  const applyCuts = () => {
    for (const { i, j, link } of live) link.cut(cutDepth[i]! > 0 || cutDepth[j]! > 0);
  };
  const grants = Object.fromEntries(TOPICS.map((t) => [t, "full" as const]));
  const dial = (i: number, j: number, gen: number) => {
    const link = new VirtualLink(sched, root.substream(`link${i}-${j}-${gen}`), {
      lossP: params.lossP,
    });
    live.push({ i, j, link });
    const h = nodes[i]!.attach(link.a, { peerId: `w${j}`, peerClass: "content", grants });
    nodes[j]!.attach(link.b, { peerId: `w${i}`, peerClass: "content", grants });
    h.onStateChange((s) => {
      if (s === "closed") sched.schedule(sched.now() + 250, () => dial(i, j, gen + 1));
    });
    applyCuts();
  };
  for (let i = 0; i < N; i++) for (let j = i + 1; j < N; j++) dial(i, j, 0);

  for (const w of params.faultWindows) {
    sched.schedule(w.fromMs, () => {
      cutDepth[w.node] = (cutDepth[w.node] ?? 0) + 1;
      applyCuts();
    });
    sched.schedule(w.toMs, () => {
      cutDepth[w.node] = (cutDepth[w.node] ?? 0) - 1;
      applyCuts();
    });
  }

  // workload: each segment draws from its own id-keyed substream
  const ledger: EntryId[] = [];
  const acks: Promise<unknown>[] = [];
  let totalAppends = 0;
  for (const seg of params.segments) {
    const wl = root.substream(`workload:${seg.id}`);
    for (let k = 0; k < seg.count; k++) {
      totalAppends++;
      const at = seg.fromMs + Math.floor(wl.next() * (seg.toMs - seg.fromMs));
      const who = wl.int(N);
      const topic = TOPICS[wl.int(TOPICS.length)]!;
      const kind = `k${wl.int(3)}`;
      sched.schedule(at, () => {
        acks.push(nodes[who]!.log(topic).append(kind, { seg: seg.id, k }).then((id) => ledger.push(id)));
      });
    }
  }

  const lastMs = Math.max(
    0,
    ...params.faultWindows.map((w) => w.toMs),
    ...params.segments.map((s) => s.toMs),
  );
  await sched.run({ untilMs: lastMs + SETTLE_MS });
  await Promise.allSettled(acks);

  const contigMaps: string[] = [];
  const viewRows: string[] = [];
  const logDumps: string[] = [];
  const writers = Array.from({ length: N }, (_, i) => `w${i}`);
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
      for (const w of writers) {
        const head = core.getStream(t, w);
        for (const e of core.entries(t, w, 1, head.contigSeq)) dump.push([t, e.writer, e.seq]);
      }
    }
    logDumps.push(jcs(dump as unknown as JsonValue));
  }
  return { contigMaps, viewRows, ledger, logDumps, totalAppends };
}

/** P1/P2/P3 from one run; P5 (seed determinism) needs a second run and is
 * only checked when target is unset (initial reproduce) or is P5 itself. */
export const checkMeshScenario: RunCheck = async (seed, params, target) => {
  const failures: PropertyId[] = [];
  const r = await runScenario(seed, params);

  if (!r.contigMaps.every((m) => m === r.contigMaps[0])) failures.push("P1");
  if (!r.viewRows.every((v) => v === r.viewRows[0])) failures.push("P2");

  let p3 = r.ledger.length !== r.totalAppends;
  if (!p3) {
    const converged = JSON.parse(r.contigMaps[0]!) as Record<
      string,
      Record<string, { contig: number } | undefined> | undefined
    >;
    for (const [topic, writer, seq] of r.ledger) {
      const contig = converged[topic]?.[writer]?.contig ?? 0;
      if (contig < seq) p3 = true;
    }
  }
  if (p3) failures.push("P3");

  if (target === undefined || target === "P5") {
    const b = await runScenario(seed, params);
    const same =
      jcs(r.logDumps as unknown as JsonValue) === jcs(b.logDumps as unknown as JsonValue) &&
      jcs(r.contigMaps as unknown as JsonValue) === jcs(b.contigMaps as unknown as JsonValue) &&
      jcs(r.viewRows as unknown as JsonValue) === jcs(b.viewRows as unknown as JsonValue);
    if (!same) failures.push("P5");
  }
  return failures;
};

// ---------- scenario table ----------

/** The real mesh scenario plus a synthetic planted-failure scenario so the
 * minimizer itself is testable without a genuinely failing seed. The synthetic
 * check is a pure function of (seed, params): it "fails P1" iff the seed is
 * odd AND a node-3 fault window covering t=10s survives AND a workload segment
 * overlapping [2500, 3500) survives — the minimal set is 1 window + 1 segment. */
export function scenarioTable(): Record<string, ScenarioDef> {
  return {
    "mesh4-loss-partition": {
      name: "mesh4-loss-partition",
      baseline: {
        nodes: 4,
        lossP: 0.05,
        faultWindows: [{ fromMs: 8_000, toMs: 14_000, node: 3 }],
        segments: [
          { id: "s0", fromMs: 500, toMs: 5_500, count: 15 },
          { id: "s1", fromMs: 5_500, toMs: 10_500, count: 15 },
          { id: "s2", fromMs: 10_500, toMs: 15_500, count: 15 },
          { id: "s3", fromMs: 15_500, toMs: 20_500, count: 15 },
        ],
      },
      minNodes: 2,
      check: checkMeshScenario,
    },
    "synthetic-planted": {
      name: "synthetic-planted",
      baseline: {
        nodes: 4,
        lossP: 0,
        faultWindows: [
          { fromMs: 2_000, toMs: 5_000, node: 1 },
          { fromMs: 8_000, toMs: 14_000, node: 3 },
          { fromMs: 16_000, toMs: 18_000, node: 2 },
        ],
        segments: [
          { id: "s0", fromMs: 500, toMs: 3_000, count: 8 },
          { id: "s1", fromMs: 3_000, toMs: 6_000, count: 8 },
          { id: "s2", fromMs: 6_000, toMs: 12_000, count: 8 },
          { id: "s3", fromMs: 12_000, toMs: 20_000, count: 8 },
        ],
      },
      minNodes: 2,
      check: (seed, params) => {
        const windowHit = params.faultWindows.some(
          (w) => w.node === 3 && w.fromMs <= 10_000 && w.toMs > 10_000,
        );
        const segmentHit = params.segments.some(
          (s) => s.count > 0 && s.fromMs < 3_500 && s.toMs > 2_500,
        );
        const fails = seed % 2 === 1 && windowHit && segmentHit;
        return Promise.resolve(fails ? (["P1"] as PropertyId[]) : []);
      },
    },
  };
}

// ---------- greedy schedule-slicing ----------

const MIN_WINDOW_MS = 500; // stop halving fault windows below this width
const MIN_SEGMENT_MS = 500; // stop halving segment intervals below this width

export interface MinimizeOutcome {
  passed: boolean; // true → the seed does not fail; nothing to minimize
  property?: PropertyId;
  params?: ScenarioParams;
  evaluations: number;
  transcript: string[];
}

const fmtWindow = (w: FaultWindow) => `node ${w.node} @ [${w.fromMs}, ${w.toMs})`;
const fmtSegment = (s: WorkloadSegment) => `${s.id} @ [${s.fromMs}, ${s.toMs}) x${s.count}`;

/** Greedy slicing per docs/harness.md §10: try each reduction in a fixed
 * structural order; keep it only if the target property still fails; repeat
 * passes to fixpoint. */
export async function minimize(opts: {
  seed: number;
  scenario: ScenarioDef;
  log?: (line: string) => void;
}): Promise<MinimizeOutcome> {
  const { seed, scenario } = opts;
  const transcript: string[] = [];
  const log = (line: string) => {
    transcript.push(line);
    opts.log?.(line);
  };
  let evaluations = 0;

  const initial = await scenario.check(seed, scenario.baseline);
  evaluations++;
  if (initial.length === 0) {
    log(`reproduce: seed=${seed} scenario=${scenario.name} → all properties PASS`);
    return { passed: true, evaluations, transcript };
  }
  const target = initial[0]!;
  log(
    `reproduce: seed=${seed} scenario=${scenario.name} → failing [${initial.join(", ")}], target ${target}`,
  );

  let params: ScenarioParams = structuredClone(scenario.baseline);
  const stillFails = async (candidate: ScenarioParams): Promise<boolean> => {
    evaluations++;
    return (await scenario.check(seed, candidate, target)).includes(target);
  };
  const attempt = async (desc: string, candidate: ScenarioParams): Promise<boolean> => {
    const ok = await stillFails(candidate);
    log(`  ${desc}: ${ok ? "still fails — kept" : "passes — reverted"}`);
    if (ok) params = candidate;
    return ok;
  };

  for (let pass = 1; ; pass++) {
    log(`pass ${pass}:`);
    let changed = false;

    // 1. drop whole fault windows
    for (let i = 0; i < params.faultWindows.length; ) {
      const cand = structuredClone(params);
      const [w] = cand.faultWindows.splice(i, 1);
      if (await attempt(`drop fault window ${fmtWindow(w!)}`, cand)) changed = true;
      else i++;
    }

    // 2. drop whole workload segments
    for (let i = 0; i < params.segments.length; ) {
      const cand = structuredClone(params);
      const [s] = cand.segments.splice(i, 1);
      if (await attempt(`drop segment ${fmtSegment(s!)}`, cand)) changed = true;
      else i++;
    }

    // 3. reduce node count (only while nothing references the top node)
    while (
      params.nodes > scenario.minNodes &&
      params.faultWindows.every((w) => w.node < params.nodes - 1)
    ) {
      const cand = structuredClone(params);
      cand.nodes--;
      if (await attempt(`reduce nodes ${params.nodes} → ${cand.nodes}`, cand)) changed = true;
      else break;
    }

    // 4. shrink surviving fault windows by halves
    for (let i = 0; i < params.faultWindows.length; i++) {
      for (;;) {
        const w = params.faultWindows[i]!;
        const width = w.toMs - w.fromMs;
        if (width <= MIN_WINDOW_MS) break;
        const mid = w.fromMs + Math.floor(width / 2);
        const first = structuredClone(params);
        first.faultWindows[i] = { ...w, toMs: mid };
        if (await attempt(`shrink window ${fmtWindow(w)} → [${w.fromMs}, ${mid})`, first)) {
          changed = true;
          continue;
        }
        const second = structuredClone(params);
        second.faultWindows[i] = { ...w, fromMs: mid };
        if (await attempt(`shrink window ${fmtWindow(w)} → [${mid}, ${w.toMs})`, second)) {
          changed = true;
          continue;
        }
        break;
      }
    }

    // 5. shrink surviving segments: halve counts, then halve intervals
    for (let i = 0; i < params.segments.length; i++) {
      for (;;) {
        const s = params.segments[i]!;
        if (s.count <= 1) break;
        const cand = structuredClone(params);
        cand.segments[i] = { ...s, count: Math.floor(s.count / 2) };
        if (await attempt(`halve segment ${fmtSegment(s)} count → ${Math.floor(s.count / 2)}`, cand)) {
          changed = true;
          continue;
        }
        break;
      }
      for (;;) {
        const s = params.segments[i]!;
        const width = s.toMs - s.fromMs;
        if (width <= MIN_SEGMENT_MS) break;
        const mid = s.fromMs + Math.floor(width / 2);
        const first = structuredClone(params);
        first.segments[i] = { ...s, toMs: mid };
        if (await attempt(`shrink segment ${fmtSegment(s)} → [${s.fromMs}, ${mid})`, first)) {
          changed = true;
          continue;
        }
        const second = structuredClone(params);
        second.segments[i] = { ...s, fromMs: mid };
        if (await attempt(`shrink segment ${fmtSegment(s)} → [${mid}, ${s.toMs})`, second)) {
          changed = true;
          continue;
        }
        break;
      }
    }

    if (!changed) break;
  }

  const b = scenario.baseline;
  log(
    `minimized: ${params.faultWindows.length}/${b.faultWindows.length} fault windows, ` +
      `${params.segments.length}/${b.segments.length} segments, ` +
      `nodes ${b.nodes} → ${params.nodes}, ${evaluations} evaluations`,
  );
  return { passed: false, property: target, params, evaluations, transcript };
}

/** Corpus-entry descriptor (harness/seeds.json shape, plus the minimized
 * parameters and the property the seed breaks). */
export function corpusEntry(
  seed: number,
  scenarioName: string,
  outcome: MinimizeOutcome,
): Record<string, unknown> {
  if (outcome.passed || !outcome.params || !outcome.property)
    throw new Error("corpusEntry: seed did not fail — nothing to record");
  const p = outcome.params;
  return {
    seed,
    scenario: scenarioName,
    property: outcome.property,
    params: p,
    note:
      `minimized by tools/minimize-seed.mjs: ${p.faultWindows.length} fault window(s), ` +
      `${p.segments.length} segment(s), ${p.nodes} nodes; breaks ${outcome.property}`,
  };
}
