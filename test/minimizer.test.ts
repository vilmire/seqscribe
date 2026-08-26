// Failing-seed minimizer (docs/harness.md §10 step 4, tools/minimize-seed.mjs).
// The synthetic-planted scenario has a pure check with a known minimal failure
// set (one node-3 fault window covering t=10s + one workload segment
// overlapping [2500, 3500), odd seeds only), so the minimizer's convergence is
// testable without a genuinely failing seed. The CLI itself is exercised via
// spawnSync for both the refusal path and the descriptor output.

import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  checkMeshScenario,
  corpusEntry,
  minimize,
  scenarioTable,
  type ScenarioParams,
} from "../harness/minimize.js";

const ROOT = join(import.meta.dirname, "..");
const CLI = join(ROOT, "tools", "minimize-seed.mjs");

describe("failing-seed minimizer", () => {
  it("converges the synthetic planted failure to the minimal surviving set", async () => {
    const scenario = scenarioTable()["synthetic-planted"]!;
    const out = await minimize({ seed: 7, scenario });

    expect(out.passed).toBe(false);
    expect(out.property).toBe("P1");
    const p = out.params!;

    // exactly the planted window survives, shrunk tightly around t=10s
    expect(p.faultWindows).toHaveLength(1);
    const w = p.faultWindows[0]!;
    expect(w.node).toBe(3);
    expect(w.fromMs).toBeLessThanOrEqual(10_000);
    expect(w.toMs).toBeGreaterThan(10_000);
    expect(w.toMs - w.fromMs).toBeLessThanOrEqual(1_000); // ≤ 2x the halving floor

    // exactly one segment survives, overlapping the planted range, count 1
    expect(p.segments).toHaveLength(1);
    const s = p.segments[0]!;
    expect(s.fromMs).toBeLessThan(3_500);
    expect(s.toMs).toBeGreaterThan(2_500);
    expect(s.count).toBe(1);

    // the minimized set still fails, and is 1-minimal: dropping either
    // surviving element makes the failure vanish
    expect(await scenario.check(7, p)).toContain("P1");
    const noWindow: ScenarioParams = { ...p, faultWindows: [] };
    const noSegment: ScenarioParams = { ...p, segments: [] };
    expect(await scenario.check(7, noWindow)).toEqual([]);
    expect(await scenario.check(7, noSegment)).toEqual([]);

    // corpus descriptor carries seed + scenario + property + params + note
    const entry = corpusEntry(7, scenario.name, out);
    expect(entry).toMatchObject({ seed: 7, scenario: "synthetic-planted", property: "P1" });
    expect(entry.params).toEqual(p);
    expect(typeof entry.note).toBe("string");
  });

  it("is deterministic: same seed → identical transcript and result", async () => {
    const table = scenarioTable();
    const a = await minimize({ seed: 7, scenario: table["synthetic-planted"]! });
    const b = await minimize({ seed: 7, scenario: table["synthetic-planted"]! });
    expect(b.transcript).toEqual(a.transcript);
    expect(b.params).toEqual(a.params);
    expect(b.evaluations).toBe(a.evaluations);
  });

  it("refuses to minimize a passing seed (module level)", async () => {
    const scenario = scenarioTable()["synthetic-planted"]!;
    const out = await minimize({ seed: 8, scenario }); // even seed → planted check passes
    expect(out.passed).toBe(true);
    expect(out.params).toBeUndefined();
    expect(() => corpusEntry(8, scenario.name, out)).toThrow(/did not fail/);
  });

  it("real mesh scenario: a corpus seed passes P1/P2/P3/P5 under the runner", async () => {
    const scenario = scenarioTable()["mesh4-loss-partition"]!;
    expect(await checkMeshScenario(20260826, scenario.baseline)).toEqual([]);
  }, 60_000);

  it("CLI: passing seed → refusal message and exit 2, no stdout descriptor", () => {
    const r = spawnSync(process.execPath, [CLI, "8", "--scenario", "synthetic-planted"], {
      cwd: ROOT,
      encoding: "utf8",
      timeout: 60_000,
    });
    expect(r.status).toBe(2);
    expect(r.stderr).toMatch(/PASSES.*nothing to minimize/);
    expect(r.stdout.trim()).toBe("");
  }, 60_000);

  it("CLI: failing seed → minimized seeds.json-shaped descriptor on stdout, exit 0", () => {
    const r = spawnSync(process.execPath, [CLI, "7", "--scenario", "synthetic-planted"], {
      cwd: ROOT,
      encoding: "utf8",
      timeout: 60_000,
    });
    expect(r.status).toBe(0);
    const entry = JSON.parse(r.stdout) as {
      seed: number;
      scenario: string;
      property: string;
      params: ScenarioParams;
      note: string;
    };
    expect(entry.seed).toBe(7);
    expect(entry.scenario).toBe("synthetic-planted");
    expect(entry.property).toBe("P1");
    expect(entry.params.faultWindows).toHaveLength(1);
    expect(entry.params.segments).toHaveLength(1);
    expect(r.stderr).toMatch(/minimized: 1\/3 fault windows, 1\/4 segments/);
  }, 60_000);
});
