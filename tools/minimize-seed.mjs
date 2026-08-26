#!/usr/bin/env node
// Failing-seed minimizer CLI (docs/harness.md §10 step 4).
// Usage:  node tools/minimize-seed.mjs <seed> [--scenario <name>]
//
// Reproduces the properties scenario under <seed>; if a property (P1/P2/P3/P5)
// fails, greedily slices the schedule (drop/shrink fault windows and workload
// segments, reduce node count) while the same property still fails, then emits
// a minimized corpus-entry descriptor as JSON on stdout — ready to be appended
// to harness/seeds.json by a human. It never writes seeds.json itself.
//
// If the seed PASSES, it says so and exits 2: a minimizer that "minimizes" a
// passing seed is lying.
//
// The scenario runner lives in harness/minimize.ts (TypeScript, shared with
// test/minimizer.test.ts). tools/ scripts are plain Node, so this CLI bundles
// that module on the fly with esbuild (already a repo dev tool; it resolves
// the codebase's `.js`-suffixed TS imports) into node_modules/.cache and
// imports the result. Exit codes: 0 minimized, 1 usage/error, 2 seed passes.

import { build } from "esbuild";
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function usage() {
  console.error("usage: node tools/minimize-seed.mjs <seed> [--scenario <name>]");
  process.exit(1);
}

const args = process.argv.slice(2);
let seedArg;
let scenarioName = "mesh4-loss-partition";
for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a === "--scenario") {
    scenarioName = args[++i];
    if (!scenarioName) usage();
  } else if (a === "--help" || a === "-h") {
    usage();
  } else if (seedArg === undefined) {
    seedArg = a;
  } else {
    usage();
  }
}
if (seedArg === undefined) usage();
const seed = Number(seedArg);
if (!Number.isSafeInteger(seed)) {
  console.error(`minimize-seed: seed must be an integer, got ${JSON.stringify(seedArg)}`);
  process.exit(1);
}

// bundle harness/minimize.ts (cache lives under node_modules so the bundle's
// external package imports resolve against the repo's node_modules)
const cacheDir = join(ROOT, "node_modules", ".cache", "seqscribe");
mkdirSync(cacheDir, { recursive: true });
const bundle = join(cacheDir, "minimize.bundle.mjs");
await build({
  entryPoints: [join(ROOT, "harness", "minimize.ts")],
  bundle: true,
  platform: "node",
  format: "esm",
  packages: "external",
  outfile: bundle,
  logLevel: "silent",
});
const { scenarioTable, minimize, corpusEntry } = await import(pathToFileURL(bundle).href);

const scenarios = scenarioTable();
const scenario = scenarios[scenarioName];
if (!scenario) {
  console.error(
    `minimize-seed: unknown scenario ${JSON.stringify(scenarioName)} — ` +
      `known: ${Object.keys(scenarios).join(", ")}`,
  );
  process.exit(1);
}

console.error(`minimize-seed: seed=${seed} scenario=${scenario.name}`);
const outcome = await minimize({ seed, scenario, log: (line) => console.error(line) });

if (outcome.passed) {
  console.error(
    `minimize-seed: seed ${seed} PASSES scenario ${scenario.name} — nothing to minimize ` +
      `(refusing to emit a descriptor for a passing seed)`,
  );
  process.exit(2);
}

console.log(JSON.stringify(corpusEntry(seed, scenario.name, outcome), null, 2));
