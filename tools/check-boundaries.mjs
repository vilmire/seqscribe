#!/usr/bin/env node
// Package-split readiness gate (proposals-v3.8 P37).
//
// DESIGN §8 splits this repo into `seqscribe` (core) / `seqscribe-ws` (reference
// transport) / `seqscribe-beacon` (reference beacon SERVER) / `@seqscribe/*`
// storage adapters. The split itself waits for release — moving folders now would
// break the embedder's `file:` vendor path for no benefit while the package is
// unpublished. What does NOT wait is the property that makes the split possible
// later: the would-be-separate modules must not reach into core internals.
//
// They satisfy it today by accident of good layering, not by anything enforcing
// it, and an accidental property is one refactor away from being false. Checking
// it costs milliseconds; discovering it broke at release time costs a redesign.
//
// Rule: each module listed below may import ONLY from the allowed set. Type-only
// imports of the public type surface are always fine — that is exactly what a
// separate package would depend on.
//
// Usage: node tools/check-boundaries.mjs

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

// module → the only local modules it may import from
const BOUNDARIES = {
  // seqscribe-ws: a Channel adapter. Knows the Channel shape and nothing else.
  "src/ws.ts": ["./types.js"],
  // @seqscribe/* storage adapters: know SqliteHandle and nothing else.
  "src/adapters.ts": ["./types.js"],
  // seqscribe-beacon's server half (beaconFetchHandler) shares this file with
  // the beacon CLIENT, which is core by design (DESIGN §5.7 — node.ts owns a
  // BeaconHub). So this file is allowed core access; it is listed to pin the
  // set, so a NEW dependency here is a deliberate decision and not a drift.
  "src/beacon.ts": [
    "./encoding.js",
    "./errors.js",
    "./log.js",
    "./register.js",
    "./topics.js",
    "./types.js",
  ],
};

// `import ... from "X"` / `export ... from "X"`, local specifiers only
const SPEC_RE = /(?:^|\n)\s*(?:import|export)[\s\S]*?from\s*["'](\.[^"']+)["']/g;

let failures = 0;
for (const [file, allowed] of Object.entries(BOUNDARIES)) {
  const src = readFileSync(join(ROOT, file), "utf8");
  const seen = new Set();
  for (const m of src.matchAll(SPEC_RE)) seen.add(m[1]);
  for (const spec of [...seen].sort()) {
    if (allowed.includes(spec)) continue;
    console.error(
      `✗ ${file} imports ${spec}\n` +
        `  Allowed: ${allowed.join(", ")}\n` +
        `  This module is slated to become its own package (DESIGN §8). Importing\n` +
        `  core internals makes that split impossible. Depend on the public type\n` +
        `  surface instead, or update BOUNDARIES in this file with a reason.`,
    );
    failures++;
  }
}

if (failures > 0) {
  console.error(`\n${failures} boundary violation(s) — see DESIGN §8.`);
  process.exit(1);
}
console.log(
  `✓ package boundaries intact (${Object.keys(BOUNDARIES).length} modules split-ready)`,
);
