#!/usr/bin/env node
// SPEC §14 types.d.ts generation rule: concatenate every normative `ts` block
// in SPEC.md in document order (excluding the §5.4 wire-message sketches), with
// `export` prefixed to every top-level declaration. CI gates on the result
// typechecking as one external module.
// Usage: node tools/gen-types.mjs   → writes types.d.ts at the repo root.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const spec = readFileSync(join(ROOT, "SPEC.md"), "utf8");

const blocks = [...spec.matchAll(/```ts\n([\s\S]*?)```/g)].map((m) => m[1]);
const normative = blocks.filter((b) => !b.includes('t:"HELLO"')); // §5.4 excluded

const exported = normative
  .map((b) =>
    b.replace(/^(type |interface |declare class |function )/gm, "export $1"),
  )
  .join("\n");

const header = `// GENERATED from SPEC.md by tools/gen-types.mjs — do not edit.\n// SPEC §14: all normative ts blocks in document order, exports prefixed.\n\n`;
writeFileSync(join(ROOT, "types.d.ts"), header + exported);
console.log(
  `types.d.ts: ${normative.length} normative blocks (of ${blocks.length} ts blocks), ${exported.split("\n").length} lines`,
);
