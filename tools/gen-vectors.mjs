#!/usr/bin/env node
// Test-vector generator for the seqscribe SPEC (hashing rules stable since v3.2).
// Regenerate with:  node tools/gen-vectors.mjs
// Writes vectors/vectors.json and prints a summary.
//
// The JCS implementation below is a minimal RFC 8785 serializer sufficient for
// the vector inputs in this file (finite numbers, no lone surrogates). The
// production library uses a vetted JCS dependency (SPEC §4.2); these vectors
// exist to verify that integration, and any conforming implementation, against
// known answers.

import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

// ---------- RFC 8785 JCS (subset sufficient for these vectors) ----------

function jcs(v) {
  if (v === null || typeof v === "boolean") return JSON.stringify(v);
  if (typeof v === "number") {
    if (!Number.isFinite(v)) throw new Error("non-finite number");
    return JSON.stringify(v); // ES number-to-string == JCS §3.2.2.3 (and -0 → "0")
  }
  if (typeof v === "string") return JSON.stringify(v); // JSON.stringify escaping == JCS §3.2.2.2
  if (Array.isArray(v)) return "[" + v.map(jcs).join(",") + "]";
  if (typeof v === "object") {
    const keys = Object.keys(v).sort(); // default sort == UTF-16 code-unit order (JCS §3.2.3)
    return "{" + keys.map((k) => JSON.stringify(k) + ":" + jcs(v[k])).join(",") + "}";
  }
  throw new Error("unsupported type: " + typeof v);
}

// ---------- SPEC §4.3 primitives ----------

const sha256hex = (buf) => createHash("sha256").update(buf).digest("hex");

function F(str) {
  const bytes = Buffer.from(str, "utf8");
  const len = Buffer.alloc(4);
  len.writeUInt32BE(bytes.length, 0);
  return Buffer.concat([len, bytes]);
}

const dec = (n) => {
  if (!Number.isSafeInteger(n) || n < 0) throw new Error("dec() domain: non-negative safe integer");
  return String(n);
};

const seed = (topic, writer) =>
  sha256hex(Buffer.concat([F("seqscribe:v1"), F(topic), F(writer)]));

function chain(prevChain, e) {
  return sha256hex(
    Buffer.concat([
      F(prevChain),
      F(dec(e.seq)),
      F(dec(e.hlc.l)),
      F(dec(e.hlc.c)),
      F(e.kind),
      F(e.key ?? ""),
      F(e.key === undefined ? "0" : "1"),
      F(e.causal ? jcs(e.causal) : ""),
      F(e.causal ? "1" : "0"),
      F(e.ref ? jcs(e.ref) : ""),
      F(e.ref ? "1" : "0"),
      F(jcs(e.payload)),
    ])
  );
}

// ---------- 1. JCS vectors ----------

const jcsCases = [
  { name: "empty object", input: {} },
  { name: "empty array", input: [] },
  { name: "key sorting (UTF-16 code units)", input: { b: 2, a: 1, A: 0 } },
  { name: "unicode key sorts after ASCII", input: { "é": 1, z: 2 } },
  { name: "nested + null", input: { x: [1, null, { y: false }] } },
  { name: "integer", input: 42 },
  { name: "negative zero", input: -0 },
  { name: "fraction", input: 0.1 },
  { name: "large exponent", input: 1e21 },
  { name: "small exponent", input: 1e-7 },
  { name: "max safe integer", input: 9007199254740991 },
  { name: "string escapes", input: "a\"b\\c\nd\te\u0001f" },
  { name: "unicode string (NFC as authored)", input: "héllo ✓" },
];
const jcsVectors = jcsCases.map((c) => ({ ...c, jcs: jcs(c.input) }));

// ---------- 2. F() framing ----------

const framingVectors = [
  { input: "seqscribe:v1", hex: F("seqscribe:v1").toString("hex") },
  { input: "", hex: F("").toString("hex") },
  { input: "0", hex: F("0").toString("hex") },
  { input: "héllo", hex: F("héllo").toString("hex") }, // len counts UTF-8 BYTES (6), not chars
];

// ---------- 3. seed vectors ----------

const seedVectors = [
  { topic: "vec.notes", writer: "vec-w1", seed: seed("vec.notes", "vec-w1") },
  { topic: "vec.notes", writer: "vec-w2", seed: seed("vec.notes", "vec-w2") },
  { topic: "mesh.ledger", writer: "adhdev:m1", seed: seed("mesh.ledger", "adhdev:m1") },
  // length-prefix framing makes boundary ambiguity impossible; these two MUST differ:
  { topic: "a", writer: "bc", seed: seed("a", "bc") },
  { topic: "ab", writer: "c", seed: seed("ab", "c") },
];

// ---------- 4. chain vectors (one stream, covers every field combination) ----------

const T = "vec.notes";
const W = "vec-w1";
const stream = [
  // plain append entry
  { topic: T, writer: W, seq: 1, hlc: { l: 1756166400000, c: 0 }, kind: "note",
    payload: { text: "hello", n: 1 } },
  // hlc.c increment; payload exercising JCS number + unicode edges
  { topic: T, writer: W, seq: 2, hlc: { l: 1756166400000, c: 1 }, kind: "note",
    payload: { "z": 2, "é": 1, big: 1e21, frac: 0.1, text: "héllo ✓" } },
  // register set: key + causal present
  { topic: T, writer: W, seq: 3, hlc: { l: 1756166400001, c: 0 }, kind: "set",
    key: "config.theme", causal: [W, 2], payload: { value: "dark" } },
  // register del: null payload
  { topic: T, writer: W, seq: 4, hlc: { l: 1756166400002, c: 0 }, kind: "del",
    key: "config.theme", causal: [W, 3], payload: null },
  // ref present (e.g. owned-request approval shape)
  { topic: T, writer: W, seq: 5, hlc: { l: 1756166400003, c: 0 }, kind: "note",
    ref: [T, W, 1], payload: { reply: true } },
];

let prev = seed(T, W);
const chainVectors = stream.map((e) => {
  const c = chain(prev, e);
  const out = { entry: e, prevChain: prev, chain: c };
  prev = c;
  return out;
});

// ---------- 5. certHash vector ----------

const cert = {
  topic: T,
  order: { l: 1756166400001, c: 0, writer: W, seq: 3 },
  cut: { [W]: { seq: 3, chain: chainVectors[2].chain } },
  generation: 1,
  authority: "vec:authority",
  sig: "304502...deadbeef", // placeholder string — certHash hashes it as an opaque string
};
const certVector = { cert, jcs: jcs(cert), certHash: sha256hex(Buffer.from(jcs(cert), "utf8")) };

// ---------- 6. snapshotId vector ----------

const snapshotBody = {
  topic: T,
  order: cert.order,
  generation: 1,
  certHash: certVector.certHash,
  cut: cert.cut,
  directives: [],
  views: [],
};
const snapshotVector = {
  body: snapshotBody,
  jcs: jcs(snapshotBody),
  snapshotId: sha256hex(Buffer.from(jcs(snapshotBody), "utf8")),
};

// ---------- 7. topicSchemaHash (PROPOSED normalization — pending SPEC ratification) ----------
// SPEC §14 fixes the field set {kind, conflict (incl. §11.7 matching semantics),
// registerSemanticsVersion: 1, finalityAuthority} but not the exact normalized object.
// Proposal (docs/proposals-v3.3.md P2): see below; vectors are computed against it.

function normalizePolicy(p) {
  return {
    kind: p.kind,
    conflict:
      p.kind === "register"
        ? { default: p.conflict?.default ?? "lww", overrides: p.conflict?.overrides ?? {} }
        : null,
    registerSemanticsVersion: 1,
    finalityAuthority: p.finalityAuthority ?? null,
  };
}
const schemaHash = (p) => sha256hex(Buffer.from(jcs(normalizePolicy(p)), "utf8"));

const policyCases = [
  { name: "append/full, no authority",
    policy: { kind: "append", retention: { mode: "full" }, replication: "full-sync", access: "content" } },
  { name: "register, defaults omitted",
    policy: { kind: "register", retention: { mode: "full" }, replication: "full-sync", access: "content",
      finalityAuthority: "vec:authority" } },
  { name: "register, explicit lww (MUST hash identically to previous)",
    policy: { kind: "register", retention: { mode: "full" }, replication: "full-sync", access: "content",
      conflict: { default: "lww", overrides: {} }, finalityAuthority: "vec:authority" } },
  { name: "register with overrides",
    policy: { kind: "register", retention: { mode: "full" }, replication: "full-sync", access: "content",
      conflict: { default: "lww", overrides: { "security.*": "fww", "providers.pinned": "resolver" } },
      finalityAuthority: "vec:authority" } },
];
const schemaHashVectors = policyCases.map((c) => ({
  ...c,
  normalized: normalizePolicy(c.policy),
  jcs: jcs(normalizePolicy(c.policy)),
  topicSchemaHash: schemaHash(c.policy),
}));

// ---------- write ----------

const out = {
  seqscribe: "vectors/v1",
  spec: "v3.2+",
  generator: "tools/gen-vectors.mjs",
  note: "Sections 1-6 follow normative SPEC text; section 7 (topicSchemaHash) follows the PROPOSED normalization in docs/proposals-v3.3.md and is not yet ratified.",
  jcs: jcsVectors,
  framing: framingVectors,
  seeds: seedVectors,
  chains: chainVectors,
  certHash: certVector,
  snapshotId: snapshotVector,
  topicSchemaHash: schemaHashVectors,
};

mkdirSync(join(ROOT, "vectors"), { recursive: true });
writeFileSync(join(ROOT, "vectors", "vectors.json"), JSON.stringify(out, null, 2) + "\n");

// ---------- print summary ----------

console.log("== JCS ==");
for (const v of jcsVectors) console.log(`  ${v.name}: ${v.jcs}`);
console.log("== F() framing (hex) ==");
for (const v of framingVectors) console.log(`  F(${JSON.stringify(v.input)}) = ${v.hex}`);
console.log("== seeds ==");
for (const v of seedVectors) console.log(`  seed(${v.topic}, ${v.writer}) = ${v.seed}`);
console.log("== chains ==");
for (const v of chainVectors) console.log(`  seq ${v.entry.seq} (${v.entry.kind}): ${v.chain}`);
console.log("== certHash ==\n  " + certVector.certHash);
console.log("  JCS: " + certVector.jcs);
console.log("== snapshotId ==\n  " + snapshotVector.snapshotId);
console.log("  JCS: " + snapshotVector.jcs);
console.log("== topicSchemaHash (proposed normalization) ==");
for (const v of schemaHashVectors) console.log(`  ${v.name}: ${v.topicSchemaHash}\n    JCS: ${v.jcs}`);
console.log("\nWrote vectors/vectors.json");
