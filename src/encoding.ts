// SPEC §4 — canonical encoding, chainHash, and the derived hash identities
// (certHash §7.2, snapshotId §5.4, topicSchemaHash §14). Vector-gated: every
// function here must reproduce vectors/vectors.json byte-for-byte.

// canonicalize is CJS with a mismatched d.ts (declares `export default`, ships
// `module.exports = fn`) — cast to the real runtime shape.
import canonicalizeCjs from "canonicalize";
const canonicalizeLib = canonicalizeCjs as unknown as (v: unknown) => string | undefined;
import { sha256 } from "@noble/hashes/sha256";
import { bytesToHex } from "@noble/hashes/utils";
import { SeqscribeError } from "./errors.js";
import type {
  EntryId,
  FinalityCert,
  Hlc,
  JsonValue,
  Key,
  Seq,
  SnapshotBody,
  Topic,
  TopicPolicy,
  WriterId,
} from "./types.js";

// TextEncoder via globalThis — the core compiles without platform lib types.
const utf8 = new (globalThis as unknown as {
  TextEncoder: new () => { encode(s: string): Uint8Array };
}).TextEncoder();

// Rejects everything JCS cannot represent (undefined, functions, non-finite
// numbers, class instances) BEFORE serialization — the JCS lib silently drops
// or coerces some of these, which would corrupt chains instead of erroring.
export function assertJsonValue(v: unknown, path = "$"): asserts v is JsonValue {
  if (v === null) return;
  const t = typeof v;
  if (t === "string" || t === "boolean") return;
  if (t === "number") {
    if (!Number.isFinite(v as number))
      throw new SeqscribeError("ERR_ENTRY_ENCODING", `non-finite number at ${path}`);
    return;
  }
  if (Array.isArray(v)) {
    for (let i = 0; i < v.length; i++) assertJsonValue(v[i], `${path}[${i}]`);
    return;
  }
  if (t === "object") {
    const proto = Object.getPrototypeOf(v);
    if (proto !== Object.prototype && proto !== null)
      throw new SeqscribeError("ERR_ENTRY_ENCODING", `non-plain object at ${path}`);
    for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
      if (val === undefined)
        throw new SeqscribeError("ERR_ENTRY_ENCODING", `undefined property at ${path}.${k}`);
      assertJsonValue(val, `${path}.${k}`);
    }
    return;
  }
  throw new SeqscribeError("ERR_ENTRY_ENCODING", `unsupported type ${t} at ${path}`);
}

// Opt-in normalizer (proposals-v3.5 P12) for the routine JavaScript payload
// shape { k: undefined }: returns a fresh tree with undefined object
// properties DROPPED (what JSON.stringify would do), guaranteed to pass
// assertJsonValue and JCS unchanged. Array positions are load-bearing (§4
// canonical encoding), so an undefined ARRAY element throws instead of
// shifting or silently becoming null. The strict default stands — append()
// still rejects unsanitized input; call this first when payloads are loose.
export function sanitizeJson(v: unknown, path = "$"): JsonValue {
  if (v === null) return null;
  const t = typeof v;
  if (t === "string" || t === "boolean") return v as JsonValue;
  if (t === "number") {
    if (!Number.isFinite(v as number))
      throw new SeqscribeError("ERR_ENTRY_ENCODING", `non-finite number at ${path}`);
    return v as JsonValue;
  }
  if (Array.isArray(v)) {
    return v.map((el, i) => {
      if (el === undefined)
        throw new SeqscribeError(
          "ERR_ENTRY_ENCODING",
          `undefined array element at ${path}[${i}] — removing it would shift positions`,
        );
      return sanitizeJson(el, `${path}[${i}]`);
    });
  }
  if (t === "object") {
    const proto = Object.getPrototypeOf(v);
    if (proto !== Object.prototype && proto !== null)
      throw new SeqscribeError("ERR_ENTRY_ENCODING", `non-plain object at ${path}`);
    const pairs: [string, JsonValue][] = [];
    for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
      if (val === undefined) continue; // the point of this helper
      pairs.push([k, sanitizeJson(val, `${path}.${k}`)]);
    }
    // fromEntries defines own data properties — an own "__proto__" key stays a
    // key instead of becoming a prototype set under index-assignment
    return Object.fromEntries(pairs);
  }
  throw new SeqscribeError("ERR_ENTRY_ENCODING", `unsupported type ${t} at ${path}`);
}

export function jcs(v: JsonValue): string {
  const s = canonicalizeLib(v);
  if (typeof s !== "string")
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "value not JCS-representable");
  return s;
}

export function utf8ByteLength(s: string): number {
  return utf8.encode(s).length;
}

// F(x) = u32be(len(bytes)) || bytes  (SPEC §4.3)
export function frame(s: string): Uint8Array {
  const b = utf8.encode(s);
  const out = new Uint8Array(4 + b.length);
  new DataView(out.buffer).setUint32(0, b.length, false);
  out.set(b, 4);
  return out;
}

export function dec(n: number): string {
  if (!Number.isSafeInteger(n) || n < 0)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", `dec() domain violation: ${n}`);
  return String(n);
}

function sha256HexOf(parts: Uint8Array[]): string {
  const h = sha256.create();
  for (const p of parts) h.update(p);
  return bytesToHex(h.digest());
}

export function sha256HexUtf8(s: string): string {
  return bytesToHex(sha256(utf8.encode(s)));
}

export function seedOf(topic: Topic, writer: WriterId): string {
  return sha256HexOf([frame("seqscribe:v1"), frame(topic), frame(writer)]);
}

export interface ChainInput {
  seq: Seq;
  hlc: Hlc;
  kind: string;
  key?: Key | undefined;
  causal?: [WriterId, Seq] | undefined;
  ref?: EntryId | undefined;
  payload: JsonValue;
}

export function chainOf(prevChain: string, e: ChainInput): string {
  return sha256HexOf([
    frame(prevChain),
    frame(dec(e.seq)),
    frame(dec(e.hlc.l)),
    frame(dec(e.hlc.c)),
    frame(e.kind),
    frame(e.key ?? ""),
    frame(e.key === undefined ? "0" : "1"),
    frame(e.causal ? jcs(e.causal) : ""),
    frame(e.causal ? "1" : "0"),
    frame(e.ref ? jcs(e.ref) : ""),
    frame(e.ref ? "1" : "0"),
    frame(jcs(e.payload)),
  ]);
}

export function certHashOf(cert: FinalityCert): string {
  return sha256HexUtf8(jcs(cert as unknown as JsonValue));
}

export function snapshotIdOf(body: SnapshotBody): string {
  return sha256HexUtf8(jcs(body as unknown as JsonValue));
}

// SPEC §14 (v3.3): the exact normalized object N.
export function normalizedPolicy(p: TopicPolicy): JsonValue {
  return {
    kind: p.kind,
    conflict:
      p.kind === "register"
        ? {
            default: p.conflict?.default ?? "lww",
            overrides: { ...(p.conflict?.overrides ?? {}) },
          }
        : null,
    registerSemanticsVersion: 1,
    finalityAuthority: p.finalityAuthority ?? null,
  };
}

export function topicSchemaHashOf(p: TopicPolicy): string {
  return sha256HexUtf8(jcs(normalizedPolicy(p)));
}
