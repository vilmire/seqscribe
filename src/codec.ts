// SPEC §1, §2 — structural validation shared by every ingress path (author
// append, wire ENTRIES, import). Wire message codecs join at milestone ②.

import { assertJsonValue, jcs, utf8ByteLength } from "./encoding.js";
import { SeqscribeError } from "./errors.js";
import { HLC_C_LIMIT } from "./hlc.js";
import type {
  Constants,
  EntryId,
  Hlc,
  JsonValue,
  Key,
  LogEntry,
  Seq,
  Topic,
  WriterId,
} from "./types.js";

export const TOPIC_RE = /^[a-z0-9_.-]{1,128}$/;
export const WRITER_RE = /^[A-Za-z0-9_.:-]{1,128}$/;
export const MAX_KEY_BYTES = 512;
export const CHAIN_RE = /^[0-9a-f]{64}$/;

// §1 charter exclusion (proposals-v3.5 P8, ratified v3.6). The character
// classes above admit the literal name `__proto__`, and record maps keyed by
// charter names are plain objects at most merge sites — an own `__proto__` key
// becomes a prototype SET under index-assignment or Object.assign. The wire
// already rejects it (§5.4 charter-key rule), but that left the name legal at
// the AUTHOR and IMPORT layers, which is the half v3.5 recorded as deferred.
// Excluded here rather than in the regexes so the published charter character
// classes (and every hash that quotes them) stay exactly as specified — this
// is a name exclusion, not a charset change, so no hash input moves.
export const RESERVED_NAME = "__proto__";

export function assertTopic(topic: unknown): asserts topic is string {
  if (typeof topic !== "string" || !TOPIC_RE.test(topic) || topic === RESERVED_NAME)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", `invalid topic: ${String(topic)}`);
}

export function assertWriter(writer: unknown): asserts writer is string {
  if (typeof writer !== "string" || !WRITER_RE.test(writer) || writer === RESERVED_NAME)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", `invalid writer: ${String(writer)}`);
}

function assertNonNegSafeInt(n: unknown, name: string): asserts n is number {
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 0)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", `${name} must be a non-negative safe integer`);
}

export function assertKey(key: unknown): asserts key is string {
  if (typeof key !== "string" || utf8ByteLength(key) > MAX_KEY_BYTES)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "invalid key (UTF-8 ≤512B required)");
}

// Full structural validation of a LogEntry-shaped value. Does NOT verify the
// chain (that needs stream context — §6.1) and does not touch the clock.
export function validateEntry(e: unknown, constants: Constants): LogEntry {
  if (typeof e !== "object" || e === null)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "entry is not an object");
  const o = e as Record<string, unknown>;

  assertTopic(o.topic);
  assertWriter(o.writer);
  assertNonNegSafeInt(o.seq, "seq");
  if (o.seq < 1) throw new SeqscribeError("ERR_ENTRY_ENCODING", "seq starts at 1");

  const hlc = o.hlc as Record<string, unknown> | undefined;
  if (typeof hlc !== "object" || hlc === null)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "missing hlc");
  assertNonNegSafeInt(hlc.l, "hlc.l");
  assertNonNegSafeInt(hlc.c, "hlc.c");
  if ((hlc.c as number) >= HLC_C_LIMIT)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "hlc.c >= 2^32");

  if (typeof o.kind !== "string" || o.kind.length === 0)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "missing kind");

  if (o.key !== undefined) assertKey(o.key);

  if (o.causal !== undefined) {
    const c = o.causal;
    if (!Array.isArray(c) || c.length !== 2)
      throw new SeqscribeError("ERR_ENTRY_ENCODING", "causal must be [writer, seq]");
    assertWriter(c[0]);
    assertNonNegSafeInt(c[1], "causal seq");
  }

  if (o.ref !== undefined) {
    const r = o.ref;
    if (!Array.isArray(r) || r.length !== 3)
      throw new SeqscribeError("ERR_ENTRY_ENCODING", "ref must be [topic, writer, seq]");
    assertTopic(r[0]);
    assertWriter(r[1]);
    assertNonNegSafeInt(r[2], "ref seq");
  }

  if (!("payload" in o)) throw new SeqscribeError("ERR_ENTRY_ENCODING", "missing payload");
  assertJsonValue(o.payload, "$.payload");

  if (typeof o.chain !== "string" || !CHAIN_RE.test(o.chain))
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "invalid chain (lowercase hex64 required)");

  // Rebuild from the §2 fields rather than returning the input object: §4
  // chainOf covers only the enumerated fields, so unknown extras would ride
  // along unauthenticated — surviving pending-table JSON round-trips while
  // sq_log drops them, and two entries differing only in junk would share a
  // chain. Stripping here makes every ingress path canonical (and is what
  // both sides of a mixed-version fleet already agree on hashing).
  const entry: LogEntry = {
    topic: o.topic,
    writer: o.writer,
    seq: o.seq,
    hlc: { l: hlc.l as number, c: hlc.c as number },
    kind: o.kind,
    ...(o.key !== undefined ? { key: o.key as string } : {}),
    ...(o.causal !== undefined ? { causal: [(o.causal as unknown[])[0], (o.causal as unknown[])[1]] as [string, number] } : {}),
    ...(o.ref !== undefined
      ? { ref: [(o.ref as unknown[])[0], (o.ref as unknown[])[1], (o.ref as unknown[])[2]] as [string, string, number] }
      : {}),
    payload: o.payload,
    chain: o.chain,
  };
  assertEntrySize(entry, constants);
  return entry;
}

// The caller-known half of an append, for size estimation before the entry
// exists (proposals-v3.5 P13). Register-helper writes stamp `causal`
// themselves — include a representative causal here when estimating those.
export interface AppendShape {
  topic: Topic;
  kind: string;
  payload: JsonValue;
  key?: Key;
  causal?: [WriterId, Seq];
  ref?: EntryId;
}

// 16 decimal digits — an upper bound on the JCS width of any safe-integer
// seq/hlc field the library will ever mint
const MAX_NUMERIC_FIELD = Number.MAX_SAFE_INTEGER;

// Append-oriented entry-size estimation (proposals-v3.5 P13). assertEntrySize
// measures the JCS bytes of the COMPLETE LogEntry, but an append caller does
// not yet know the library-owned fields (writer, seq, hlc, chain). This
// substitutes them: exact values wherever ctx supplies them, conservative
// worst-case stand-ins otherwise (max-length writer, 16-digit seq/hlc; chain
// is always exactly 64 hex chars) — so with full context the result equals
// what assertEntrySize will measure, and without it the result is a
// monotone upper bound. Compare against Constants.MAX_ENTRY_BYTES;
// assertEntrySize at apply time remains authoritative.
export function estimateEntryBytes(
  shape: AppendShape,
  ctx?: { writer?: WriterId; seq?: Seq; hlc?: Hlc },
): number {
  const entry: LogEntry = {
    topic: shape.topic,
    writer: ctx?.writer ?? "W".repeat(128),
    seq: ctx?.seq ?? MAX_NUMERIC_FIELD,
    hlc: ctx?.hlc ?? { l: MAX_NUMERIC_FIELD, c: MAX_NUMERIC_FIELD },
    kind: shape.kind,
    ...(shape.key !== undefined ? { key: shape.key } : {}),
    ...(shape.causal !== undefined ? { causal: shape.causal } : {}),
    ...(shape.ref !== undefined ? { ref: shape.ref } : {}),
    payload: shape.payload,
    chain: "f".repeat(64),
  };
  return utf8ByteLength(jcs(entry as unknown as JsonValue));
}

// Entry ≤ MAX_ENTRY_BYTES, measured over the canonical (JCS) serialization —
// the only byte count two implementations agree on.
export function assertEntrySize(e: LogEntry, constants: Constants): void {
  const bytes = utf8ByteLength(jcs(e as unknown as Parameters<typeof jcs>[0]));
  if (bytes > constants.MAX_ENTRY_BYTES)
    throw new SeqscribeError(
      "ERR_ENTRY_TOO_LARGE",
      `entry is ${bytes}B > MAX_ENTRY_BYTES ${constants.MAX_ENTRY_BYTES}`,
    );
}
