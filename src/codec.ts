// SPEC §1, §2 — structural validation shared by every ingress path (author
// append, wire ENTRIES, import). Wire message codecs join at milestone ②.

import { assertJsonValue, jcs, utf8ByteLength } from "./encoding.js";
import { SeqscribeError } from "./errors.js";
import { HLC_C_LIMIT } from "./hlc.js";
import type { Constants, LogEntry } from "./types.js";

export const TOPIC_RE = /^[a-z0-9_.-]{1,128}$/;
export const WRITER_RE = /^[A-Za-z0-9_.:-]{1,128}$/;
export const MAX_KEY_BYTES = 512;
export const CHAIN_RE = /^[0-9a-f]{64}$/;

export function assertTopic(topic: unknown): asserts topic is string {
  if (typeof topic !== "string" || !TOPIC_RE.test(topic))
    throw new SeqscribeError("ERR_ENTRY_ENCODING", `invalid topic: ${String(topic)}`);
}

export function assertWriter(writer: unknown): asserts writer is string {
  if (typeof writer !== "string" || !WRITER_RE.test(writer))
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
