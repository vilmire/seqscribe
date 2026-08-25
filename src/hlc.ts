// SPEC §1, §3 — HLC stamping, first-sight merge with deterministic carry,
// and the total order over entries.

import { SeqscribeError } from "./errors.js";
import type { Hlc, LogEntry, Order } from "./types.js";

export const HLC_C_LIMIT = 2 ** 32; // hlc.c MUST be < 2^32 (§1)

export interface HlcState {
  l: number;
  c: number;
}

// Stamp on append: l' = max(l, pt); c' = (l'==l ? c+1 : 0). A stamp that would
// reach 2^32 fails with ERR_ENTRY_ENCODING (pathological-clock guard, §1).
export function stamp(state: HlcState, pt: number): { state: HlcState; hlc: Hlc } {
  const l = Math.max(state.l, pt);
  const c = l === state.l ? state.c + 1 : 0;
  if (c >= HLC_C_LIMIT) throw new SeqscribeError("ERR_ENTRY_ENCODING", "hlc.c overflow at stamp");
  const next = { l, c };
  return { state: next, hlc: { ...next } };
}

// Merge on receive (first-sight accepted entries only — caller enforces §3's
// eligibility). Standard HLC merge; the carry rule turns a c that would reach
// 2^32 into (l+1, 0) — clock state only, entry stamps are never touched.
export function merge(state: HlcState, remote: Hlc, pt: number): HlcState {
  const l = Math.max(state.l, remote.l, pt);
  let c: number;
  if (l === state.l && l === remote.l) c = Math.max(state.c, remote.c) + 1;
  else if (l === state.l) c = state.c + 1;
  else if (l === remote.l) c = remote.c + 1;
  else c = 0;
  if (c >= HLC_C_LIMIT) return { l: l + 1, c: 0 };
  return { l, c };
}

export function isOverEpsilon(remote: Hlc, pt: number, epsilonMs: number): boolean {
  return remote.l > pt + epsilonMs;
}

export function hlcCompare(a: Hlc, b: Hlc): number {
  if (a.l !== b.l) return a.l < b.l ? -1 : 1;
  if (a.c !== b.c) return a.c < b.c ? -1 : 1;
  return 0;
}

// Total order (§1): lexicographic (hlc.l, hlc.c, writer, seq); writer bytewise
// (charter is ASCII, so JS string comparison is bytewise).
export function orderCompare(a: Order, b: Order): number {
  if (a.l !== b.l) return a.l < b.l ? -1 : 1;
  if (a.c !== b.c) return a.c < b.c ? -1 : 1;
  if (a.writer !== b.writer) return a.writer < b.writer ? -1 : 1;
  if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1;
  return 0;
}

export function orderOf(e: LogEntry): Order {
  return { l: e.hlc.l, c: e.hlc.c, writer: e.writer, seq: e.seq };
}
