// SPEC §5.4 wire messages — types plus parse/serialize with structural
// validation for EVERY handled message type. parseMsg is the single wire
// ingress gate: handlers may trust field presence and primitive types after
// it. Deep semantics stay with their owners — chain/signature verification
// with the hubs (§4, §7, §13), per-entry LogEntry validation with
// codec.validateEntry at apply time (§6.1), ACL re-verification per §5.4.

import { CHAIN_RE, TOPIC_RE, WRITER_RE } from "./codec.js";
import { utf8ByteLength } from "./encoding.js";
import { SeqscribeError } from "./errors.js";
import type {
  Constants,
  ErrCode,
  FinalityCert,
  HaveVectors,
  JsonValue,
  LogEntry,
  Order,
  Row,
  Seq,
  Topic,
  WriterDirective,
  WriterId,
} from "./types.js";

export interface MsgHello {
  t: "HELLO";
  protoMin: number;
  protoMax: number;
  node: WriterId;
  grants: Record<Topic, { mode: "full" | "serve" | "none"; schemaHash: string }>;
  // P15 grant re-advertisement (extension; absent ≡ 0 / not-an-ack): grantsGen
  // versions the sender's grant map; ackGen marks the frame as the answer to a
  // post-ready re-advertisement carrying that gen. Pre-P15 peers ignore both.
  grantsGen?: number;
  ackGen?: number;
}
export interface MsgHaveGet {
  t: "HAVE_GET";
  req: number;
}
export interface MsgHave {
  t: "HAVE";
  req: number;
  page: number;
  of: number;
  vectors: HaveVectors;
}
export interface MsgWant {
  t: "WANT";
  req: number;
  topic: Topic;
  writer: WriterId;
  fromSeq: Seq;
}
export interface MsgEntries {
  t: "ENTRIES";
  mid: number;
  req?: number;
  topic: Topic;
  writer: WriterId;
  fromSeq: Seq;
  toSeq: Seq;
  entries: LogEntry[];
  done: boolean;
}
export interface MsgProbe {
  t: "PROBE";
  topic: Topic;
  writer: WriterId;
  seqs: Seq[];
}
export interface MsgProbeRes {
  t: "PROBE_RES";
  topic: Topic;
  writer: WriterId;
  points: { seq: Seq; chain: string }[];
  unavailable?: { belowSeq: Seq };
}
export interface MsgFinality {
  t: "FINALITY";
  topic: Topic;
  cert: FinalityCert;
}
export interface MsgWriterDirective {
  t: "WRITER_DIRECTIVE";
  directive: WriterDirective;
}
export interface MsgSnapshotOffer {
  t: "SNAPSHOT_OFFER";
  topic: Topic;
  snapshotId: string;
  order: Order;
  cut: Record<WriterId, { seq: Seq; chain: string }>;
  certHash: string;
}
export interface MsgSnapshotGet {
  t: "SNAPSHOT_GET";
  topic: Topic;
  snapshotId: string;
  wants: { name: string; version: string }[];
}
export interface MsgSnapshot {
  t: "SNAPSHOT";
  mid: number;
  topic: Topic;
  snapshotId: string;
  chunk: number;
  of: number;
  data: string;
  totalHash?: string;
}
export interface MsgSub {
  t: "SUB";
  subId: number;
  view: string;
  params: JsonValue;
  fromCursor?: string;
}
export interface MsgSnap {
  t: "SNAP";
  mid: number;
  subId: number;
  chunk: number;
  of: number;
  data: string;
  cursor: string;
  reset: boolean;
  totalHash?: string;
}
export interface MsgDelta {
  t: "DELTA";
  mid: number;
  subId: number;
  changes: { upserts: Row[]; deletes: string[] };
  cursor: string;
}
export interface MsgUnsub {
  t: "UNSUB";
  subId: number;
}
export interface MsgSubErr {
  t: "SUB_ERR";
  subId: number;
  code: ErrCode;
}
export interface MsgAck {
  t: "ACK";
  upTo: number;
}
export interface MsgErr {
  t: "ERR";
  code: ErrCode;
  ref?: string;
  detail?: string;
}

export type DataMsg = MsgEntries | MsgSnapshot | MsgSnap | MsgDelta;
export type ControlMsg =
  | MsgHello
  | MsgHaveGet
  | MsgHave
  | MsgWant
  | MsgProbe
  | MsgProbeRes
  | MsgFinality
  | MsgWriterDirective
  | MsgSnapshotOffer
  | MsgSnapshotGet
  | MsgSub
  | MsgUnsub
  | MsgSubErr
  | MsgAck
  | MsgErr;
export type WireMsg = DataMsg | ControlMsg;

export const DATA_TYPES: ReadonlySet<string> = new Set(["ENTRIES", "SNAPSHOT", "SNAP", "DELTA"]);
const ALL_TYPES: ReadonlySet<string> = new Set([
  "HELLO",
  "HAVE_GET",
  "HAVE",
  "WANT",
  "ENTRIES",
  "PROBE",
  "PROBE_RES",
  "FINALITY",
  "WRITER_DIRECTIVE",
  "SNAPSHOT_OFFER",
  "SNAPSHOT_GET",
  "SNAPSHOT",
  "SUB",
  "SNAP",
  "DELTA",
  "UNSUB",
  "SUB_ERR",
  "ACK",
  "ERR",
]);

// ---- structural validation (§5.4) ----
//
// The schema below is derived from what this implementation EMITS (the send
// sites in session/sync/subs/snapshot) — optional fields stay optional and no
// field is constrained tighter than its producer, so mixed-version fleets
// interoperate. Violations are ERR_ENTRY_ENCODING, same as unparseable JSON.

type Rec = Record<string, unknown>;

// PROBE seqs bound (proposals-v3.5 P7): our emitter (sync.locateFork's
// log-spaced sweep) stops at 32 probe points, and 32 points already span any
// seq range (1 + 2^31 exceeds every safe contig). 64 = 2× headroom for a
// second implementation's sweep; beyond that is per-frame work amplification
// (each seq costs the responder a store lookup), not a conforming probe.
const MAX_PROBE_SEQS = 64;

function bad(t: string, why: string): never {
  throw new SeqscribeError("ERR_ENTRY_ENCODING", `${t}: ${why}`);
}

function vInt(t: string, v: unknown, name: string): number {
  if (!Number.isSafeInteger(v)) bad(t, `${name} must be a safe integer`);
  return v as number;
}

function vNonNeg(t: string, v: unknown, name: string): number {
  if (!Number.isSafeInteger(v) || (v as number) < 0)
    bad(t, `${name} must be a non-negative safe integer`);
  return v as number;
}

function vPos(t: string, v: unknown, name: string): number {
  if (!Number.isSafeInteger(v) || (v as number) < 1)
    bad(t, `${name} must be a positive safe integer`);
  return v as number;
}

function vStr(t: string, v: unknown, name: string): string {
  if (typeof v !== "string") bad(t, `${name} must be a string`);
  return v as string;
}

function vBool(t: string, v: unknown, name: string): boolean {
  if (typeof v !== "boolean") bad(t, `${name} must be a boolean`);
  return v as boolean;
}

function vTopic(t: string, v: unknown, name: string): string {
  if (typeof v !== "string" || !TOPIC_RE.test(v)) bad(t, `${name} is not a valid topic`);
  return v as string;
}

function vWriter(t: string, v: unknown, name: string): string {
  if (typeof v !== "string" || !WRITER_RE.test(v)) bad(t, `${name} is not a valid writer id`);
  return v as string;
}

function vChain(t: string, v: unknown, name: string): string {
  if (typeof v !== "string" || !CHAIN_RE.test(v)) bad(t, `${name} is not a 64-hex hash`);
  return v as string;
}

function vArr(t: string, v: unknown, name: string): unknown[] {
  if (!Array.isArray(v)) bad(t, `${name} must be an array`);
  return v as unknown[];
}

// Wire record maps (grants, vectors, writers, cut) carry attacker-chosen keys
// that downstream code writes into plain objects — a "__proto__" own key (which
// JSON.parse happily creates, and TOPIC_RE/WRITER_RE happily match) would flow
// through Object.assign/index-assignment as a prototype SET, silently corrupting
// the merge. Such a key already misbehaves in every ordinary-object consumer,
// so rejecting it loses nothing conforming.
function vMap(t: string, v: unknown, name: string): [string, unknown][] {
  if (typeof v !== "object" || v === null || Array.isArray(v))
    bad(t, `${name} must be an object`);
  const entries = Object.entries(v as Rec);
  for (const [k] of entries) if (k === "__proto__") bad(t, `${name} key __proto__ forbidden`);
  return entries;
}

// One HAVE vectors entry: {contig, chain, rgen?} | {retired:true, finalSeq, finalChain, rgen}
function vHaveWriter(t: string, v: unknown, name: string): void {
  if (typeof v !== "object" || v === null) bad(t, `${name} must be an object`);
  const w = v as Rec;
  if (w.retired !== undefined) {
    if (w.retired !== true) bad(t, `${name}.retired must be true when present`);
    vNonNeg(t, w.finalSeq, `${name}.finalSeq`);
    vChain(t, w.finalChain, `${name}.finalChain`);
    vNonNeg(t, w.rgen, `${name}.rgen`);
    return;
  }
  vNonNeg(t, w.contig, `${name}.contig`);
  vChain(t, w.chain, `${name}.chain`);
  if (w.rgen !== undefined) vNonNeg(t, w.rgen, `${name}.rgen`);
}

function validateShape(m: Rec): void {
  const t = m.t as string;
  switch (t) {
    case "HELLO": {
      vNonNeg(t, m.protoMin, "protoMin");
      vNonNeg(t, m.protoMax, "protoMax");
      vWriter(t, m.node, "node");
      if (m.grantsGen !== undefined) vNonNeg(t, m.grantsGen, "grantsGen");
      if (m.ackGen !== undefined) vNonNeg(t, m.ackGen, "ackGen");
      for (const [topic, g] of vMap(t, m.grants, "grants")) {
        vTopic(t, topic, "grants key");
        if (typeof g !== "object" || g === null) bad(t, `grants[${topic}] must be an object`);
        const gr = g as Rec;
        if (gr.mode !== "full" && gr.mode !== "serve" && gr.mode !== "none")
          bad(t, `grants[${topic}].mode unknown`);
        vStr(t, gr.schemaHash, `grants[${topic}].schemaHash`);
      }
      break;
    }
    case "HAVE_GET":
      vInt(t, m.req, "req"); // opaque round correlator — any safe integer
      break;
    case "HAVE": {
      vInt(t, m.req, "req");
      const of = vPos(t, m.of, "of");
      if (vPos(t, m.page, "page") > of) bad(t, "page exceeds of");
      for (const [topic, v] of vMap(t, m.vectors, "vectors")) {
        vTopic(t, topic, "vectors key");
        if (typeof v !== "object" || v === null) bad(t, `vectors[${topic}] must be an object`);
        const tv = v as Rec;
        if (tv.fgen !== undefined) vNonNeg(t, tv.fgen, `vectors[${topic}].fgen`);
        for (const [writer, w] of vMap(t, tv.writers, `vectors[${topic}].writers`)) {
          vWriter(t, writer, `vectors[${topic}] writer key`);
          vHaveWriter(t, w, `vectors[${topic}].writers[${writer}]`);
        }
      }
      break;
    }
    case "WANT":
      vInt(t, m.req, "req");
      vTopic(t, m.topic, "topic");
      vWriter(t, m.writer, "writer");
      vPos(t, m.fromSeq, "fromSeq");
      break;
    case "ENTRIES":
      if (m.req !== undefined) vInt(t, m.req, "req");
      vTopic(t, m.topic, "topic");
      vWriter(t, m.writer, "writer");
      vPos(t, m.fromSeq, "fromSeq");
      vNonNeg(t, m.toSeq, "toSeq");
      vArr(t, m.entries, "entries"); // per-entry validation at apply (§6.1)
      vBool(t, m.done, "done");
      break;
    case "PROBE": {
      vTopic(t, m.topic, "topic");
      vWriter(t, m.writer, "writer");
      const seqs = vArr(t, m.seqs, "seqs");
      if (seqs.length > MAX_PROBE_SEQS) bad(t, `seqs exceeds ${MAX_PROBE_SEQS} probe points`);
      for (const s of seqs) vPos(t, s, "seqs[]");
      break;
    }
    case "PROBE_RES": {
      vTopic(t, m.topic, "topic");
      vWriter(t, m.writer, "writer");
      for (const p of vArr(t, m.points, "points")) {
        if (typeof p !== "object" || p === null) bad(t, "points[] must be objects");
        vPos(t, (p as Rec).seq, "points[].seq");
        vChain(t, (p as Rec).chain, "points[].chain");
      }
      if (m.unavailable !== undefined) {
        if (typeof m.unavailable !== "object" || m.unavailable === null)
          bad(t, "unavailable must be an object");
        vPos(t, (m.unavailable as Rec).belowSeq, "unavailable.belowSeq");
      }
      break;
    }
    case "FINALITY": {
      // structural only — FinalityHub verifies signature/generation/order (§7)
      vTopic(t, m.topic, "topic");
      if (typeof m.cert !== "object" || m.cert === null) bad(t, "cert must be an object");
      const c = m.cert as Rec;
      vTopic(t, c.topic, "cert.topic");
      vNonNeg(t, c.generation, "cert.generation");
      vStr(t, c.authority, "cert.authority");
      vStr(t, c.sig, "cert.sig");
      if (typeof c.order !== "object" || c.order === null) bad(t, "cert.order must be an object");
      for (const [writer, cut] of vMap(t, c.cut, "cert.cut")) {
        vWriter(t, writer, "cert.cut key");
        if (typeof cut !== "object" || cut === null) bad(t, "cert.cut[] must be objects");
        vNonNeg(t, (cut as Rec).seq, "cert.cut[].seq");
        vStr(t, (cut as Rec).chain, "cert.cut[].chain");
      }
      break;
    }
    case "WRITER_DIRECTIVE": {
      // structural only — DirectiveHub verifies signature/rgen/state rules (§13)
      if (typeof m.directive !== "object" || m.directive === null)
        bad(t, "directive must be an object");
      const d = m.directive as Rec;
      vTopic(t, d.topic, "directive.topic");
      vWriter(t, d.writer, "directive.writer");
      if (d.state !== "live" && d.state !== "retired") bad(t, "directive.state unknown");
      vNonNeg(t, d.rgen, "directive.rgen");
      if (d.finalSeq !== undefined) vNonNeg(t, d.finalSeq, "directive.finalSeq");
      if (d.finalChain !== undefined) vStr(t, d.finalChain, "directive.finalChain");
      vStr(t, d.authority, "directive.authority");
      vStr(t, d.sig, "directive.sig");
      break;
    }
    case "SNAPSHOT_OFFER": {
      vTopic(t, m.topic, "topic");
      vChain(t, m.snapshotId, "snapshotId"); // sha256(JCS(body)) — always 64-hex
      vChain(t, m.certHash, "certHash");
      if (typeof m.order !== "object" || m.order === null) bad(t, "order must be an object");
      for (const [writer, cut] of vMap(t, m.cut, "cut")) {
        vWriter(t, writer, "cut key");
        if (typeof cut !== "object" || cut === null) bad(t, "cut[] must be objects");
        vNonNeg(t, (cut as Rec).seq, "cut[].seq");
        vStr(t, (cut as Rec).chain, "cut[].chain");
      }
      break;
    }
    case "SNAPSHOT_GET":
      vTopic(t, m.topic, "topic");
      vChain(t, m.snapshotId, "snapshotId");
      for (const w of vArr(t, m.wants, "wants")) {
        if (typeof w !== "object" || w === null) bad(t, "wants[] must be objects");
        vStr(t, (w as Rec).name, "wants[].name");
        vStr(t, (w as Rec).version, "wants[].version");
      }
      break;
    case "SNAPSHOT": {
      vTopic(t, m.topic, "topic");
      vChain(t, m.snapshotId, "snapshotId");
      const of = vPos(t, m.of, "of");
      if (vPos(t, m.chunk, "chunk") > of) bad(t, "chunk exceeds of"); // bounds reassembly maps
      vStr(t, m.data, "data");
      if (m.totalHash !== undefined) vChain(t, m.totalHash, "totalHash");
      break;
    }
    case "SUB":
      vInt(t, m.subId, "subId");
      vStr(t, m.view, "view");
      // params: any JSON value, absent when the subscriber passed undefined
      if (m.fromCursor !== undefined) vStr(t, m.fromCursor, "fromCursor");
      break;
    case "SNAP": {
      vInt(t, m.subId, "subId");
      const of = vPos(t, m.of, "of");
      if (vPos(t, m.chunk, "chunk") > of) bad(t, "chunk exceeds of"); // bounds reassembly maps
      vStr(t, m.data, "data");
      vStr(t, m.cursor, "cursor");
      vBool(t, m.reset, "reset");
      if (m.totalHash !== undefined) vStr(t, m.totalHash, "totalHash");
      break;
    }
    case "DELTA": {
      vInt(t, m.subId, "subId");
      vStr(t, m.cursor, "cursor");
      if (typeof m.changes !== "object" || m.changes === null)
        bad(t, "changes must be an object");
      const ch = m.changes as Rec;
      for (const u of vArr(t, ch.upserts, "changes.upserts")) {
        if (typeof u !== "object" || u === null || Array.isArray(u))
          bad(t, "changes.upserts[] must be row objects");
      }
      for (const d of vArr(t, ch.deletes, "changes.deletes")) vStr(t, d, "changes.deletes[]");
      break;
    }
    case "UNSUB":
      vInt(t, m.subId, "subId");
      break;
    case "SUB_ERR":
      vInt(t, m.subId, "subId");
      // code is an open string set: a newer peer may emit codes we don't know
      if (typeof m.code !== "string" || m.code.length === 0) bad(t, "code must be a string");
      break;
    case "ACK":
      vNonNeg(t, m.upTo, "upTo");
      break;
    case "ERR":
      if (typeof m.code !== "string" || m.code.length === 0) bad(t, "code must be a string");
      if (m.ref !== undefined) vStr(t, m.ref, "ref");
      if (m.detail !== undefined) vStr(t, m.detail, "detail");
      break;
    default:
      break; // unreachable — ALL_TYPES gate in parseMsg
  }
}

export function serializeMsg(m: WireMsg, constants: Constants): string {
  const s = JSON.stringify(m);
  if (utf8ByteLength(s) > constants.MAX_FRAME_BYTES)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", `frame exceeds MAX_FRAME_BYTES (${m.t})`);
  return s;
}

export function parseMsg(raw: string, constants: Constants): WireMsg {
  if (utf8ByteLength(raw) > constants.MAX_FRAME_BYTES)
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "frame exceeds MAX_FRAME_BYTES");
  let v: unknown;
  try {
    v = JSON.parse(raw);
  } catch {
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "frame is not JSON");
  }
  if (typeof v !== "object" || v === null || typeof (v as { t?: unknown }).t !== "string")
    throw new SeqscribeError("ERR_ENTRY_ENCODING", "frame has no message type");
  const t = (v as { t: string }).t;
  if (!ALL_TYPES.has(t)) throw new SeqscribeError("ERR_ENTRY_ENCODING", `unknown message type ${t}`);
  if (DATA_TYPES.has(t) && !Number.isSafeInteger((v as { mid?: unknown }).mid))
    throw new SeqscribeError("ERR_ENTRY_ENCODING", `data message ${t} without mid`);
  validateShape(v as Record<string, unknown>);
  return v as WireMsg;
}
