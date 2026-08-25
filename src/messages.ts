// SPEC §5.4 wire messages — types plus parse/serialize with structural
// validation for the milestone-② subset (HELLO/HAVE/WANT/ENTRIES/PROBE/ACK/ERR).
// Later-milestone messages are typed now and validated when their handlers land.

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
  return v as WireMsg;
}
