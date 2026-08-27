// Public API surface. §14 defines the normative contract; everything exported
// here is either §14 surface, a spec-defined pure helper (§3 HLC, §4 encoding,
// §1 charters), or the shipped reference tier (transports, adapters, host
// sugar). Internal machinery (Store, LogCore, Session, SyncEngine, the hubs,
// wire message codecs) is deliberately NOT re-exported: tests and the harness
// import those from their modules directly, and keeping them off the package
// surface is what lets them change without a semver event. `coreOf` is the
// one sanctioned escape hatch (unstable — see node.ts).
export * from "./types.js";
export { SeqscribeError } from "./errors.js";
export { DEFAULT_CONSTANTS, resolveConstants } from "./constants.js";
export {
  assertJsonValue,
  sanitizeJson,
  jcs,
  frame,
  dec,
  seedOf,
  chainOf,
  certHashOf,
  snapshotIdOf,
  normalizedPolicy,
  topicSchemaHashOf,
  sha256HexUtf8,
  utf8ByteLength,
} from "./encoding.js";
export {
  stamp,
  merge,
  isOverEpsilon,
  hlcCompare,
  orderCompare,
  orderOf,
  HLC_C_LIMIT,
  type HlcState,
} from "./hlc.js";
export {
  validateEntry,
  assertEntrySize,
  estimateEntryBytes,
  assertTopic,
  assertWriter,
  assertKey,
  TOPIC_RE,
  WRITER_RE,
  type AppendShape,
} from "./codec.js";
// Peer-session extension types (proposals-v3.5 P10/P15/P16) — the Session
// class itself stays internal; only the handle/event shapes are public.
export type {
  SessionCloseReason,
  PeerLifecycleEvent,
  PeerHandleExt,
} from "./session.js";
export { validatePolicy, conflictPolicyFor } from "./topics.js";
export { exportTopic, importTopic } from "./export.js";
export { httpBeaconTransport, beaconFetchHandler, type FetchRequestLike } from "./beacon.js";
export { webSocketChannel, dataChannelChannel, type WebSocketLike } from "./ws.js";
export {
  betterSqlite3Handle,
  sqliteWasmHandle,
  durableObjectSqlHandle,
  type BetterSqlite3Like,
  type LockDbLike,
  type SqliteWasmDbLike,
  type DurableObjectSqlLike,
} from "./adapters.js";
export {
  hmacAuthority,
  startFinalityLoop,
  manageReconnect,
  loadOrCreateWriterId,
  migrateLegacyJsonl,
  type HmacAuthority,
  type HmacAuthorityOpts,
  type FinalityLoopHandle,
  type ReconnectHandle,
  type PeerUnresponsiveInfo,
} from "./host.js";
export { createSeqscribe, coreOf, type SeqscribeNodeExt, type NodeStats } from "./node.js";
