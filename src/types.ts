// Public type surface — mirrors SPEC v3.4 normative ts blocks (§1, §2, §7.2, §7.7,
// §9, §11.6, §13, §14). CI cross-checks against types.d.ts generated from the SPEC.

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [k: string]: JsonValue };
export type Row = Record<string, string | number | null>;
export type Unsub = () => void;

export type Topic = string; // /^[a-z0-9_.-]{1,128}$/ — ASCII only
export type WriterId = string; // /^[A-Za-z0-9_.:-]{1,128}$/ — ASCII only; host-issued canonical id
export type Seq = number; // per (topic, writer) stream, monotonic from 1, gap-free at the author
export type Hlc = { l: number; c: number };
export type EntryId = [Topic, WriterId, Seq];
export type Order = { l: number; c: number; writer: WriterId; seq: Seq };
export type Key = string; // register only, UTF-8 ≤512B

export interface LogEntry {
  topic: Topic;
  writer: WriterId;
  seq: Seq;
  hlc: Hlc;
  kind: string;
  key?: Key;
  causal?: [WriterId, Seq];
  ref?: EntryId;
  payload: JsonValue;
  chain: string;
}

export interface FinalityCert {
  topic: Topic;
  order: Order; // watermark P — THE normative judgment
  cut: Record<WriterId, { seq: Seq; chain: string }>; // projection of P; absent writer ⇒ seq 0
  generation: number; // strictly increasing per topic; reuse forbidden
  authority: string; // stable authority ID (a role, not a node)
  sig: string;
}

export interface WriterDirective {
  topic: Topic;
  writer: WriterId;
  state: "live" | "retired";
  rgen: number; // strictly increasing per (topic, writer)
  finalSeq?: Seq;
  finalChain?: string; // required when state=="retired"
  authority: string;
  sig: string;
}

export interface RegisterSnapshotState {
  keys: Record<
    Key,
    {
      value?: JsonValue; // absent = deleted scalar state
      winner: EntryId;
      winnerOrder: Order;
      owner?: WriterId;
      resetGen: number;
      members?: Record<
        string,
        {
          present: boolean;
          entry: EntryId;
          order: Order;
          frontier?: EntryId[];
          superseded?: EntryId[];
        }
      >;
      frontier: EntryId[]; // key-level unresolved heads
      superseded?: EntryId[];
    }
  >;
}

export interface SnapshotBody {
  topic: Topic;
  order: Order;
  generation: number;
  certHash: string;
  cut: Record<WriterId, { seq: Seq; chain: string }>;
  directives: WriterDirective[]; // the SIGNED directive originals current at snapshot time
  register?: RegisterSnapshotState;
  views: { name: string; version: string; state: JsonValue; stateHash: string }[];
}

export interface ViewDef<S extends JsonValue, R extends Row> {
  version: string;
  init: S;
  reduce: (s: S, e: LogEntry) => S; // pure, deterministic, non-mutating
  rows: (s: S) => Iterable<R>; // pure; order meaningless — stored sorted by rowKey
  rowKey: string;
  schema: Record<string, "TEXT" | "INTEGER" | "REAL">;
  delta?: (s: S, e: LogEntry) => { upserts: R[]; deletes: string[] };
  fts?: string[];
}

export interface Constants {
  MAX_ENTRY_BYTES: number;
  MAX_FRAME_BYTES: number;
  MAX_ROW_BYTES: number;
  MAX_REASSEMBLY_BYTES: number;
  INFLIGHT_CREDITS: number;
  CHANNEL_STALL_MS: number;
  SEND_QUEUE_CAP: number;
  CONTROL_RETRY_MS: number;
  PENDING_CAP: number;
  SUB_DELTA_RETAIN: number;
  EAGER_PUSH_K: number;
  ANTI_ENTROPY_MS: number;
  BEACON_DEBOUNCE_MS: number;
  HLC_EPSILON_MS: number;
  CLOCK_WARN_MS: number;
  CHECKPOINT_EVERY: number;
  RING_DEFAULT: number;
  GROUP_COMMIT_MS: number;
  GROUP_COMMIT_N: number;
  HELLO_TIMEOUT_MS: number;
  FINALITY_WINDOW_MS: number;
  RETIRE_GRACE_MS: number;
  REQUEST_TTL_MS: number;
  TOMBSTONE_RETAIN_MS: number;
  durability: "normal" | "full";
}

export type ErrCode =
  | "ERR_PROTO_VERSION"
  | "ERR_SCHEMA_MISMATCH"
  | "ERR_ACL_DENIED"
  | "ERR_UNKNOWN_TOPIC"
  | "ERR_UNKNOWN_VIEW"
  | "ERR_FUTURE_CURSOR"
  | "ERR_ENTRY_TOO_LARGE"
  | "ERR_ENTRY_ENCODING"
  | "ERR_PRE_FINALITY"
  | "ERR_OWNED_REJECT"
  | "ERR_WRITER_SEALED"
  | "ERR_DB_OWNED"
  | "ERR_STORAGE"
  | "ERR_MISUSE";

export interface SqliteHandle {
  run(sql: string, params?: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  get<T = unknown>(sql: string, params?: unknown[]): T | undefined;
  all<T = unknown>(sql: string, params?: unknown[]): T[];
  transaction<T>(fn: () => T): T;
  acquireOwnerLock(): void; // throws if another process owns the DB
  releaseOwnerLock(): void;
}

export interface Channel {
  send(msg: string): void;
  onMessage(cb: (m: string) => void): void;
  onClose(cb: () => void): void;
  close(): void;
}

export interface TopicPolicy {
  kind: "append" | "register";
  retention: { mode: "full" } | { mode: "ring"; size: number } | { mode: "none" };
  replication: "full-sync" | "subscribe-only";
  access: "content" | "metadata";
  conflict?: {
    default?: "lww" | "fww" | "resolver" | "owned";
    overrides?: Record<string, "lww" | "fww" | "resolver" | "owned">;
  };
  finalityAuthority?: string;
  hintKeys?: "plain" | "hash";
  flushThrottleMs?: number;
}

export interface AuthorityHooks {
  verifyFinality?: (cert: FinalityCert) => boolean | Promise<boolean>;
  verifyWriterDirective?: (d: WriterDirective) => boolean | Promise<boolean>;
  verifyTakeover?: (evt: LogEntry) => boolean | Promise<boolean>;
  issueWriterDirective?: (
    unsigned: Omit<WriterDirective, "authority" | "sig">,
  ) => Promise<WriterDirective>;
  issueTakeover?: (unsigned: {
    topic: Topic;
    key: Key;
    newOwner: WriterId;
  }) => Promise<{ hostSig: string }>;
}

export interface Timers {
  setTimeout(cb: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
}

export interface CreateOpts {
  writerId: WriterId;
  storage: SqliteHandle;
  clock?: () => number;
  rng?: () => number;
  timers?: Timers;
  authority?: AuthorityHooks;
  constants?: Partial<Constants>;
}

export interface RegisterHandle {
  set(key: Key, value: JsonValue): Promise<EntryId>;
  del(key: Key): Promise<EntryId>;
  add(key: Key, member: string): Promise<EntryId>;
  remove(key: Key, member: string): Promise<EntryId>;
  resolve(key: Key, supersedes: EntryId[], value: JsonValue): Promise<EntryId>;
  resolveMember(
    key: Key,
    member: string,
    supersedes: EntryId[],
    present: boolean,
  ): Promise<EntryId>;
  chown(key: Key, newOwner: WriterId): Promise<EntryId>;
  takeover(key: Key, newOwner: WriterId): Promise<EntryId>;
  request(key: Key, value: JsonValue): Promise<EntryId>;
}

export interface PeerHandle {
  peerId: string;
  state(): "attached" | "ready" | "closed";
  onStateChange(cb: (s: "attached" | "ready" | "closed") => void): Unsub;
  detach(): void;
}

export interface ViewHandle {
  name: string;
  version: string;
  rebuild(): Promise<void>;
  query<T = Row>(sql: string, params?: unknown[]): T[];
}

export interface Subscription {
  onSnapshot(cb: (rows: Row[], reset: boolean) => void): Unsub;
  onDelta(cb: (c: { upserts: Row[]; deletes: string[] }) => void): Unsub;
  cursor: string | undefined;
  close(): void;
}

export interface BeaconTransport {
  put(body: BeaconReport): Promise<void>;
  get(): Promise<BeaconReport[]>;
}
export interface BeaconHandle {
  stop(): void;
}
export interface BeaconReport {
  node: string;
  at: string;
  vectors: HaveVectors;
  hints?: Record<Topic, Record<string, [WriterId, Seq]>>;
}

export interface ConflictBase {
  topic: Topic;
  key: Key;
  entries: LogEntry[];
  concurrent: true;
  provisionalWinner: LogEntry;
}
export interface KeyConflict extends ConflictBase {
  member?: undefined;
  resolve(value: JsonValue): Promise<EntryId>;
}
export interface MemberConflict extends ConflictBase {
  member: string;
  resolveMember(present: boolean): Promise<EntryId>;
}
export type Conflict = KeyConflict | MemberConflict;

export interface OwnedRequest {
  topic: Topic;
  key: Key;
  value: JsonValue;
  requester: WriterId;
  entry: EntryId;
  at: Hlc;
}

export interface Staleness {
  behind: Record<WriterId, number>;
  asOf: string;
  keyStale?: { latestKnown: EntryId; haveLocally: boolean };
}

export type HaveVectors = Record<
  Topic,
  {
    fgen?: number;
    writers: Record<
      WriterId,
      | { contig: Seq; chain: string; rgen?: number }
      | { retired: true; finalSeq: Seq; finalChain: string; rgen: number }
    >;
  }
>;

export interface Anomaly {
  kind:
    | "clock_outlier"
    | "writer_forked"
    | "owned_violation"
    | "pre_finality_rejected"
    | "entry_quarantined"
    | "takeover_invalid"
    | "bad_cert"
    | "bad_directive"
    | "delta_mismatch"
    | "view_faulted"
    | "consumer_abandoned"
    | "canonical_unavailable"
    | "sync_stalled"; // extension (proposals-v3.5 P22): WANT rounds toward a peer stopped progressing
  entry?: LogEntry;
}

export interface SeqscribeNode {
  defineTopic(topic: Topic, policy: TopicPolicy): void;
  log(topic: Topic): {
    append(kind: string, payload: JsonValue, o?: { ref?: EntryId }): Promise<EntryId>;
  };
  register(topic: Topic): RegisterHandle;
  onEntry(topic: Topic, consumer: string, cb: (e: LogEntry) => void | Promise<void>): Unsub;
  attach(
    ch: Channel,
    o: {
      peerId: string;
      peerClass: "content" | "metadata";
      grants: Record<Topic, "full" | "serve" | "none">;
    },
  ): PeerHandle;
  vectors(): HaveVectors;
  setKnownVectors(v: BeaconReport[]): void;
  staleness(topic: Topic, key?: Key): Staleness;
  beacon(t: BeaconTransport): BeaconHandle;
  finality(topic: Topic): FinalityCert | null;
  proposeFinality(topic: Topic): Omit<FinalityCert, "sig"> | null;
  ingestFinality(cert: FinalityCert): Promise<void>;
  publishWriterDirective(d: WriterDirective): Promise<void>;
  retire(writer: WriterId, o?: { chownTo?: WriterId }): Promise<void>;
  unretire(writer: WriterId): Promise<void>;
  view<S extends JsonValue, R extends Row>(
    name: string,
    topic: Topic,
    def: ViewDef<S, R>,
  ): ViewHandle;
  serveView(name: string, resolver: (params: JsonValue) => ViewHandle): void;
  subscribe(
    peer: PeerHandle,
    o: { view: string; params: JsonValue; fromCursor?: string },
  ): Subscription;
  onConflict(topic: Topic, cb: (c: Conflict) => void | Promise<void>): Unsub;
  onOwnedRequest(topic: Topic, cb: (r: OwnedRequest) => Promise<"approve" | "deny">): Unsub;
  onAnomaly(cb: (a: Anomaly) => void): Unsub;
  ownerOf(topic: Topic, key: Key): WriterId | null;
  pendingRequests(topic: Topic, now?: number): OwnedRequest[];
  rebuildView(name: string): Promise<void>;
  export(topic: Topic, format: "jsonl"): AsyncIterable<string>;
  import(topic: Topic, lines: AsyncIterable<string>): Promise<number>;
  close(): Promise<void>;
}
