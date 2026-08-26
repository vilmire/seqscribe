// SPEC §11 — register semantics: a pure fold over totally-ordered entries
// (winner/owner/frontier/resetGen per key, member sub-registers), race-safe
// causal stamping through the commit queue, conflict surfacing, and the owned
// request/approval/takeover flow. Late arrivals refold from scratch (registers
// are small; checkpointed folds are a later optimization).

import { jcs, sha256HexUtf8 } from "./encoding.js";
import { misuse, SeqscribeError } from "./errors.js";
import { orderCompare, orderOf } from "./hlc.js";
import type { LogCore } from "./log.js";
import type { Store } from "./store.js";
import { conflictPolicyFor, type TopicRegistry } from "./topics.js";
import type {
  Anomaly,
  AuthorityHooks,
  Conflict,
  Constants,
  EntryId,
  JsonValue,
  Key,
  LogEntry,
  Order,
  OwnedRequest,
  RegisterHandle,
  RegisterSnapshotState,
  Timers,
  Topic,
  Unsub,
  WriterId,
} from "./types.js";

const ANCESTOR_WALK_CAP = 1024;

interface MemberState {
  present: boolean;
  entry: EntryId;
  order: Order;
  frontier: EntryId[];
  superseded: EntryId[];
}

interface RequestState {
  entry: EntryId;
  requester: WriterId;
  value: JsonValue;
  at: { l: number; c: number };
  approvedBy?: EntryId;
}

interface KeyState {
  value?: JsonValue | undefined;
  scalar: boolean; // false once the key holds a member set (§11.5 mirror rules)
  winner?: EntryId;
  winnerOrder?: Order;
  owner?: WriterId;
  resetGen: number;
  members: Map<string, MemberState>;
  frontier: EntryId[];
  superseded: EntryId[];
  requests: RequestState[];
}

interface TopicState {
  keys: Map<Key, KeyState>;
  ord: Order | null;
  lastRowid: number;
  // bootstrap base (§7.8): refolds restart from this floor, not from empty
  base?: { keys: Map<Key, KeyState>; ord: Order } | undefined;
}

export interface RegisterHubDeps {
  core: LogCore;
  store: Store;
  topics: TopicRegistry;
  writerId: WriterId;
  constants: Constants;
  timers: Timers;
  clock: () => number;
  emitAnomaly: (a: Anomaly) => void;
  authority?: AuthorityHooks | undefined;
}

export class RegisterHub {
  private readonly states = new Map<Topic, TopicState>();
  private readonly changeListeners = new Set<(topic: Topic) => void>();
  private readonly conflictCbs = new Map<Topic, Set<(c: Conflict) => void | Promise<void>>>();
  private readonly requestCbs = new Map<
    Topic,
    Set<(r: OwnedRequest) => Promise<"approve" | "deny">>
  >();
  private readonly surfacedConflicts = new Set<string>();
  private readonly handledRequests = new Set<string>();
  private readonly takeoverVerdicts = new Map<string, boolean>();
  // own uncommitted tail per causal scope — updated inside commit processing (§11.2)
  private readonly scopeTails = new Map<string, [WriterId, number]>();
  private readonly scheduled = new Set<Topic>();
  private readonly materializing = new Set<Topic>();

  constructor(private readonly deps: RegisterHubDeps) {}

  // ---- public API ----

  handle(topic: Topic): RegisterHandle {
    const policy = this.deps.topics.get(topic).policy;
    if (policy.kind !== "register") throw misuse(`register() on non-register topic ${topic}`);
    return {
      set: (key, value) => this.write(topic, key, "set", { value }),
      del: (key) => this.write(topic, key, "del", null),
      add: (key, member) => this.write(topic, key, "add", { member }, member),
      remove: (key, member) => this.write(topic, key, "remove", { member }, member),
      resolve: (key, supersedes, value) =>
        this.write(topic, key, "resolve", {
          supersedes: supersedes as unknown as JsonValue,
          value,
        }),
      resolveMember: (key, member, supersedes, present) =>
        this.write(
          topic,
          key,
          "resolve-member",
          { member, supersedes: supersedes as unknown as JsonValue, present },
          member,
        ),
      chown: (key, newOwner) => this.write(topic, key, "chown", { newOwner }),
      takeover: async (key, newOwner) => {
        const issue = this.deps.authority?.issueTakeover;
        if (!issue) throw misuse("takeover requires authority.issueTakeover");
        const { hostSig } = await issue({ topic, key, newOwner });
        return this.write(topic, key, "chown-takeover", { newOwner, hostSig });
      },
      request: (key, value) => this.write(topic, key, "request", { value }),
    };
  }

  // fires after a materialize pass rewrote the built-in register table —
  // subscription serving pushes SNAP resets from this (coarse but correct)
  onChange(cb: (topic: Topic) => void): Unsub {
    this.changeListeners.add(cb);
    return () => this.changeListeners.delete(cb);
  }

  tableRowsSorted(topic: Topic): Record<string, string | number | null>[] {
    return this.deps.store
      .raw()
      .all<Record<string, string | number | null>>(
        `SELECT * FROM "${this.tableName(topic)}" ORDER BY key`,
      );
  }

  onConflict(topic: Topic, cb: (c: Conflict) => void | Promise<void>): Unsub {
    const set = this.conflictCbs.get(topic) ?? new Set();
    set.add(cb);
    this.conflictCbs.set(topic, set);
    return () => set.delete(cb);
  }

  onOwnedRequest(topic: Topic, cb: (r: OwnedRequest) => Promise<"approve" | "deny">): Unsub {
    const set = this.requestCbs.get(topic) ?? new Set();
    set.add(cb);
    this.requestCbs.set(topic, set);
    return () => set.delete(cb);
  }

  ownerOf(topic: Topic, key: Key): WriterId | null {
    return this.topicState(topic).keys.get(key)?.owner ?? null;
  }

  pendingRequests(topic: Topic, now?: number): OwnedRequest[] {
    const cutoff = (now ?? this.deps.clock()) - this.deps.constants.REQUEST_TTL_MS;
    const out: OwnedRequest[] = [];
    for (const [key, ks] of this.topicState(topic).keys) {
      for (const r of ks.requests) {
        if (r.approvedBy) continue;
        if (r.at.l < cutoff) continue; // expiry is query-time derivation (§11.4)
        out.push({ topic, key, value: r.value, requester: r.requester, entry: r.entry, at: r.at });
      }
    }
    return out;
  }

  snapshotState(topic: Topic): RegisterSnapshotState {
    return this.snapshotOf(this.topicState(topic));
  }

  // register state exactly at `ord` — a fold bounded by the cut, starting from
  // the base floor when one exists (pre-base rows may be archived) (§7.7)
  async stateAt(topic: Topic, ord: Order): Promise<RegisterSnapshotState> {
    const live = this.topicState(topic);
    const st: TopicState = { keys: new Map(), ord: null, lastRowid: 0 };
    let cursor: Order | null = null;
    if (live.base && orderCompare(live.base.ord, ord) <= 0) {
      st.keys = this.cloneKeys(live.base.keys);
      st.ord = live.base.ord;
      cursor = live.base.ord;
    }
    outer: for (;;) {
      const entries = this.deps.store.entriesAfterOrder(topic, cursor, 500);
      if (entries.length === 0) break;
      for (const e of entries) {
        const o = orderOf(e);
        if (orderCompare(o, ord) > 0) break outer;
        await this.fold(topic, st, e, []);
        cursor = o;
      }
    }
    return this.snapshotOf(st);
  }

  installSnapshot(topic: Topic, snap: RegisterSnapshotState, ord: Order): void {
    const keys = this.keysFromSnapshot(snap);
    const st = this.topicState(topic);
    st.keys = keys;
    st.ord = ord;
    st.lastRowid = this.deps.store.maxRowid(topic);
    st.base = { keys: this.keysFromSnapshot(snap), ord };
    this.deps.store.metaSet(`regbase:${topic}`, JSON.stringify({ snap, ord }));
    this.rewriteTable(topic, st);
  }

  private keysFromSnapshot(snap: RegisterSnapshotState): Map<Key, KeyState> {
    const keys = new Map<Key, KeyState>();
    for (const [key, k] of Object.entries(snap.keys)) {
      const members = new Map<string, MemberState>();
      for (const [m, mm] of Object.entries(k.members ?? {})) {
        members.set(m, {
          present: mm.present,
          entry: mm.entry,
          order: mm.order,
          frontier: [...(mm.frontier ?? [mm.entry])],
          superseded: [...(mm.superseded ?? [])],
        });
      }
      const ks: KeyState = {
        scalar: members.size === 0,
        winner: k.winner,
        winnerOrder: k.winnerOrder,
        resetGen: k.resetGen,
        members,
        frontier: [...k.frontier],
        superseded: [...(k.superseded ?? [])],
        requests: [], // pre-cut requests are not carried (known v1 gap)
      };
      if (k.value !== undefined) ks.value = k.value;
      if (k.owner !== undefined) ks.owner = k.owner;
      keys.set(key, ks);
    }
    return keys;
  }

  private cloneKeys(keys: Map<Key, KeyState>): Map<Key, KeyState> {
    const out = new Map<Key, KeyState>();
    for (const [k, ks] of keys) {
      const members = new Map<string, MemberState>();
      for (const [m, ms] of ks.members)
        members.set(m, { ...ms, frontier: [...ms.frontier], superseded: [...ms.superseded] });
      out.set(k, {
        ...ks,
        members,
        frontier: [...ks.frontier],
        superseded: [...ks.superseded],
        requests: ks.requests.map((r) => ({ ...r })),
      });
    }
    return out;
  }

  private snapshotOf(st: TopicState): RegisterSnapshotState {
    const keys: RegisterSnapshotState["keys"] = {};
    for (const [key, ks] of st.keys) {
      if (!ks.winner || !ks.winnerOrder) continue;
      const k: RegisterSnapshotState["keys"][string] = {
        winner: ks.winner,
        winnerOrder: ks.winnerOrder,
        resetGen: ks.resetGen,
        frontier: [...ks.frontier],
      };
      if (ks.scalar && ks.value !== undefined) k.value = ks.value;
      if (ks.owner !== undefined) k.owner = ks.owner;
      if (ks.superseded.length > 0) k.superseded = [...ks.superseded];
      if (ks.members.size > 0) {
        k.members = {};
        for (const [m, ms] of ks.members) {
          const mm: NonNullable<typeof k.members>[string] = {
            present: ms.present,
            entry: ms.entry,
            order: ms.order,
          };
          if (ms.frontier.length > 0) mm.frontier = [...ms.frontier];
          if (ms.superseded.length > 0) mm.superseded = [...ms.superseded];
          k.members![m] = mm;
        }
      }
      keys[key] = k;
    }
    return { keys };
  }

  notifyApplied(topic: Topic): void {
    if (this.deps.topics.get(topic).policy.kind !== "register") return;
    if (this.scheduled.has(topic)) return;
    this.scheduled.add(topic);
    this.deps.timers.setTimeout(() => {
      this.scheduled.delete(topic);
      void this.materialize(topic);
    }, 0);
  }

  // ---- write path (§11.2 race-safe causal stamping) ----

  private write(
    topic: Topic,
    key: Key,
    kind: string,
    payload: JsonValue,
    member?: string,
  ): Promise<EntryId> {
    this.deps.topics.get(topic); // throws ERR_UNKNOWN_TOPIC
    const scope = this.scopeKey(topic, key, kind, member);
    const provider = (seq: number): [WriterId, number] | undefined => {
      const causal = this.scopeTails.get(scope) ?? this.materializedLastObserved(topic, key, member);
      this.scopeTails.set(scope, [this.deps.writerId, seq]);
      return causal;
    };
    return this.deps.core.append(topic, kind, payload, { key, causalProvider: provider });
  }

  private scopeKey(topic: Topic, key: Key, kind: string, member?: string): string {
    // member ops stamp at (key, member, resetGen) scope; key-level ops at key scope
    if (member !== undefined) {
      const gen = this.topicState(topic).keys.get(key)?.resetGen ?? 0;
      return `${topic}\u0000${key}\u0000m\u0000${member}\u0000${gen}`;
    }
    return `${topic}\u0000${key}`;
  }

  private materializedLastObserved(
    topic: Topic,
    key: Key,
    member?: string,
  ): [WriterId, number] | undefined {
    const ks = this.topicState(topic).keys.get(key);
    if (!ks) return undefined;
    if (member !== undefined) {
      const ms = ks.members.get(member);
      return ms ? [ms.entry[1], ms.entry[2]] : undefined;
    }
    return ks.winner ? [ks.winner[1], ks.winner[2]] : undefined;
  }

  // ---- fold ----

  private topicState(topic: Topic): TopicState {
    let st = this.states.get(topic);
    if (!st) {
      st = { keys: new Map(), ord: null, lastRowid: 0 };
      // restore the persisted base floor (survives restarts + archiving, §7.6)
      const raw = this.deps.store.metaGet(`regbase:${topic}`);
      if (raw) {
        const saved = JSON.parse(raw) as { snap: RegisterSnapshotState; ord: Order };
        st.base = { keys: this.keysFromSnapshot(saved.snap), ord: saved.ord };
        st.keys = this.keysFromSnapshot(saved.snap);
        st.ord = saved.ord;
      }
      this.states.set(topic, st);
    }
    return st;
  }

  // §7.6: the cut state becomes the permanent fold floor — refolds start here,
  // which keeps register materialization correct after pre-cut rows archive
  async rebaseAtCut(topic: Topic, ord: Order): Promise<void> {
    if (this.deps.topics.get(topic).policy.kind !== "register") return;
    const snap = await this.stateAt(topic, ord);
    const st = this.topicState(topic);
    st.base = { keys: this.keysFromSnapshot(snap), ord };
    this.deps.store.metaSet(`regbase:${topic}`, JSON.stringify({ snap, ord }));
  }

  private async materialize(topic: Topic): Promise<void> {
    if (this.materializing.has(topic)) {
      this.notifyApplied(topic); // rerun after the current pass
      return;
    }
    this.materializing.add(topic);
    try {
      const st = this.topicState(topic);
      const events: (() => void | Promise<void>)[] = [];
      for (;;) {
        const rows = this.deps.store.entriesForTopicFromRowid(topic, st.lastRowid, 500);
        if (rows.length === 0) break;
        const sorted = rows.map((r) => r.entry).sort((a, b) => orderCompare(orderOf(a), orderOf(b)));
        const lastRowid = rows[rows.length - 1]!.rowid;
        const first = sorted[0]!;
        if (st.ord !== null && orderCompare(orderOf(first), st.ord) <= 0) {
          // late arrival — refold in total order from the base floor (or empty)
          if (st.base) {
            st.keys = this.cloneKeys(st.base.keys);
            st.ord = st.base.ord;
          } else {
            st.keys.clear();
            st.ord = null;
          }
          this.surfacedConflicts.clear();
          let cursor: Order | null = st.ord;
          for (;;) {
            const batch = this.deps.store.entriesAfterOrder(topic, cursor, 500);
            if (batch.length === 0) break;
            for (const e of batch) await this.fold(topic, st, e, events);
            cursor = st.ord;
          }
          st.lastRowid = this.deps.store.maxRowid(topic);
          break;
        }
        for (const e of sorted) await this.fold(topic, st, e, events);
        st.lastRowid = lastRowid;
      }
      this.rewriteTable(topic, st);
      for (const fn of events) await fn();
      for (const cb of this.changeListeners) cb(topic);
    } finally {
      this.materializing.delete(topic);
    }
  }

  private async fold(
    topic: Topic,
    st: TopicState,
    e: LogEntry,
    events: (() => void | Promise<void>)[],
  ): Promise<void> {
    st.ord = orderOf(e);
    if (e.writer === this.deps.writerId) {
      // §11.2: the "own uncommitted tail" shrinks as own writes materialize —
      // once folded, causal stamping falls back to the materialized winner.
      // Own entries appear in seq order within the total order (hlc monotonic),
      // so every own tail at seq ≤ e.seq is now materialized.
      for (const [scope, tail] of this.scopeTails) {
        if (tail[0] === this.deps.writerId && tail[1] <= e.seq && scope.startsWith(`${topic}\u0000`))
          this.scopeTails.delete(scope);
      }
    }
    if (e.key === undefined) return; // not a register op
    const key = e.key;
    let ks = st.keys.get(key);
    if (!ks) {
      ks = {
        scalar: true,
        resetGen: 0,
        members: new Map(),
        frontier: [],
        superseded: [],
        requests: [],
      };
      st.keys.set(key, ks);
    }
    const policy = conflictPolicyFor(this.deps.topics.get(topic).policy, key);
    const id: EntryId = [topic, e.writer, e.seq];
    const payload = (e.payload ?? {}) as Record<string, JsonValue>;

    switch (e.kind) {
      case "request": {
        const req: RequestState = {
          entry: id,
          requester: e.writer,
          value: payload.value ?? null,
          at: e.hlc,
        };
        ks.requests.push(req);
        events.push(() => this.maybeAskOwner(topic, key, ks!, req));
        return;
      }
      case "chown": {
        if (ks.owner !== undefined && ks.owner !== e.writer) {
          this.violation(e);
          return;
        }
        ks.owner = String(payload.newOwner ?? "");
        return;
      }
      case "chown-takeover": {
        if (!(await this.verifyTakeover(e))) {
          this.deps.emitAnomaly({ kind: "takeover_invalid", entry: e });
          return;
        }
        ks.owner = String(payload.newOwner ?? "");
        return;
      }
      case "set":
      case "del": {
        if (policy === "owned" && !this.ownedWriteAllowed(ks, e)) return;
        // §11.5 mirror rule: set on a member key replaces the whole member map
        if (e.kind === "set" && !ks.scalar) {
          ks.members.clear();
          ks.resetGen++;
          ks.scalar = true;
        }
        if (e.kind === "del") {
          // effective del bumps resetGen and clears members (reset epoch)
          ks.members.clear();
          ks.resetGen++;
          ks.scalar = true;
        }
        this.foldKeyWrite(topic, ks, e, id, policy, e.kind === "set" ? payload.value : undefined, events);
        if (e.kind === "set" && e.ref) this.markApproved(ks, e.ref, id);
        if (policy === "owned" && ks.owner === undefined) ks.owner = e.writer;
        return;
      }
      case "resolve": {
        const supersedes = (payload.supersedes ?? []) as unknown as EntryId[];
        this.applySupersedes(ks.frontier, ks.superseded, supersedes);
        this.foldKeyWrite(topic, ks, e, id, policy, payload.value, events);
        return;
      }
      case "add":
      case "remove": {
        if (policy === "owned" && !this.ownedWriteAllowed(ks, e)) return;
        // mirror rule: a member op on a scalar-valued key clears the scalar
        if (ks.scalar && ks.value !== undefined) {
          ks.value = undefined;
          ks.resetGen++;
        }
        ks.scalar = false;
        this.foldMemberOp(topic, ks, e, id, policy, String(payload.member ?? ""), e.kind === "add", events);
        if (policy === "owned" && ks.owner === undefined) ks.owner = e.writer;
        return;
      }
      case "resolve-member": {
        const member = String(payload.member ?? "");
        const ms = ks.members.get(member);
        const supersedes = (payload.supersedes ?? []) as unknown as EntryId[];
        if (ms) this.applySupersedes(ms.frontier, ms.superseded, supersedes);
        this.foldMemberOp(topic, ks, e, id, policy, member, Boolean(payload.present), events);
        return;
      }
      default:
        return; // unknown kinds on register topics are inert
    }
  }

  private foldKeyWrite(
    topic: Topic,
    ks: KeyState,
    e: LogEntry,
    id: EntryId,
    policy: string,
    value: JsonValue | undefined,
    events: (() => void | Promise<void>)[],
  ): void {
    const ord = orderOf(e);
    const prevWinner = ks.winner;
    let adopt: boolean;
    if (ks.winner === undefined) adopt = true;
    else if (policy === "fww") {
      // adopt iff causal successor of the current adoptee; keep earlier when concurrent
      adopt = this.isAncestor(topic, ks.winner, e);
    } else {
      // lww / resolver / owned: last in total order
      adopt = orderCompare(ord, ks.winnerOrder!) > 0;
    }
    // frontier maintenance: e supersedes its causal ancestors among the heads
    ks.frontier = ks.frontier.filter((h) => !this.isAncestor(topic, h, e) && !this.sameId(h, id));
    ks.frontier.push(id);
    if (adopt) {
      ks.winner = id;
      ks.winnerOrder = ord;
      if (e.kind === "del") ks.value = undefined;
      else if (value !== undefined) ks.value = value;
    }
    if (
      prevWinner !== undefined &&
      ks.frontier.length > 1 &&
      !this.isAncestor(topic, prevWinner, e)
    ) {
      events.push(() => this.surfaceConflict(topic, e.key!, ks, undefined));
    }
  }

  private foldMemberOp(
    topic: Topic,
    ks: KeyState,
    e: LogEntry,
    id: EntryId,
    policy: string,
    member: string,
    present: boolean,
    events: (() => void | Promise<void>)[],
  ): void {
    const ord = orderOf(e);
    // the key-level winner tracks the latest effective op on the key so that
    // member-only keys still carry winner/winnerOrder in snapshot state (§11.6)
    if (ks.winnerOrder === undefined || orderCompare(ord, ks.winnerOrder) > 0) {
      ks.winner = id;
      ks.winnerOrder = ord;
    }
    let ms = ks.members.get(member);
    if (!ms) {
      ms = { present, entry: id, order: ord, frontier: [id], superseded: [] };
      ks.members.set(member, ms);
      return;
    }
    const prev = ms.entry;
    let adopt: boolean;
    if (policy === "fww") adopt = this.isAncestor(topic, ms.entry, e);
    else adopt = orderCompare(ord, ms.order) > 0;
    ms.frontier = ms.frontier.filter((h) => !this.isAncestor(topic, h, e) && !this.sameId(h, id));
    ms.frontier.push(id);
    if (adopt) {
      ms.present = present;
      ms.entry = id;
      ms.order = ord;
    }
    if (ms.frontier.length > 1 && !this.isAncestor(topic, prev, e)) {
      events.push(() => this.surfaceConflict(topic, e.key!, ks, member));
    }
  }

  private applySupersedes(frontier: EntryId[], superseded: EntryId[], ids: EntryId[]): void {
    for (const s of ids) {
      const idx = frontier.findIndex((h) => this.sameId(h, s));
      if (idx >= 0) {
        superseded.push(frontier[idx]!);
        frontier.splice(idx, 1);
      }
    }
  }

  private ownedWriteAllowed(ks: KeyState, e: LogEntry): boolean {
    if (ks.owner === undefined || ks.owner === e.writer) return true;
    this.violation(e);
    return false;
  }

  private violation(e: LogEntry): void {
    this.deps.store.annotate(
      e.topic,
      e.writer,
      e.seq,
      "owned_violation",
      new Date(this.deps.clock()).toISOString(),
    );
    this.deps.emitAnomaly({ kind: "owned_violation", entry: e });
  }

  private markApproved(ks: KeyState, ref: EntryId, approver: EntryId): void {
    const req = ks.requests.find((r) => this.sameId(r.entry, ref));
    if (req) req.approvedBy = approver;
  }

  private async verifyTakeover(e: LogEntry): Promise<boolean> {
    const k = `${e.topic}\u0000${e.writer}\u0000${e.seq}`;
    const cached = this.takeoverVerdicts.get(k);
    if (cached !== undefined) return cached;
    const verify = this.deps.authority?.verifyTakeover;
    const verdict = verify ? Boolean(await verify(e)) : false;
    this.takeoverVerdicts.set(k, verdict);
    return verdict;
  }

  // causal-ancestor reachability (§11.3): walk b's causal chain looking for a
  private isAncestor(topic: Topic, a: EntryId, b: LogEntry | EntryId): boolean {
    let cur: LogEntry | undefined;
    if (Array.isArray(b)) cur = this.deps.store.getEntry(topic, b[1], b[2]);
    else cur = b;
    for (let hops = 0; hops < ANCESTOR_WALK_CAP && cur; hops++) {
      const c = cur.causal;
      if (!c) return false;
      if (c[0] === a[1] && c[1] === a[2]) return true;
      cur = this.deps.store.getEntry(topic, c[0], c[1]);
    }
    return false;
  }

  private sameId(a: EntryId, b: EntryId): boolean {
    return a[0] === b[0] && a[1] === b[1] && a[2] === b[2];
  }

  // ---- conflict & request surfacing ----

  private async surfaceConflict(
    topic: Topic,
    key: Key,
    ks: KeyState,
    member: string | undefined,
  ): Promise<void> {
    const frontier = member === undefined ? ks.frontier : (ks.members.get(member)?.frontier ?? []);
    if (frontier.length < 2) return; // already resolved by a later fold step
    const sig = `${topic}\u0000${key}\u0000${member ?? ""}\u0000${jcs(frontier as unknown as JsonValue)}`;
    if (this.surfacedConflicts.has(sig)) return;
    this.surfacedConflicts.add(sig);

    const entries = frontier
      .map((h) => this.deps.store.getEntry(topic, h[1], h[2]))
      .filter((x): x is LogEntry => x !== undefined);
    const winnerId = member === undefined ? ks.winner : ks.members.get(member)?.entry;
    const winner = winnerId
      ? this.deps.store.getEntry(topic, winnerId[1], winnerId[2])
      : undefined;
    if (!winner) return;

    const reg = this.handle(topic);
    const base = { topic, key, entries, concurrent: true as const, provisionalWinner: winner };
    const conflict: Conflict =
      member === undefined
        ? {
            ...base,
            resolve: (value: JsonValue) => reg.resolve(key, [...frontier], value),
          }
        : {
            ...base,
            member,
            resolveMember: (present: boolean) => reg.resolveMember(key, member, [...frontier], present),
          };
    for (const cb of this.conflictCbs.get(topic) ?? []) await cb(conflict);
  }

  private async maybeAskOwner(topic: Topic, key: Key, ks: KeyState, req: RequestState): Promise<void> {
    if (ks.owner !== this.deps.writerId) return; // approval runs on the owner node
    const sig = `${topic}\u0000${jcs(req.entry as unknown as JsonValue)}`;
    if (this.handledRequests.has(sig)) return;
    this.handledRequests.add(sig);
    const reg = this.handle(topic);
    for (const cb of this.requestCbs.get(topic) ?? []) {
      const verdict = await cb({
        topic,
        key,
        value: req.value,
        requester: req.requester,
        entry: req.entry,
        at: req.at,
      });
      if (verdict === "approve") {
        await this.deps.core.append(topic, "set", { value: req.value }, {
          key,
          ref: req.entry,
          causalProvider: (seq) => {
            const scope = `${topic}\u0000${key}`;
            const causal = this.scopeTails.get(scope) ?? this.materializedLastObserved(topic, key);
            this.scopeTails.set(scope, [this.deps.writerId, seq]);
            return causal;
          },
        });
        return;
      }
    }
  }

  // ---- built-in register table (§9) ----

  private rewriteTable(topic: Topic, st: TopicState): void {
    const table = `sqv_reg_${topic.replace(/[^a-z0-9_]/g, "_")}_${sha256HexUtf8(topic).slice(0, 8)}`;
    const raw = this.deps.store.raw();
    raw.run(
      `CREATE TABLE IF NOT EXISTS "${table}" (key TEXT PRIMARY KEY, value TEXT, owner TEXT,
        reset_gen INTEGER NOT NULL, members TEXT, conflicted INTEGER NOT NULL)`,
    );
    this.deps.store.transaction(() => {
      raw.run(`DELETE FROM "${table}"`);
      for (const [key, ks] of st.keys) {
        const hasScalar = ks.scalar && ks.value !== undefined;
        const memberMap: Record<string, boolean> = {};
        let hasMembers = false;
        for (const [m, ms] of ks.members) {
          if (ms.present) {
            memberMap[m] = true;
            hasMembers = true;
          }
        }
        if (!hasScalar && !hasMembers && ks.requests.length === 0) continue; // deleted key
        const conflicted =
          ks.frontier.length > 1 ||
          [...ks.members.values()].some((m) => m.frontier.length > 1);
        raw.run(`INSERT INTO "${table}" (key, value, owner, reset_gen, members, conflicted)
                 VALUES (?, ?, ?, ?, ?, ?)`, [
          key,
          hasScalar ? JSON.stringify(ks.value) : null,
          ks.owner ?? null,
          ks.resetGen,
          hasMembers ? JSON.stringify(memberMap) : null,
          conflicted ? 1 : 0,
        ]);
      }
    });
  }

  tableName(topic: Topic): string {
    return `sqv_reg_${topic.replace(/[^a-z0-9_]/g, "_")}_${sha256HexUtf8(topic).slice(0, 8)}`;
  }

  async settle(topic: Topic): Promise<void> {
    await this.materialize(topic);
  }
}

export function assertRegisterAppendGuard(kind: string): void {
  void kind;
  throw new SeqscribeError("ERR_MISUSE", "raw append on register topic");
}
