// Acknowledged retention for full-sync append topics (host-guide §4.8) — a
// local extension, like pruneTopic/pruneSuperseded, with one version-gated
// wire addition (TRUNCATED, proto ≥ 3).
//
// A full-sync topic's rows are cross-peer canonical: deleting them locally
// leaves any peer that still needs them with a permanent gap (its next entry
// from that writer lands out of order and sits in sq_pending forever). So the
// deletion floor is bounded by what every relevant peer has ACKNOWLEDGED
// holding, and a peer that is below the floor anyway (new, or silent past the
// max-lag policy) is given a way over it instead of a gap:
//
//   · ACKS — every completed HAVE round from a mutual-full peer records, per
//     (topic, peer node, writer), the contig that peer advertised. HAVE is the
//     peer's own statement of its committed stream heads (the same authority
//     §6.2 already gives it), so no new wire message is needed and a pre-§4.8
//     peer acknowledges exactly like a new one. Hosts may add acks learned
//     out-of-band (e.g. Beacon reports) with noteAcks().
//   · FLOOR — per stream, "rows with seq ≤ floor are not held here". Written
//     by pruneAcked (after deleting the prefix) and by floor adoption.
//   · TRUNCATED — a WANT at or below the responder's floor is answered with
//     {floorSeq, floorChain}; the requester advances its contig to the floor
//     (chains continue from floorChain, §4.1 trust model: a full-sync peer is
//     already trusted with the content itself) and resumes WANTs above it.
//
// This module owns the ack recorder, the floor planner and the per-topic
// retention counters. The prune/adopt mutations run inside LogCore's append
// queue (log.ts processPruneAcked / processAdoptFloor) so they are ordered
// against appends and wire applies exactly like pruneTopic.

import type { Store } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type { HaveVectors, Seq, Topic, WriterId } from "./types.js";

// pruneAcked's per-call row bound when maxRows is omitted, and the clamp for
// an explicit one — one call is one flush transaction's worth of DELETEs.
// Local operation bounds, like PRUNE_SUPERSEDED_*, so not §16 Constants.
export const ACKED_PRUNE_DEFAULT_MAX_ROWS = 1_000;
export const ACKED_PRUNE_MAX_ROWS_CAP = 10_000;
// An observed node's seen_at is rewritten at most this often when nothing
// else about its acks changed — HAVE rounds run every ANTI_ENTROPY_MS, and a
// liveness stamp finer than a minute buys nothing against day-scale max-lag.
export const ACK_SEEN_REFRESH_MS = 60_000;
// Ack rows of a node silent for this many max-lag periods are forgotten by
// pruneAcked (bounded: a handful of rows per node). A node that returns is
// simply re-recorded by its next HAVE.
export const ACK_FORGET_LAG_MULTIPLE = 2;

export interface AckedPruneOptions {
  // Only rows whose author stamp hlc.l is older than now − olderThanMs are
  // deleted — the retention window the topic's consumers rely on.
  olderThanMs: number;
  // A member whose newest evidence (its last recorded HAVE, its first naming
  // for a host-listed member, or its own newest entry here when it is a
  // writer of the topic) is older than this stops pinning the floor. It is
  // then pruned past and recovers with TRUNCATED when it returns.
  maxLagMs: number;
  // Explicit membership. Default: every node that has acknowledged this
  // topic here plus every non-retired writer of the topic.
  members?: WriterId[];
  // Removed nodes — never pin, whatever their evidence.
  excludeMembers?: WriterId[];
  maxRows?: number; // default ACKED_PRUNE_DEFAULT_MAX_ROWS, clamped to ACKED_PRUNE_MAX_ROWS_CAP
  dryRun?: boolean; // plan only: report what would go, delete nothing, persist nothing
}

export interface AckedPruneWriter {
  floor: Seq; // this node's retention floor for the stream after the call
  ackFloor: Seq; // min acknowledged contig over the included members
  pinnedBy: WriterId | null; // the member holding ackFloor down (null: no members / not pinned by acks)
  skipped?: "forked" | "recovering";
}

export interface AckedPruneMember {
  node: WriterId;
  lastSeenAt: number | null;
  excluded: null | "lagging" | "excluded";
}

export interface AckedPruneResult {
  prunedRows: number; // deleted (or, with dryRun, deletable within the budget)
  more: boolean; // the row budget ran out with deletable rows left — call again
  writers: Record<WriterId, AckedPruneWriter>;
  members: AckedPruneMember[];
}

export interface TopicRetentionCounters {
  prunedRows: number; // rows deleted by pruneAcked
  floorsAdopted: number; // streams advanced past a peer's floor (TRUNCATED / import)
  truncatedServed: number; // WANTs answered with TRUNCATED
  truncatedUnservable: number; // WANTs below the floor from a proto < 3 peer
}

// Cumulative per-topic counters, shared by LogCore (prune/adopt) and the sync
// engine (TRUNCATED service). Fixed scalar keys; no content.
export class RetentionStats {
  private readonly byTopic = new Map<Topic, TopicRetentionCounters>();

  bump(topic: Topic, key: keyof TopicRetentionCounters, n = 1): void {
    let c = this.byTopic.get(topic);
    if (!c) {
      c = { prunedRows: 0, floorsAdopted: 0, truncatedServed: 0, truncatedUnservable: 0 };
      this.byTopic.set(topic, c);
    }
    c[key] += n;
  }

  get(topic: Topic): TopicRetentionCounters | undefined {
    const c = this.byTopic.get(topic);
    return c ? { ...c } : undefined;
  }
}

// A topic acknowledged retention applies to: an append log that is
// full-sync replicated with full retention and is not keyed. Register topics
// replay from genesis (§11); keyed topics' state is newest-per-key over the
// whole topic (§4.7) — neither survives losing a prefix.
export function ackedRetentionRefusal(topics: TopicRegistry, topic: Topic): string | null {
  const p = topics.get(topic).policy;
  if (p.kind !== "append") return `register-kind topics are not prunable (${topic})`;
  if (p.replication !== "full-sync") return `requires a full-sync topic (${topic}) — use pruneTopic`;
  if (p.retention.mode !== "full") return `requires retention "full" (${topic} is ${p.retention.mode})`;
  if (p.keyed !== undefined) return `keyed topics are compacted by pruneSuperseded (${topic})`;
  return null;
}

// Records peers' acknowledgments. Called by the sync engine once per
// completed HAVE round (and by the host through noteAcks). Writes only what
// changed — an unchanged ack is a Map hit, an unchanged liveness stamp is
// rewritten at most every ACK_SEEN_REFRESH_MS — inside one transaction.
export class AckRecorder {
  private readonly last = new Map<string, Seq>(); // `${topic}\0${node}\0${writer}` → recorded seq
  private readonly seenWritten = new Map<string, number>(); // `${topic}\0${node}` → last seen_at written

  constructor(
    private readonly store: Store,
    private readonly topics: TopicRegistry,
    private readonly selfWriter: WriterId,
  ) {}

  record(node: WriterId, vectors: HaveVectors, at: number, proto: number | null): void {
    if (node === this.selfWriter) return;
    const writes: (() => void)[] = [];
    for (const [topic, v] of Object.entries(vectors)) {
      if (!this.topics.has(topic) || ackedRetentionRefusal(this.topics, topic) !== null) continue;
      let changed = false;
      for (const [writer, w] of Object.entries(v.writers)) {
        const seq = "retired" in w ? w.finalSeq : w.contig;
        const k = `${topic}\u0000${node}\u0000${writer}`;
        if (this.last.get(k) === seq) continue;
        this.last.set(k, seq);
        changed = true;
        writes.push(() => this.store.ackUpsert(topic, node, writer, seq));
      }
      const nk = `${topic}\u0000${node}`;
      const prev = this.seenWritten.get(nk);
      if (changed || prev === undefined || at - prev >= ACK_SEEN_REFRESH_MS) {
        this.seenWritten.set(nk, at);
        writes.push(() => this.store.ackNodeSeen(topic, node, at, proto));
      }
    }
    if (writes.length === 0) return;
    try {
      this.store.transaction(() => {
        for (const w of writes) w();
      });
    } catch {
      // advisory bookkeeping: a failed write must never break a HAVE round —
      // drop the memo so the next round rewrites what did not land
      this.last.clear();
      this.seenWritten.clear();
    }
  }

  // pruneAcked may have forgotten a node — its memo must not suppress the re-record.
  forget(topic: Topic, node: WriterId): void {
    this.seenWritten.delete(`${topic}\u0000${node}`);
    const prefix = `${topic}\u0000${node}\u0000`;
    for (const k of [...this.last.keys()]) if (k.startsWith(prefix)) this.last.delete(k);
  }
}

// ---- floor planning (pure over the store; runs inside the flush) ----

export interface StreamView {
  writer: WriterId;
  contigSeq: Seq;
  sealReason: "fork" | "retired" | null;
  finalSeq: Seq | null;
  recovering: boolean;
}

export interface MemberPlan {
  members: AckedPruneMember[];
  included: WriterId[];
  forget: WriterId[]; // recorded nodes silent past ACK_FORGET_LAG_MULTIPLE × maxLag
}

export function planMembers(
  store: Store,
  topic: Topic,
  o: AckedPruneOptions,
  streams: StreamView[],
  selfWriter: WriterId,
  now: number,
): MemberPlan {
  const recorded = new Map(store.ackNodes(topic).map((r) => [r.node, r]));
  const excluded = new Set(o.excludeMembers ?? []);
  const writerNewest = new Map<WriterId, number>();
  for (const s of streams) {
    if (s.contigSeq === 0) continue;
    const l = store.hlcLAt(topic, s.writer, s.contigSeq);
    if (l !== undefined) writerNewest.set(s.writer, l);
  }
  const names = new Set<WriterId>();
  if (o.members !== undefined) {
    for (const n of o.members) names.add(n);
  } else {
    for (const n of recorded.keys()) names.add(n);
    for (const s of streams) if (s.sealReason !== "retired") names.add(s.writer);
  }
  names.delete(selfWriter);

  const members: AckedPruneMember[] = [];
  const included: WriterId[] = [];
  const forget: WriterId[] = [];
  for (const node of [...names].sort()) {
    const rec = recorded.get(node);
    let firstAt = rec?.firstAt;
    if (firstAt === undefined && o.members !== undefined) {
      // host-named but never observed: its max-lag clock starts now
      if (!o.dryRun) store.ackNodeName(topic, node, now);
      firstAt = now;
    }
    let evidence: number | null = null;
    for (const t of [rec?.seenAt ?? null, firstAt ?? null, writerNewest.get(node) ?? null])
      if (t !== null && (evidence === null || t > evidence)) evidence = t;
    let why: AckedPruneMember["excluded"] = null;
    if (excluded.has(node)) why = "excluded";
    else if (evidence === null || now - evidence > o.maxLagMs) why = "lagging";
    members.push({ node, lastSeenAt: rec?.seenAt ?? null, excluded: why });
    if (why === null) included.push(node);
    if (
      rec !== undefined &&
      (why === "excluded" ||
        (evidence !== null && now - evidence > o.maxLagMs * ACK_FORGET_LAG_MULTIPLE))
    )
      forget.push(node);
  }
  return { members, included, forget };
}

// Per-stream acknowledged floor: the minimum contig any included member has
// advertised for the stream. A member that never acknowledged the stream
// counts as 0 — except the stream's own author, which holds what it wrote.
export function ackFloors(
  store: Store,
  topic: Topic,
  streams: StreamView[],
  included: WriterId[],
): Map<WriterId, { ackFloor: Seq; pinnedBy: WriterId | null }> {
  const acks = new Map<string, Seq>();
  for (const a of store.acksForTopic(topic)) acks.set(`${a.node}\u0000${a.writer}`, a.seq);
  const out = new Map<WriterId, { ackFloor: Seq; pinnedBy: WriterId | null }>();
  for (const s of streams) {
    const cap = s.sealReason === "retired" ? (s.finalSeq ?? s.contigSeq) : s.contigSeq;
    let floor = cap;
    let pinnedBy: WriterId | null = null;
    for (const node of included) {
      const a = acks.get(`${node}\u0000${s.writer}`) ?? (node === s.writer ? cap : 0);
      if (a < floor) {
        floor = a;
        pinnedBy = node;
      }
    }
    out.set(s.writer, { ackFloor: floor, pinnedBy });
  }
  return out;
}
