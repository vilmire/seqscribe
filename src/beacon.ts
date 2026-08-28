// SPEC §5.7 (DESIGN), §14 — beacon client: debounced content-free vector
// reports plus node-side staleness prediction from known vectors.

import { sha256HexUtf8 } from "./encoding.js";
import { misuse } from "./errors.js";
import type { LogCore } from "./log.js";
import type { RegisterHub } from "./register.js";
import type { TopicRegistry } from "./topics.js";
import type {
  BeaconHandle,
  BeaconReport,
  BeaconTransport,
  Constants,
  Key,
  Seq,
  Staleness,
  Timers,
  Topic,
  WriterId,
} from "./types.js";

// §5.7 pre-write warning (proposals-v3.5 P27). `hints` advertises, per key, the
// latest change this node knows about — the reader side (staleness(topic, key))
// has always existed, but nothing ever produced the field, so the feature could
// not fire. Two supply paths, not mutually exclusive: the host's callback (any
// topic, any key shape) and the library's own register fold (opt-in per topic
// via TopicPolicy.hintKeys).
export type HintMap = Record<Topic, Record<string, [WriterId, Seq]>>;
export interface BeaconStartOpts {
  hints?: () => HintMap;
}

export interface BeaconHubDeps {
  core: LogCore;
  topics: TopicRegistry;
  writerId: WriterId;
  constants: Constants;
  timers: Timers;
  clock: () => number;
  registers?: RegisterHub | undefined;
}

export class BeaconHub {
  private known: BeaconReport[] = [];
  private transport: BeaconTransport | null = null;
  private debounceTimer: unknown = null;
  // Node-level teardown (proposals-v3.5 P28). Terminal and distinct from an
  // arming's stop(): a BeaconHub that outlived its owning node's close() must
  // never push again, so this is the one latch that is genuinely one-way.
  private closed = false;
  private hostHints: (() => HintMap) | null = null;
  // Monotonic arming id. stop() is a PAUSE — the hub is re-startable, matching
  // the shape of start() itself (a handle you can stop() reads as something you
  // can start() again) and the reconnect-scoped re-arm hosts actually write.
  // Handles are per-arming: a stale handle's stop() must not tear down a LATER
  // arming, which is why stop() compares its own generation before acting.
  private armGen = 0;

  constructor(private readonly deps: BeaconHubDeps) {}

  start(t: BeaconTransport, o?: BeaconStartOpts): BeaconHandle {
    if (this.closed) throw misuse("beacon closed — the node is closed");
    if (this.transport) throw misuse("beacon already started");
    const gen = ++this.armGen;
    this.transport = t;
    this.hostHints = o?.hints ?? null;
    this.push(); // initial report; GET piggybacks on each push
    return {
      stop: () => {
        if (this.armGen !== gen) return; // stale handle — a later arming owns the hub
        this.armGen++; // retire this arming: its in-flight rounds stop counting
        if (this.debounceTimer !== null) {
          this.deps.timers.clearTimeout(this.debounceTimer);
          this.debounceTimer = null;
        }
        this.transport = null;
        this.hostHints = null;
      },
    };
  }

  // §14 close quiescence: a started beacon's debounce timer reads
  // core.vectors() when it fires, so node.close() must cancel it like every
  // other hub timer rather than rely on the host calling the handle's stop().
  close(): void {
    this.closed = true;
    this.armGen++; // invalidate every outstanding handle
    if (this.debounceTimer !== null) {
      this.deps.timers.clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    this.transport = null;
    this.hostHints = null;
  }

  // called from the applied fan-out — 5 s debounce after append (§5.7)
  notifyApplied(): void {
    if (!this.transport || this.closed || this.debounceTimer !== null) return;
    this.debounceTimer = this.deps.timers.setTimeout(() => {
      this.debounceTimer = null;
      this.push();
    }, this.deps.constants.BEACON_DEBOUNCE_MS);
  }

  setKnownVectors(v: BeaconReport[]): void {
    this.known = v;
  }

  // wake-up lag & pre-write warning source (§5.7): how far each known peer's
  // vector is ahead of the local replica
  staleness(topic: Topic, key?: Key): Staleness {
    this.deps.topics.get(topic);
    const behind: Record<WriterId, number> = {};
    let latestKnown: [Topic, WriterId, number] | null = null;
    for (const report of this.known) {
      const vec = report.vectors[topic];
      if (!vec) continue;
      for (const [writer, w] of Object.entries(vec.writers)) {
        const theirSeq = "retired" in w ? w.finalSeq : w.contig;
        const mine = this.deps.core.getStream(topic, writer).contigSeq;
        const lag = Math.max(0, theirSeq - mine);
        if (lag > (behind[writer] ?? 0)) behind[writer] = lag;
      }
      if (key !== undefined) {
        const hint = report.hints?.[topic]?.[key];
        if (hint && (latestKnown === null || hint[1] > latestKnown[2]))
          latestKnown = [topic, hint[0], hint[1]];
      }
    }
    const out: Staleness = { behind, asOf: new Date(this.deps.clock()).toISOString() };
    if (key !== undefined && latestKnown) {
      const [t, w, s] = latestKnown;
      out.keyStale = {
        latestKnown: [t, w, s],
        haveLocally: this.deps.core.getStream(t, w).contigSeq >= s,
      };
    }
    return out;
  }

  // §5.7 hints (P27). `hintKeys` is the per-topic opt-in gate: a topic that has
  // not set it contributes nothing and, if the host names it anyway, is dropped
  // — the SPEC wire format implies the gate, and honoring it is what stops a
  // "hash" declaration from leaking plaintext keys by omission.
  //   "plain" — the key as authored
  //   "hash"  — sha256 hex of the key, so a board operator sees change activity
  //             without learning key names
  // Host-supplied entries are merged OVER library-derived ones for the same
  // topic: the host asked for a specific advertisement, so it wins.
  private buildHints(): HintMap | undefined {
    const out: HintMap = {};
    // The opt-in gate, resolved BEFORE any hint is computed: a topic that has
    // not set hintKeys must cost nothing on the push path (P27 item 3), so an
    // opted-out register topic is never walked at all.
    const modeOf = (topic: Topic): "plain" | "hash" | undefined => {
      try {
        return this.deps.topics.get(topic).policy.hintKeys;
      } catch {
        return undefined; // a host naming an undefined topic contributes nothing
      }
    };
    const add = (topic: Topic, mode: "plain" | "hash", raw: Record<string, [WriterId, Seq]>): void => {
      const dst = (out[topic] ??= {});
      for (const [key, at] of Object.entries(raw))
        dst[mode === "hash" ? sha256HexUtf8(key) : key] = at;
    };

    const registers = this.deps.registers;
    if (registers)
      for (const topic of this.deps.topics.list()) {
        const mode = modeOf(topic);
        if (mode === undefined) continue; // not opted in — never computed
        if (this.deps.topics.get(topic).policy.kind !== "register") continue;
        add(topic, mode, registers.hintsFor(topic));
      }

    if (this.hostHints) {
      let supplied: HintMap;
      try {
        supplied = this.hostHints();
      } catch {
        supplied = {}; // a throwing host callback must not break the report
      }
      for (const [topic, raw] of Object.entries(supplied)) {
        const mode = modeOf(topic);
        if (mode === undefined) continue; // gate applies to the host path too
        add(topic, mode, raw); // incl. hashing — a host cannot bypass "hash"
      }
    }

    // An empty map stays ABSENT, not `{}` — a node that opts into nothing emits
    // byte-identical reports to pre-P27 (item 3).
    for (const topic of Object.keys(out)) if (Object.keys(out[topic]!).length === 0) delete out[topic];
    return Object.keys(out).length > 0 ? out : undefined;
  }

  private push(): void {
    const t = this.transport;
    if (!t || this.closed) return;
    const gen = this.armGen;
    const report: BeaconReport = {
      node: this.deps.writerId,
      at: new Date(this.deps.clock()).toISOString(),
      vectors: this.deps.core.vectors(),
    };
    const hints = this.buildHints();
    if (hints) report.hints = hints;
    void t
      .put(report)
      .then(() => t.get())
      .then((reports) => {
        // Adopt only if this hub is still on the arming that issued the round:
        // a GET answered after a stop (or after a re-arm onto a different
        // transport) describes a board this hub is no longer reporting to.
        if (!this.closed && this.armGen === gen) this.known = reports;
      })
      .catch(() => {
        // beacon is best-effort — sync is unaffected without it (§5.7)
      });
  }
}

// Reference HTTP transport for the §14 beacon wire:
//   POST /v1/a/{account}/vectors   (body: BeaconReport, bearer auth)
//   GET  /v1/a/{account}/vectors → BeaconReport[]
export function httpBeaconTransport(
  baseUrl: string,
  account: string,
  token?: string,
): BeaconTransport {
  const url = `${baseUrl.replace(/\/$/, "")}/v1/a/${encodeURIComponent(account)}/vectors`;
  const f = (globalThis as unknown as {
    fetch: (u: string, init?: object) => Promise<{ ok: boolean; status: number; json(): Promise<unknown> }>;
  }).fetch;
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (token !== undefined) headers.authorization = `Bearer ${token}`;
  return {
    async put(body: BeaconReport): Promise<void> {
      const res = await f(url, { method: "POST", headers, body: JSON.stringify(body) });
      if (!res.ok) throw new Error(`beacon PUT failed: ${res.status}`);
    },
    async get(): Promise<BeaconReport[]> {
      const res = await f(url, { method: "GET", headers });
      if (!res.ok) throw new Error(`beacon GET failed: ${res.status}`);
      return (await res.json()) as BeaconReport[];
    },
  };
}

// Reference beacon as a fetch handler (§14 wire) — deployable on Cloudflare
// Workers/Durable Objects or any fetch-shaped runtime. State lives in the
// closure: run it inside a Durable Object (or a single instance) so the board
// is actually shared; a stateless multi-isolate Worker would shard it.
export interface FetchRequestLike {
  method: string;
  url: string;
  headers: { get(name: string): string | null };
  json(): Promise<unknown>;
}

// Optional board persistence (proposals-v3.5 P26). A Durable Object is the host
// the comment above recommends, but a DO is not a persistent process: an idle
// one hibernates and its isolate is evicted, so the closure `board` below resets
// to empty on the next wake — every vector every node ever PUT, gone silently.
//
// Granularity is PER-ACCOUNT, matching the existing `board.get(account)`
// granularity: an account's report set is one BeaconReport per active node, and
// a POST already overwrites a whole node entry ("one report per node"), so a
// per-account read-modify-write mirrors the in-memory semantics exactly and
// needs no merge logic. Finer (per-node) rows would buy nothing here and would
// add write coordination; a single whole-board blob would couple accounts.
// A host whose storage has a per-value size limit (e.g. a DO's 128 KiB) shards
// inside its own adapter — that is a storage detail, not a contract change.
export interface BeaconBoardStore {
  load(account: string): Promise<Record<string, BeaconReport> | null>; // node → report
  save(account: string, board: Record<string, BeaconReport>): Promise<void>;
}

export function beaconFetchHandler(o?: {
  token?: string;
  store?: BeaconBoardStore;
}): (req: FetchRequestLike) => Promise<{ status: number; body: string }> {
  const board = new Map<string, Map<string, BeaconReport>>(); // account → node → report
  // Accounts already seeded from the store this isolate's lifetime. Without a
  // store this stays empty and nothing below it ever runs.
  const hydrated = new Set<string>();
  // One in-flight hydration per account: concurrent first requests must not
  // each issue their own load and clobber one another's seed.
  const hydrating = new Map<string, Promise<void>>();

  const hydrate = async (account: string): Promise<void> => {
    const store = o?.store;
    if (!store || hydrated.has(account)) return;
    let p = hydrating.get(account);
    if (!p) {
      p = (async () => {
        try {
          const saved = await store.load(account);
          if (saved) {
            // Merge UNDER anything this isolate already accepted: a POST that
            // landed while the load was in flight is newer than what the store
            // held when it was read, so it must not be overwritten by the seed.
            const acct = board.get(account) ?? new Map<string, BeaconReport>();
            for (const [node, report] of Object.entries(saved))
              if (!acct.has(node)) acct.set(node, report);
            board.set(account, acct);
          }
          hydrated.add(account);
        } catch {
          // A failed load is not a serving failure — the board is advisory,
          // self-healing data (§5.7): nodes re-PUT their own current vector on
          // their own debounce cycle, so an un-seeded board refills on its own.
          // Deliberately NOT marked hydrated: the next request retries the load.
        } finally {
          hydrating.delete(account);
        }
      })();
      hydrating.set(account, p);
    }
    await p;
  };

  // Tail of the in-flight save chain per account. Each link snapshots the board
  // when its own turn comes, so the last write to commit is the newest state.
  const saveChain = new Map<string, Promise<void>>();
  const saveSerialized = (
    store: BeaconBoardStore,
    account: string,
    acct: Map<string, BeaconReport>,
  ): Promise<void> => {
    const prev = saveChain.get(account) ?? Promise.resolve();
    const next = prev.then(async () => {
      try {
        // snapshot HERE, not at enqueue time — see the call site
        await store.save(account, Object.fromEntries(acct));
      } catch {
        // a failed save never fails the request (see the call site)
      }
    });
    saveChain.set(account, next);
    void next.finally(() => {
      if (saveChain.get(account) === next) saveChain.delete(account);
    });
    return next;
  };

  return async (req) => {
    if (o?.token !== undefined && req.headers.get("authorization") !== `Bearer ${o.token}`)
      return { status: 401, body: "" };
    const URLCtor = (globalThis as unknown as {
      URL: new (u: string, base?: string) => { pathname: string };
    }).URL;
    const path = new URLCtor(req.url, "http://x").pathname;
    const m = /^\/v1\/a\/([^/]+)\/vectors$/.exec(path);
    if (!m) return { status: 404, body: "" };
    const account = decodeURIComponent(m[1]!);
    if (req.method === "GET") {
      await hydrate(account);
      const reports = [...(board.get(account)?.values() ?? [])];
      return { status: 200, body: JSON.stringify(reports) };
    }
    if (req.method === "POST") {
      let acct: Map<string, BeaconReport>;
      try {
        const report = (await req.json()) as BeaconReport;
        if (typeof report.node !== "string") throw new Error("bad report");
        await hydrate(account);
        acct = board.get(account) ?? new Map<string, BeaconReport>();
        acct.set(report.node, report); // one report per node, overwrite (§14)
        board.set(account, acct);
      } catch {
        return { status: 400, body: "" };
      }
      // Write-through, but never at the cost of the request: a save failure
      // leaves this isolate's board updated and still answers 204. Vectors are
      // advisory and self-healing, so a lost write degrades staleness estimates
      // for one debounce cycle — it cannot corrupt sync, block a write, or lose
      // durable log data. Over-engineering transactional guarantees here would
      // buy nothing the data's own design doesn't already provide.
      //
      // Saves for one account are SERIALIZED, and each snapshots the board at
      // its own turn rather than at enqueue time. Concurrent POSTs otherwise
      // race: two overlapping saves can commit out of order and leave the store
      // holding the OLDER snapshot, so a node that was acknowledged with a 204
      // is missing after the next hibernation — a lost write, not the tolerable
      // "lost the last few seconds" this design accepts.
      if (o?.store) await saveSerialized(o.store, account, acct);
      return { status: 204, body: "" };
    }
    return { status: 405, body: "" };
  };
}
