// Host-side helpers. Everything here is OPTIONAL sugar over the host contract
// (docs/host-guide.md): the library still never signs or reconnects on its own —
// these implement the host's obligations in the common shapes so a fleet with a
// shared secret and ordinary channels wires up in a few lines.

import { hmac } from "@noble/hashes/hmac";
import { sha256 } from "@noble/hashes/sha256";
import { bytesToHex } from "@noble/hashes/utils";
import { jcs } from "./encoding.js";
import { misuse } from "./errors.js";
import type {
  AuthorityHooks,
  Channel,
  FinalityCert,
  JsonValue,
  PeerHandle,
  SeqscribeNode,
  SqliteHandle,
  Timers,
  Topic,
  WriterDirective,
  WriterId,
} from "./types.js";

const utf8 = new (globalThis as unknown as {
  TextEncoder: new () => { encode(s: string): Uint8Array };
}).TextEncoder();

const g = globalThis as unknown as {
  setTimeout(cb: () => void, ms: number): unknown;
  clearTimeout(h: unknown): void;
  crypto?: { getRandomValues(a: Uint8Array): Uint8Array };
};
const defaultTimers: Timers = {
  setTimeout: (cb, ms) => g.setTimeout(cb, ms),
  clearTimeout: (h) => g.clearTimeout(h),
};

// ---- HMAC authority (shared fleet secret) ----
//
// Signatures are hex HMAC-SHA256 over the JCS bytes of the artifact minus its
// signature field. Fits the §4.1 trust model (all full peers already share
// content trust; the secret only has to be as private as the fleet). Hosts
// needing asymmetric keys implement AuthorityHooks themselves — the shapes
// here show exactly what must be covered.

export interface HmacAuthorityOpts {
  authorityId: string;
  secret: string;
  // §14 role binding: whether this authority governs (topic, writer). Without
  // it any valid signer could retire arbitrary writers — default governs all,
  // which is only safe in single-authority fleets.
  governs?: (topic: Topic, writer: WriterId) => boolean;
}

export interface HmacAuthority extends AuthorityHooks {
  signFinality(proposal: Omit<FinalityCert, "sig">): FinalityCert;
  signDirective(unsigned: Omit<WriterDirective, "authority" | "sig">): WriterDirective;
}

export function hmacAuthority(opts: HmacAuthorityOpts): HmacAuthority {
  const key = utf8.encode(opts.secret);
  const sig = (v: JsonValue): string => bytesToHex(hmac(sha256, key, utf8.encode(jcs(v))));
  const governs = opts.governs ?? (() => true);

  const signFinality: HmacAuthority["signFinality"] = (proposal) => {
    if (proposal.authority !== opts.authorityId)
      throw misuse(`proposal authority ${proposal.authority} != ${opts.authorityId}`);
    return { ...proposal, sig: sig(proposal as unknown as JsonValue) };
  };
  const signDirective: HmacAuthority["signDirective"] = (unsigned) => {
    const withAuthority = { ...unsigned, authority: opts.authorityId };
    return { ...withAuthority, sig: sig(withAuthority as unknown as JsonValue) };
  };

  return {
    signFinality,
    signDirective,
    verifyFinality(cert) {
      if (cert.authority !== opts.authorityId) return false;
      const { sig: got, ...unsigned } = cert;
      return got === sig(unsigned as unknown as JsonValue);
    },
    verifyWriterDirective(d) {
      if (d.authority !== opts.authorityId) return false;
      if (!governs(d.topic, d.writer)) return false; // role binding, not just signature
      const { sig: got, ...unsigned } = d;
      return got === sig(unsigned as unknown as JsonValue);
    },
    issueWriterDirective: async (unsigned) => signDirective(unsigned),
    verifyTakeover(evt) {
      const p = evt.payload as { newOwner?: string; hostSig?: string } | null;
      if (!p || typeof p.newOwner !== "string" || typeof p.hostSig !== "string") return false;
      return p.hostSig === sig({ topic: evt.topic, key: evt.key ?? "", newOwner: p.newOwner });
    },
    issueTakeover: async (unsigned) => ({
      hostSig: sig({ topic: unsigned.topic, key: unsigned.key, newOwner: unsigned.newOwner }),
    }),
  };
}

// ---- finality authority loop ----

export interface FinalityLoopHandle {
  stop(): void;
  runOnce(): Promise<number>; // certs issued
}

// The §7.4 issuance loop: propose → sign → ingest, per topic, on a cadence.
// Run this on the authority host only.
export function startFinalityLoop(
  node: SeqscribeNode,
  o: {
    topics: Topic[];
    authority: Pick<HmacAuthority, "signFinality">;
    intervalMs: number;
    timers?: Timers;
    onError?: (topic: Topic, err: unknown) => void;
  },
): FinalityLoopHandle {
  const timers = o.timers ?? defaultTimers;
  let stopped = false;
  let timer: unknown = null;

  const runOnce = async (): Promise<number> => {
    let issued = 0;
    for (const topic of o.topics) {
      try {
        const proposal = node.proposeFinality(topic);
        if (!proposal) continue; // empty window / nothing new — correct silence
        await node.ingestFinality(o.authority.signFinality(proposal));
        issued++;
      } catch (err) {
        o.onError?.(topic, err);
      }
    }
    return issued;
  };

  const tick = () => {
    if (stopped) return;
    void runOnce().finally(() => {
      if (!stopped) timer = timers.setTimeout(tick, o.intervalMs);
    });
  };
  timer = timers.setTimeout(tick, o.intervalMs);

  return {
    stop() {
      stopped = true;
      if (timer !== null) timers.clearTimeout(timer);
    },
    runOnce,
  };
}

// ---- reconnect manager ----

export interface ReconnectHandle {
  stop(): void;
  current(): PeerHandle | null;
}

// The host contract's reconnect obligation (§5.2 "host reconnects") in its
// common shape: dial, attach, and on close redial with jittered exponential
// backoff; backoff resets once a session reaches ready.
export function manageReconnect(
  node: SeqscribeNode,
  o: {
    peerId: string;
    peerClass: "content" | "metadata";
    grants: Record<Topic, "full" | "serve" | "none">;
    dial: () => Channel | Promise<Channel>;
    backoff?: { minMs?: number; maxMs?: number; factor?: number };
    timers?: Timers;
    rng?: () => number;
    onError?: (err: unknown) => void;
  },
): ReconnectHandle {
  const timers = o.timers ?? defaultTimers;
  const rng = o.rng ?? Math.random;
  const minMs = o.backoff?.minMs ?? 500;
  const maxMs = o.backoff?.maxMs ?? 30_000;
  const factor = o.backoff?.factor ?? 2;

  let stopped = false;
  let handle: PeerHandle | null = null;
  let timer: unknown = null;
  let delay = minMs;

  const schedule = (ms: number) => {
    if (stopped) return;
    timer = timers.setTimeout(attempt, ms + Math.floor(rng() * ms * 0.25));
  };

  const attempt = () => {
    if (stopped) return;
    void (async () => {
      try {
        const ch = await o.dial();
        if (stopped) {
          ch.close();
          return;
        }
        const h = node.attach(ch, { peerId: o.peerId, peerClass: o.peerClass, grants: o.grants });
        handle = h;
        h.onStateChange((s) => {
          if (s === "ready") delay = minMs; // healthy session resets the backoff
          if (s === "closed" && !stopped) {
            handle = null;
            schedule(delay);
            delay = Math.min(maxMs, delay * factor);
          }
        });
      } catch (err) {
        o.onError?.(err);
        schedule(delay);
        delay = Math.min(maxMs, delay * factor);
      }
    })();
  };
  attempt();

  return {
    stop() {
      stopped = true;
      if (timer !== null) timers.clearTimeout(timer);
      handle?.detach();
      handle = null;
    },
    current: () => handle,
  };
}

// ---- writerId persistence ----

// writerId discipline (host-guide §1): stable per machine, never reused across
// machines. This persists a generated id in sq_meta so restarts keep it —
// NOTE: restoring the DB file onto a second machine copies the id, which is a
// fork by construction; clone/restore procedures must delete the row first.
export function loadOrCreateWriterId(
  storage: SqliteHandle,
  o?: { prefix?: string; rng?: () => number },
): WriterId {
  storage.run("CREATE TABLE IF NOT EXISTS sq_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)");
  const existing = storage.get<{ v: string }>("SELECT v FROM sq_meta WHERE k = 'writer_id'");
  if (existing) return existing.v;
  let hex = "";
  if (g.crypto) {
    const buf = new Uint8Array(8);
    g.crypto.getRandomValues(buf);
    hex = bytesToHex(buf);
  } else {
    const rng = o?.rng ?? Math.random;
    for (let i = 0; i < 16; i++) hex += Math.floor(rng() * 16).toString(16);
  }
  const id = `${o?.prefix ?? "node"}-${hex}`;
  storage.run("INSERT INTO sq_meta (k, v) VALUES ('writer_id', ?)", [id]);
  return id;
}

// ---- legacy JSONL migration ----

// Genesis migration for pre-seqscribe JSONL logs (e.g. the ADHDev mesh
// ledger): each line becomes an ordinary append on THIS node's stream —
// chains/hlc/seq are minted fresh, which is the point (the legacy file had
// none). Run once per legacy file, on the machine that owned it.
export async function migrateLegacyJsonl(
  node: SeqscribeNode,
  topic: Topic,
  lines: AsyncIterable<string> | Iterable<string>,
  o?: { kind?: (obj: JsonValue) => string },
): Promise<number> {
  const pending: Promise<unknown>[] = [];
  let count = 0;
  for await (const line of lines) {
    const trimmed = line.trim();
    if (trimmed === "") continue;
    const obj = JSON.parse(trimmed) as JsonValue;
    const kind = o?.kind?.(obj) ?? "event";
    pending.push(node.log(topic).append(kind, obj));
    count++;
  }
  await Promise.all(pending);
  return count;
}
