// SPEC §5.7 (DESIGN), §14 — beacon client: debounced content-free vector
// reports plus node-side staleness prediction from known vectors.

import { misuse } from "./errors.js";
import type { LogCore } from "./log.js";
import type { TopicRegistry } from "./topics.js";
import type {
  BeaconHandle,
  BeaconReport,
  BeaconTransport,
  Constants,
  Key,
  Staleness,
  Timers,
  Topic,
  WriterId,
} from "./types.js";

export interface BeaconHubDeps {
  core: LogCore;
  topics: TopicRegistry;
  writerId: WriterId;
  constants: Constants;
  timers: Timers;
  clock: () => number;
}

export class BeaconHub {
  private known: BeaconReport[] = [];
  private transport: BeaconTransport | null = null;
  private debounceTimer: unknown = null;
  private stopped = false;

  constructor(private readonly deps: BeaconHubDeps) {}

  start(t: BeaconTransport): BeaconHandle {
    if (this.transport) throw misuse("beacon already started");
    this.transport = t;
    this.push(); // initial report; GET piggybacks on each push
    return {
      stop: () => {
        this.stopped = true;
        if (this.debounceTimer !== null) this.deps.timers.clearTimeout(this.debounceTimer);
        this.transport = null;
      },
    };
  }

  // called from the applied fan-out — 5 s debounce after append (§5.7)
  notifyApplied(): void {
    if (!this.transport || this.stopped || this.debounceTimer !== null) return;
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

  private push(): void {
    const t = this.transport;
    if (!t || this.stopped) return;
    const report: BeaconReport = {
      node: this.deps.writerId,
      at: new Date(this.deps.clock()).toISOString(),
      vectors: this.deps.core.vectors(),
    };
    void t
      .put(report)
      .then(() => t.get())
      .then((reports) => {
        if (!this.stopped) this.known = reports;
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
