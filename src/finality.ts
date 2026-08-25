// SPEC §7 — finality certificates: proposal computation (the library computes,
// the host signs), verified ingestion with strict generation monotonicity, and
// propagation via push + HAVE-fgen repair (wired through SyncEngine).

import { jcs } from "./encoding.js";
import { misuse, SeqscribeError } from "./errors.js";
import { orderCompare } from "./hlc.js";
import type { LogCore } from "./log.js";
import type { Store } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type {
  Anomaly,
  AuthorityHooks,
  Constants,
  FinalityCert,
  JsonValue,
  Topic,
  WriterId,
} from "./types.js";

export interface FinalityHubDeps {
  core: LogCore;
  store: Store;
  topics: TopicRegistry;
  constants: Constants;
  clock: () => number;
  emitAnomaly: (a: Anomaly) => void;
  authority?: AuthorityHooks | undefined;
}

export class FinalityHub {
  private broadcast: ((topic: Topic, cert: FinalityCert) => void) | null = null;
  private onAccepted: ((cert: FinalityCert, quarantined: number) => void) | null = null;

  constructor(private readonly deps: FinalityHubDeps) {}

  setBroadcast(fn: (topic: Topic, cert: FinalityCert) => void): void {
    this.broadcast = fn;
  }

  setOnAccepted(fn: (cert: FinalityCert, quarantined: number) => void): void {
    this.onAccepted = fn;
  }

  finality(topic: Topic): FinalityCert | null {
    this.deps.topics.get(topic);
    return this.deps.core.getCert(topic);
  }

  // §7.2: P = total-order max among held entries with hlc.l ≤ now − WINDOW;
  // empty set ⇒ null (no advancement). Hosts never reimplement this.
  proposeFinality(topic: Topic): Omit<FinalityCert, "sig"> | null {
    const policy = this.deps.topics.get(topic).policy;
    const authorityId = policy.finalityAuthority;
    if (authorityId === undefined)
      throw misuse(`proposeFinality on a topic with no configured authority (${topic})`);
    const cutoff = this.deps.clock() - this.deps.constants.FINALITY_WINDOW_MS;
    const p = this.deps.store.maxOrderUpTo(topic, cutoff);
    if (!p) return null;
    const existing = this.deps.core.getCert(topic);
    if (existing && orderCompare(p, existing.order) <= 0) return null; // nothing new to certify

    const cut: Record<WriterId, { seq: number; chain: string }> = {};
    for (const writer of this.deps.store.topicWriters(topic)) {
      const seq = this.deps.store.lastSeqAtOrBeforeOrder(topic, writer, p);
      if (seq === 0) continue; // absent writer ⇒ seq 0 (§7.2)
      const chain = this.deps.store.getEntry(topic, writer, seq)?.chain;
      if (chain === undefined) continue;
      cut[writer] = { seq, chain };
    }
    return {
      topic,
      order: p,
      cut,
      generation: (existing?.generation ?? 0) + 1,
      authority: authorityId,
    };
  }

  // Host path: throws on invalid input. Applies locally, then pushes FINALITY
  // to READY peers (§7.4).
  async ingestFinality(cert: FinalityCert): Promise<void> {
    const outcome = await this.verifyAndClassify(cert);
    if (outcome === "duplicate") return;
    if (outcome !== "accept")
      throw new SeqscribeError("ERR_MISUSE", `finality cert rejected: ${outcome}`);
    const quarantined = await this.deps.core.applyCert(cert);
    this.onAccepted?.(cert, quarantined);
    this.broadcast?.(cert.topic, cert);
  }

  // Wire path: never throws — invalid certs are dropped (with bad_cert where
  // the spec says so).
  async ingestFromWire(cert: FinalityCert): Promise<void> {
    try {
      const outcome = await this.verifyAndClassify(cert);
      if (outcome !== "accept") return;
      const quarantined = await this.deps.core.applyCert(cert);
      this.onAccepted?.(cert, quarantined);
      this.broadcast?.(cert.topic, cert);
    } catch {
      // malformed beyond classification — drop
    }
  }

  private async verifyAndClassify(
    cert: FinalityCert,
  ): Promise<"accept" | "duplicate" | "stale" | "bad"> {
    if (!this.deps.topics.has(cert.topic)) return "bad";
    const policy = this.deps.topics.get(cert.topic).policy;
    if (policy.finalityAuthority === undefined || cert.authority !== policy.finalityAuthority) {
      this.deps.emitAnomaly({ kind: "bad_cert" });
      return "bad";
    }
    const verify = this.deps.authority?.verifyFinality;
    if (!verify) return "bad";
    if (!(await verify(cert))) {
      this.deps.emitAnomaly({ kind: "bad_cert" });
      return "bad";
    }
    const existing = this.deps.core.getCert(cert.topic);
    if (existing) {
      if (cert.generation < existing.generation) return "stale";
      if (cert.generation === existing.generation) {
        if (jcs(cert as unknown as JsonValue) === jcs(existing as unknown as JsonValue))
          return "duplicate";
        // generation reuse with different content (§7.2)
        this.deps.emitAnomaly({ kind: "bad_cert" });
        return "bad";
      }
      if (orderCompare(cert.order, existing.order) < 0) {
        this.deps.emitAnomaly({ kind: "bad_cert" }); // watermark went backwards
        return "bad";
      }
    }
    return "accept";
  }
}
