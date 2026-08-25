// SPEC §13 — writer directives: signed artifacts in (`publishWriterDirective`),
// host-sugar `retire`/`unretire` through the issuer hook with match
// verification, wire ingestion, and rgen-repair propagation (wired via sync).

import { jcs } from "./encoding.js";
import { misuse, SeqscribeError } from "./errors.js";
import type { LogCore } from "./log.js";
import type { RegisterHub } from "./register.js";
import type { Store } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type {
  Anomaly,
  AuthorityHooks,
  JsonValue,
  Topic,
  WriterDirective,
  WriterId,
} from "./types.js";

export interface DirectiveHubDeps {
  core: LogCore;
  store: Store;
  topics: TopicRegistry;
  writerId: WriterId;
  emitAnomaly: (a: Anomaly) => void;
  authority?: AuthorityHooks | undefined;
  registers?: RegisterHub | undefined;
}

export class DirectiveHub {
  private broadcast: ((d: WriterDirective) => void) | null = null;
  private onApplied: ((d: WriterDirective, result: string) => void) | null = null;

  constructor(private readonly deps: DirectiveHubDeps) {}

  setBroadcast(fn: (d: WriterDirective) => void): void {
    this.broadcast = fn;
  }

  setOnApplied(fn: (d: WriterDirective, result: string) => void): void {
    this.onApplied = fn;
  }

  latest(topic: Topic, writer: WriterId): WriterDirective | null {
    const raw = this.deps.store.directiveLatest(topic, writer);
    return raw ? (JSON.parse(raw) as WriterDirective) : null;
  }

  // Host path (the normative artifact API): throws on invalid input.
  async publishWriterDirective(d: WriterDirective): Promise<void> {
    this.validateShape(d);
    if (!(await this.verify(d)))
      throw new SeqscribeError("ERR_MISUSE", "writer directive failed verification");
    const result = await this.deps.core.applyDirective(d);
    if (result === "refused") throw new SeqscribeError("ERR_MISUSE", "writer directive refused");
    this.onApplied?.(d, result);
    this.broadcast?.(d);
  }

  // Wire path: verify, apply, never throw.
  async ingestFromWire(d: WriterDirective): Promise<void> {
    try {
      this.validateShape(d);
      if (!(await this.verify(d))) {
        this.deps.emitAnomaly({ kind: "bad_directive" });
        return;
      }
      const result = await this.deps.core.applyDirective(d);
      if (result === "applied" || result === "recovery") {
        this.onApplied?.(d, result);
        this.broadcast?.(d);
      }
    } catch {
      // malformed — drop
    }
  }

  // retire() sugar (§13): one unsigned directive per topic where the writer has
  // a stream; finalSeq/finalChain are the per-topic stream heads.
  async retire(writer: WriterId, o?: { chownTo?: WriterId }): Promise<void> {
    const issue = this.deps.authority?.issueWriterDirective;
    if (!issue) throw misuse("retire() requires authority.issueWriterDirective");

    if (o?.chownTo && writer === this.deps.writerId && this.deps.registers) {
      await this.bulkChown(writer, o.chownTo);
    }

    const streams = this.deps.store.listWriters().filter((w) => w.writer === writer);
    if (streams.length === 0) throw misuse(`retire(): writer ${writer} has no streams`);
    for (const w of streams) {
      const unsigned: Omit<WriterDirective, "authority" | "sig"> = {
        topic: w.topic,
        writer,
        state: "retired",
        rgen: w.rgen + 1,
        finalSeq: w.contigSeq,
        finalChain: w.contigChain,
      };
      const signed = await issue(unsigned);
      this.assertMatchesProposal(unsigned, signed);
      await this.publishWriterDirective(signed);
    }
  }

  async unretire(writer: WriterId): Promise<void> {
    const issue = this.deps.authority?.issueWriterDirective;
    if (!issue) throw misuse("unretire() requires authority.issueWriterDirective");
    const streams = this.deps.store
      .listWriters()
      .filter((w) => w.writer === writer && w.sealReason === "retired");
    if (streams.length === 0) throw misuse(`unretire(): writer ${writer} has no retired streams`);
    for (const w of streams) {
      const unsigned: Omit<WriterDirective, "authority" | "sig"> = {
        topic: w.topic,
        writer,
        state: "live",
        rgen: w.rgen + 1,
      };
      const signed = await issue(unsigned);
      this.assertMatchesProposal(unsigned, signed);
      await this.publishWriterDirective(signed);
    }
  }

  private async bulkChown(writer: WriterId, chownTo: WriterId): Promise<void> {
    const regs = this.deps.registers!;
    for (const topic of this.deps.topics.list()) {
      if (this.deps.topics.get(topic).policy.kind !== "register") continue;
      await regs.settle(topic);
      const snap = regs.snapshotState(topic);
      const reg = regs.handle(topic);
      for (const [key, ks] of Object.entries(snap.keys)) {
        if (ks.owner === writer) await reg.chown(key, chownTo);
      }
    }
  }

  private validateShape(d: WriterDirective): void {
    if (d.state !== "live" && d.state !== "retired") throw misuse("bad directive state");
    if (!Number.isSafeInteger(d.rgen) || d.rgen < 1) throw misuse("bad directive rgen");
    if (d.state === "retired" && (d.finalSeq === undefined || d.finalChain === undefined))
      throw misuse("retired directive requires finalSeq/finalChain");
    this.deps.topics.get(d.topic);
  }

  private async verify(d: WriterDirective): Promise<boolean> {
    const verify = this.deps.authority?.verifyWriterDirective;
    if (!verify) return false; // no verifier configured → no directive is acceptable
    return Boolean(await verify(d));
  }

  private assertMatchesProposal(
    unsigned: Omit<WriterDirective, "authority" | "sig">,
    signed: WriterDirective,
  ): void {
    // the issuer fills authority+sig only; everything else MUST match (§14)
    const { authority: _a, sig: _s, ...rest } = signed;
    if (jcs(rest as unknown as JsonValue) !== jcs(unsigned as unknown as JsonValue))
      throw misuse("issueWriterDirective returned a directive that differs from the proposal");
  }
}
