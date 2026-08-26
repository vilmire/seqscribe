// SPEC §7.6 — cold archiving: certificate effects are immediate, archiving
// lags locally until every registered consumer's cursor passes the covered
// region; a consumer idle past FINALITY_WINDOW_MS is dropped and resumes at
// the first post-cut rowid. Archived rows move to sq_archive (out of the hot
// log; the file-based archive/<topic>.jsonl.gz form is an adapter concern —
// export can drain sq_archive).

import type { LogCore } from "./log.js";
import type { RegisterHub } from "./register.js";
import type { Store } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type { Anomaly, Constants, Topic } from "./types.js";
import type { ViewHub } from "./views.js";

export interface ArchiveHubDeps {
  core: LogCore;
  store: Store;
  topics: TopicRegistry;
  views: ViewHub;
  registers: RegisterHub;
  constants: Constants;
  clock: () => number;
  emitAnomaly: (a: Anomaly) => void;
}

export class ArchiveHub {
  private readonly busy = new Set<Topic>();

  constructor(private readonly deps: ArchiveHubDeps) {}

  // cert acceptance: rebase views/registers onto the cut (permanent base
  // checkpoint; pre-cut checkpoints prune), then archive what consumers allow
  async onCertAccepted(topic: Topic): Promise<void> {
    const cert = this.deps.core.getCert(topic);
    if (!cert) return;
    this.deps.views.rebaseAtCut(topic, cert.order);
    await this.deps.registers.rebaseAtCut(topic, cert.order);
    this.tryArchive(topic);
  }

  // consumer cursor advanced — more of the covered region may now be cold
  onConsumerAdvance(topic: Topic): void {
    this.tryArchive(topic);
  }

  tryArchive(topic: Topic): void {
    if (this.busy.has(topic)) return;
    this.busy.add(topic);
    try {
      this.archiveNow(topic);
    } finally {
      this.busy.delete(topic);
    }
  }

  private archiveNow(topic: Topic): void {
    const cert = this.deps.core.getCert(topic);
    if (!cert) return; // no finality → no compaction (§7.3)
    const now = this.deps.clock();
    const staleBefore = now - this.deps.constants.FINALITY_WINDOW_MS;

    // consumers gate archiving; stale ones are dropped (consumer_abandoned)
    let minCursor = Number.MAX_SAFE_INTEGER;
    for (const c of this.deps.store.cursorsForTopic(topic)) {
      const updatedAt = Date.parse(c.updatedAt);
      if (Number.isFinite(updatedAt) && updatedAt < staleBefore) {
        this.deps.store.cursorDelete(c.consumer, topic);
        this.deps.emitAnomaly({ kind: "consumer_abandoned" });
        continue; // resume resets to the first post-cut rowid (cursor row gone)
      }
      minCursor = Math.min(minCursor, c.lastRowid);
    }

    const at = new Date(now).toISOString();
    for (const [writer, cut] of Object.entries(cert.cut)) {
      if (cut.seq === 0) continue;
      this.deps.store.archiveCovered(topic, writer, cut.seq, minCursor, at);
    }
  }
}
