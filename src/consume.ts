// SPEC §9 `onEntry` — at-least-once, per-consumer serial dispatch in rowid
// order. The cursor advances only after the callback's promise resolves;
// failure leaves the cursor and retries with backoff. Deliveries are
// provisional until finality covers them (§7.5).

import { misuse } from "./errors.js";
import type { Store } from "./store.js";
import type { TopicRegistry } from "./topics.js";
import type { Constants, LogEntry, Timers, Topic, Unsub } from "./types.js";

const BATCH = 64;
const BACKOFF_BASE_MS = 100;
const BACKOFF_MAX_MS = 30_000;

// P19 catch-up waiter: resolves once the durable cursor reaches the topic
// head observed when caughtUp() was called; a later append starts a new
// interval rather than deferring resolution forever.
interface CatchUpWaiter {
  through: number;
  resolve: (r: { throughRowid: number }) => void;
  reject: (e: unknown) => void;
}

interface Consumer {
  topic: Topic;
  name: string;
  cb: (e: LogEntry) => void | Promise<void>;
  running: boolean;
  rerun: boolean;
  failures: number;
  retryTimer: unknown;
  gone: boolean;
  waiters: CatchUpWaiter[];
}

// proposals-v3.5 P17 — the actual replay boundary a reset installed
export interface ConsumerResetResult {
  existed: boolean; // a durable cursor row was already present
  from: "earliest-retained" | "head";
  replayFromRowid: number; // the installed cursor (0 = full retained hot log)
  // coverage metadata: rows already cold-archived (§7.6) are NOT replayed by
  // "earliest-retained" — a non-zero count means "start" is the archive floor,
  // not genesis; rebuild completeness below the cut needs the snapshot basis
  archivedRows: number;
}

export interface ConsumerInfo {
  consumer: string;
  lastRowid: number;
  updatedAt: string;
  active: boolean; // registered in this process right now
}

export class ConsumerHub {
  private readonly consumers = new Map<string, Consumer>();
  private onAdvance: ((topic: Topic) => void) | null = null;
  private closed = false;

  constructor(
    private readonly deps: {
      store: Store;
      topics: TopicRegistry;
      timers: Timers;
      constants: Constants;
      clock: () => number;
    },
  ) {}

  // archiving hook: fires after a drain pass advanced a cursor (§7.6)
  setOnAdvance(fn: (topic: Topic) => void): void {
    this.onAdvance = fn;
  }

  // §14 close quiescence: cancel backoff timers and mark every consumer gone
  // so an in-flight drain stops before its next store access. Cursors are
  // durable — redelivery on next open preserves at-least-once (§9).
  close(): void {
    this.closed = true;
    for (const c of this.consumers.values()) {
      c.gone = true;
      if (c.retryTimer !== null) {
        this.deps.timers.clearTimeout(c.retryTimer);
        c.retryTimer = null;
      }
      this.rejectWaiters(c, misuse("node closed before catch-up"));
    }
    this.consumers.clear();
  }

  private rejectWaiters(c: Consumer, err: unknown): void {
    const ws = c.waiters.splice(0);
    for (const w of ws) w.reject(err);
  }

  onEntry(topic: Topic, name: string, cb: (e: LogEntry) => void | Promise<void>): Unsub {
    if (this.closed) throw misuse("node is closed");
    const policy = this.deps.topics.get(topic).policy;
    if (policy.retention.mode !== "full")
      throw misuse(`onEntry requires retention "full" (${topic}) — ring/none serve via SUB`);
    const key = `${topic} ${name}`;
    if (this.consumers.has(key)) throw misuse(`consumer already registered: ${key}`);
    // a registered consumer gates archiving from the moment it registers (§7.6)
    // — materialize its cursor row even before the first delivery succeeds
    if (this.deps.store.cursorsForTopic(topic).every((c) => c.consumer !== name))
      this.deps.store.cursorSet(name, topic, 0, new Date(this.deps.clock()).toISOString());
    const c: Consumer = {
      topic,
      name,
      cb,
      running: false,
      rerun: false,
      failures: 0,
      retryTimer: null,
      gone: false,
      waiters: [],
    };
    this.consumers.set(key, c);
    this.pump(c);
    return () => {
      c.gone = true;
      if (c.retryTimer !== null) this.deps.timers.clearTimeout(c.retryTimer);
      this.rejectWaiters(c, misuse(`consumer unsubscribed before catch-up: ${key}`));
      this.consumers.delete(key);
    };
  }

  // ---- durable-consumer lifecycle (proposals-v3.5 P17–P19) ----

  isActive(topic: Topic, name: string): boolean {
    return this.consumers.has(`${topic} ${name}`);
  }

  private assertInactive(topic: Topic, name: string, op: string): void {
    if (this.closed) throw misuse("node is closed");
    this.deps.topics.get(topic);
    if (this.consumers.has(`${topic} ${name}`))
      throw misuse(`${op} on active consumer ${topic} ${name} — unsubscribe first`);
  }

  // P17: discard-derived-state rebuild for the SAME stable consumer name.
  // Inactive-only (registration would race the cursor write); the next
  // onEntry with this name resumes at the installed boundary. NOTE: the reset
  // materializes a cursor row, which gates §7.6 archiving from this moment —
  // re-register promptly, or the row ages into consumer_abandoned.
  resetConsumer(
    topic: Topic,
    name: string,
    o?: { from?: "earliest-retained" | "head" },
  ): ConsumerResetResult {
    this.assertInactive(topic, name, "resetConsumer");
    const from = o?.from ?? "earliest-retained";
    const existed = this.deps.store.cursorsForTopic(topic).some((c) => c.consumer === name);
    const replayFromRowid = from === "head" ? this.deps.store.maxRowid(topic) : 0;
    this.deps.store.cursorSet(name, topic, replayFromRowid, new Date(this.deps.clock()).toISOString());
    return {
      existed,
      from,
      replayFromRowid,
      archivedRows: from === "earliest-retained" ? this.deps.store.archivedCount(topic) : 0,
    };
  }

  // P18: explicit durable-cursor deletion — never implicit (forgetting a
  // cursor changes at-least-once restart semantics). Inactive-only; a deleted
  // cursor no longer gates archiving, so the archive floor re-evaluates.
  deleteConsumer(topic: Topic, name: string): { existed: boolean } {
    this.assertInactive(topic, name, "deleteConsumer");
    const existed = this.deps.store.cursorsForTopic(topic).some((c) => c.consumer === name);
    if (existed) {
      this.deps.store.cursorDelete(name, topic);
      this.onAdvance?.(topic); // §7.6 re-evaluation, same as cursor advancement
    }
    return { existed };
  }

  listConsumers(topic: Topic): ConsumerInfo[] {
    if (this.closed) throw misuse("node is closed");
    this.deps.topics.get(topic);
    return this.deps.store
      .cursorsForTopic(topic)
      .map((c) => ({ ...c, active: this.isActive(topic, c.consumer) }));
  }

  // P18 GC: deliberate cleanup of versioned/renamed consumers. Only inactive
  // cursors are eligible; filters compose (prefix AND inactiveBefore).
  pruneConsumers(topic: Topic, o?: { prefix?: string; inactiveBefore?: number }): string[] {
    if (this.closed) throw misuse("node is closed");
    this.deps.topics.get(topic);
    const pruned: string[] = [];
    for (const c of this.deps.store.cursorsForTopic(topic)) {
      if (this.isActive(topic, c.consumer)) continue;
      if (o?.prefix !== undefined && !c.consumer.startsWith(o.prefix)) continue;
      if (o?.inactiveBefore !== undefined) {
        const at = Date.parse(c.updatedAt);
        if (!Number.isFinite(at) || at >= o.inactiveBefore) continue;
      }
      this.deps.store.cursorDelete(c.consumer, topic);
      pruned.push(c.consumer);
    }
    if (pruned.length > 0) this.onAdvance?.(topic);
    return pruned;
  }

  // P19: deterministic caught-up signal. Snapshots the topic head NOW and
  // resolves once the durable cursor reaches it (i.e. every entry through that
  // head has completed its callback); later appends start a new interval.
  // Callback failure/backoff delays resolution by construction (the cursor
  // does not advance); unsubscribe and node close reject.
  caughtUp(topic: Topic, name: string): Promise<{ throughRowid: number }> {
    try {
      if (this.closed) throw misuse("node is closed");
      this.deps.topics.get(topic);
      const c = this.consumers.get(`${topic} ${name}`);
      if (!c) throw misuse(`caughtUp on unregistered consumer ${topic} ${name}`);
      const through = this.deps.store.maxRowid(topic);
      if (this.deps.store.cursorGet(name, topic) >= through)
        return Promise.resolve({ throughRowid: through });
      return new Promise((resolve, reject) => c.waiters.push({ through, resolve, reject }));
    } catch (e) {
      return Promise.reject(e); // Promise-returning APIs reject (P11 discipline)
    }
  }

  notifyApplied(topic: Topic): void {
    for (const c of this.consumers.values()) {
      if (c.topic === topic) this.pump(c);
    }
  }

  private pump(c: Consumer): void {
    if (c.running || c.gone) {
      c.rerun = c.running;
      return;
    }
    c.running = true;
    void this.drain(c).finally(() => {
      c.running = false;
      if (c.rerun && !c.gone) {
        c.rerun = false;
        this.pump(c);
      }
    });
  }

  private async drain(c: Consumer): Promise<void> {
    let advanced = false;
    try {
      await this.drainInner(c, () => (advanced = true));
    } finally {
      if (advanced) this.onAdvance?.(c.topic);
    }
  }

  private async drainInner(c: Consumer, onProgress: () => void): Promise<void> {
    for (;;) {
      if (c.gone) return;
      const cursor = this.deps.store.cursorGet(c.name, c.topic);
      const rows = this.deps.store.entriesForTopicFromRowid(c.topic, cursor, BATCH);
      if (rows.length === 0) return;
      for (const { entry, rowid } of rows) {
        if (c.gone) return;
        try {
          await c.cb(entry);
        } catch {
          if (c.gone) return; // closed/unsubscribed while the callback ran
          // no advance; retry with backoff (§9). Touch the timestamp so an
          // actively-retrying consumer is not "idle" for §7.6 abandonment —
          // only a consumer whose process is gone ages out.
          this.deps.store.cursorSet(
            c.name,
            c.topic,
            this.deps.store.cursorGet(c.name, c.topic),
            new Date(this.deps.clock()).toISOString(),
          );
          c.failures++;
          const delay = Math.min(BACKOFF_BASE_MS * 2 ** (c.failures - 1), BACKOFF_MAX_MS);
          // a notify-triggered drain can fail while a retry is already
          // scheduled — replace it, never orphan it (close() must be able to
          // cancel every live timer)
          if (c.retryTimer !== null) this.deps.timers.clearTimeout(c.retryTimer);
          c.retryTimer = this.deps.timers.setTimeout(() => {
            c.retryTimer = null;
            this.pump(c);
          }, delay);
          return;
        }
        if (c.gone) return; // closed/unsubscribed while the callback ran
        c.failures = 0;
        this.deps.store.cursorSet(
          c.name,
          c.topic,
          rowid,
          new Date(this.deps.clock()).toISOString(),
        );
        onProgress();
        if (c.waiters.length > 0) {
          const done = c.waiters.filter((w) => rowid >= w.through);
          if (done.length > 0) {
            c.waiters = c.waiters.filter((w) => rowid < w.through);
            for (const w of done) w.resolve({ throughRowid: w.through });
          }
        }
      }
    }
  }
}
