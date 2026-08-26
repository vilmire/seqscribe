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

interface Consumer {
  topic: Topic;
  name: string;
  cb: (e: LogEntry) => void | Promise<void>;
  running: boolean;
  rerun: boolean;
  failures: number;
  retryTimer: unknown;
  gone: boolean;
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
    }
    this.consumers.clear();
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
    };
    this.consumers.set(key, c);
    this.pump(c);
    return () => {
      c.gone = true;
      if (c.retryTimer !== null) this.deps.timers.clearTimeout(c.retryTimer);
      this.consumers.delete(key);
    };
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
      }
    }
  }
}
