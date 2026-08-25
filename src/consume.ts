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

  constructor(
    private readonly deps: {
      store: Store;
      topics: TopicRegistry;
      timers: Timers;
      constants: Constants;
      clock: () => number;
    },
  ) {}

  onEntry(topic: Topic, name: string, cb: (e: LogEntry) => void | Promise<void>): Unsub {
    const policy = this.deps.topics.get(topic).policy;
    if (policy.retention.mode !== "full")
      throw misuse(`onEntry requires retention "full" (${topic}) — ring/none serve via SUB`);
    const key = `${topic} ${name}`;
    if (this.consumers.has(key)) throw misuse(`consumer already registered: ${key}`);
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
          // no advance; retry with backoff (§9)
          c.failures++;
          const delay = Math.min(BACKOFF_BASE_MS * 2 ** (c.failures - 1), BACKOFF_MAX_MS);
          c.retryTimer = this.deps.timers.setTimeout(() => {
            c.retryTimer = null;
            this.pump(c);
          }, delay);
          return;
        }
        c.failures = 0;
        this.deps.store.cursorSet(
          c.name,
          c.topic,
          rowid,
          new Date(this.deps.clock()).toISOString(),
        );
      }
    }
  }
}
