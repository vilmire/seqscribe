// Virtual-time scheduler (docs/harness.md §4). Only scheduled delays advance
// the clock; compute is free. Between events every microtask settles, so
// library-internal promise chains resolve "instantaneously" in virtual time.

import { setImmediate as setImmediatePromise } from "node:timers/promises";
import type { Timers } from "../src/types.js";

interface Ev {
  at: number;
  seq: number;
  fn: () => void;
  cancelled: boolean;
}

export class Scheduler {
  private heap: Ev[] = [];
  private seqNo = 0;
  private nowMs: number;

  constructor(startMs = 0) {
    this.nowMs = startMs;
  }

  now(): number {
    return this.nowMs;
  }

  clock(): () => number {
    return () => this.nowMs;
  }

  schedule(atMs: number, fn: () => void): Ev {
    const ev: Ev = { at: Math.max(atMs, this.nowMs), seq: this.seqNo++, fn, cancelled: false };
    this.heap.push(ev);
    this.bubbleUp(this.heap.length - 1);
    return ev;
  }

  cancel(ev: Ev): void {
    ev.cancelled = true;
  }

  timers(): Timers {
    return {
      setTimeout: (cb, ms) => this.schedule(this.nowMs + ms, cb),
      clearTimeout: (h) => {
        if (h) this.cancel(h as Ev);
      },
    };
  }

  pendingCount(): number {
    return this.heap.filter((e) => !e.cancelled).length;
  }

  // Run events in (time, insertion) order until the queue is empty or untilMs
  // is passed. maxEvents guards against runaway self-rescheduling loops.
  async run(opts?: { untilMs?: number; maxEvents?: number }): Promise<void> {
    const max = opts?.maxEvents ?? 1_000_000;
    if (opts?.untilMs !== undefined && opts.untilMs < this.nowMs)
      throw new Error(`run untilMs ${opts.untilMs} is in the past (now ${this.nowMs})`);
    let count = 0;
    await this.drainMicrotasks();
    for (;;) {
      const ev = this.pop();
      if (!ev) {
        // no events left — time still advances to the requested boundary
        if (opts?.untilMs !== undefined && opts.untilMs > this.nowMs) this.nowMs = opts.untilMs;
        return;
      }
      if (opts?.untilMs !== undefined && ev.at > opts.untilMs) {
        // put it back; time stops at the boundary
        this.heap.push(ev);
        this.bubbleUp(this.heap.length - 1);
        this.nowMs = opts.untilMs;
        return;
      }
      if (++count > max) throw new Error(`scheduler exceeded ${max} events — runaway loop?`);
      this.nowMs = ev.at;
      ev.fn();
      await this.drainMicrotasks();
    }
  }

  async drainMicrotasks(): Promise<void> {
    await setImmediatePromise();
  }

  private pop(): Ev | undefined {
    for (;;) {
      const top = this.heap[0];
      if (!top) return undefined;
      const last = this.heap.pop();
      if (this.heap.length > 0 && last) {
        this.heap[0] = last;
        this.sinkDown(0);
      }
      if (!top.cancelled) return top;
    }
  }

  private less(a: Ev, b: Ev): boolean {
    return a.at !== b.at ? a.at < b.at : a.seq < b.seq;
  }

  private bubbleUp(i: number): void {
    while (i > 0) {
      const parent = (i - 1) >> 1;
      const a = this.heap[i];
      const b = this.heap[parent];
      if (!a || !b || !this.less(a, b)) return;
      this.heap[i] = b;
      this.heap[parent] = a;
      i = parent;
    }
  }

  private sinkDown(i: number): void {
    for (;;) {
      const l = 2 * i + 1;
      const r = 2 * i + 2;
      let smallest = i;
      const heapAt = (j: number) => this.heap[j];
      if (l < this.heap.length) {
        const cl = heapAt(l);
        const cs = heapAt(smallest);
        if (cl && cs && this.less(cl, cs)) smallest = l;
      }
      if (r < this.heap.length) {
        const cr = heapAt(r);
        const cs = heapAt(smallest);
        if (cr && cs && this.less(cr, cs)) smallest = r;
      }
      if (smallest === i) return;
      const a = this.heap[i];
      const b = this.heap[smallest];
      if (!a || !b) return;
      this.heap[i] = b;
      this.heap[smallest] = a;
      i = smallest;
    }
  }
}
