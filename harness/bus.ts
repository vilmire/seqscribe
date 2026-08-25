// Virtual channel pair with a per-link fault model (docs/harness.md §5).
// The Channel contract guarantees neither delivery nor ordering (§5.1): loss,
// duplication, and reordering happen inside a live link without closing it.

import type { Channel } from "../src/types.js";
import type { SeededRng } from "./rng.js";
import type { Scheduler } from "./scheduler.js";

export interface LinkFaults {
  latency: () => number; // per-message delay draw (reordering emerges from jitter)
  lossP: number;
  dupP: number;
}

interface Side {
  onMessage: ((m: string) => void) | null;
  onClose: (() => void) | null;
  closed: boolean;
}

export class VirtualLink {
  readonly a: Channel;
  readonly b: Channel;
  private cutNow = false;
  private readonly sides: [Side, Side] = [
    { onMessage: null, onClose: null, closed: false },
    { onMessage: null, onClose: null, closed: false },
  ];

  constructor(
    private readonly sched: Scheduler,
    rng: SeededRng,
    faults?: Partial<LinkFaults>,
  ) {
    const lr = rng.substream("link");
    const f: LinkFaults = {
      latency: faults?.latency ?? (() => 1 + Math.floor(lr.next() * 4)),
      lossP: faults?.lossP ?? 0,
      dupP: faults?.dupP ?? 0,
    };
    this.a = this.makeSide(0, 1, f, lr.substream("a2b"));
    this.b = this.makeSide(1, 0, f, lr.substream("b2a"));
  }

  cut(on: boolean): void {
    this.cutNow = on;
  }

  private makeSide(me: 0 | 1, other: 0 | 1, f: LinkFaults, rng: SeededRng): Channel {
    const sides = this.sides;
    const deliver = (msg: string) => {
      const dst = sides[other];
      if (!dst.closed && dst.onMessage) dst.onMessage(msg);
    };
    return {
      send: (msg: string) => {
        if (sides[me].closed || sides[other].closed || this.cutNow) return;
        if (rng.next() < f.lossP) return;
        this.sched.schedule(this.sched.now() + f.latency(), () => deliver(msg));
        if (rng.next() < f.dupP)
          this.sched.schedule(this.sched.now() + f.latency(), () => deliver(msg));
      },
      onMessage: (cb) => {
        sides[me].onMessage = cb;
      },
      onClose: (cb) => {
        sides[me].onClose = cb;
      },
      close: () => {
        if (sides[me].closed) return;
        sides[me].closed = true;
        const peer = sides[other];
        if (!this.cutNow) {
          // close propagates unless the link is cut (a cut partition is silent)
          this.sched.schedule(this.sched.now() + f.latency(), () => {
            if (!peer.closed) {
              peer.closed = true;
              peer.onClose?.();
            }
          });
        }
      },
    };
  }
}
