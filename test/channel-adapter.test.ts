// proposals-v3.5 P4/P5 regression: transport-shape generality of the socket
// channel adapter, pre-open ordering/bounding, and idempotent owner locks.

import { describe, expect, it } from "vitest";
import { betterSqlite3Handle, createSeqscribe, webSocketChannel } from "../src/index.js";
import { memoryHandle } from "../harness/sqlite.js";
import Database from "better-sqlite3";
import type { WebSocketLike } from "../src/index.js";

type Listener = (ev: { data: unknown }) => void;

function fakeSocket(shape: "ws" | "rtc" | "predicate") {
  const sent: string[] = [];
  const listeners = new Map<string, ((ev: never) => void)[]>();
  let open = false;
  const sock = {
    ...(shape === "ws" ? { readyState: 0 } : {}),
    ...(shape === "rtc" ? { readyState: "connecting" } : {}),
    ...(shape === "predicate" ? { isOpen: () => open } : {}),
    send: (d: string) => sent.push(d),
    close: () => {},
    addEventListener: (t: string, cb: (ev: never) => void) => {
      const l = listeners.get(t) ?? [];
      l.push(cb);
      listeners.set(t, l);
    },
  } as unknown as WebSocketLike & { readyState?: number | string };
  return {
    sock,
    sent,
    open() {
      open = true;
      if (shape === "ws") (sock as { readyState?: number }).readyState = 1;
      if (shape === "rtc") (sock as { readyState?: string }).readyState = "open";
      for (const cb of listeners.get("open") ?? []) (cb as () => void)();
    },
    openSilently() {
      // state flips but no "open" event fires (RTCDataChannel opened before wiring)
      open = true;
      if (shape === "ws") (sock as { readyState?: number }).readyState = 1;
      if (shape === "rtc") (sock as { readyState?: string }).readyState = "open";
    },
    emit(data: string) {
      for (const cb of listeners.get("message") ?? []) (cb as Listener)({ data });
    },
  };
}

describe("socket channel adapter shapes (P4)", () => {
  for (const shape of ["ws", "rtc", "predicate"] as const) {
    it(`sends immediately on an open ${shape}-shaped transport`, () => {
      const f = fakeSocket(shape);
      f.openSilently();
      const ch = webSocketChannel(f.sock);
      ch.send("hello");
      expect(f.sent).toEqual(["hello"]);
    });

    it(`buffers pre-open frames on ${shape} and flushes them IN ORDER`, () => {
      const f = fakeSocket(shape);
      const ch = webSocketChannel(f.sock);
      ch.send("a");
      ch.send("b");
      expect(f.sent).toEqual([]); // nothing leaks before open
      if (shape === "predicate") {
        // no "open" event exists for predicate wrappers — the next send flushes
        f.openSilently();
        ch.send("c");
      } else {
        f.open();
        ch.send("c");
      }
      expect(f.sent).toEqual(["a", "b", "c"]); // buffered frames lead, in order
    });
  }

  it("flushes via state polling even when the open event fired before wiring", () => {
    const f = fakeSocket("rtc");
    const ch = webSocketChannel(f.sock);
    ch.send("early");
    f.openSilently(); // event lost — state is the only signal
    ch.send("late");
    expect(f.sent).toEqual(["early", "late"]);
  });

  it("bounds the pre-open buffer instead of growing without limit", () => {
    const f = fakeSocket("rtc");
    const ch = webSocketChannel(f.sock);
    for (let i = 0; i < 2_000; i++) ch.send(`m${i}`);
    f.open();
    ch.send("last");
    expect(f.sent.length).toBe(1_024 + 1); // cap kept, overflow dropped, tail delivered
    expect(f.sent[0]).toBe("m0");
    expect(f.sent[f.sent.length - 1]).toBe("last");
  });
});

describe("idempotent owner lock (P5)", () => {
  it("a host's own acquireOwnerLock before createSeqscribe is harmless", () => {
    const storage = betterSqlite3Handle(new Database(":memory:"));
    storage.acquireOwnerLock(); // diligent-host pattern that used to self-destruct
    const node = createSeqscribe({ writerId: "wA", storage });
    node.defineTopic("t.x", {
      kind: "append",
      retention: { mode: "full" },
      replication: "full-sync",
      access: "content",
    });
    void node.close();
  });

  it("still refuses a genuinely second process (lock file held elsewhere)", () => {
    const { mkdtempSync } = require("node:fs") as typeof import("node:fs");
    const { tmpdir } = require("node:os") as typeof import("node:os");
    const { join } = require("node:path") as typeof import("node:path");
    const dir = mkdtempSync(join(tmpdir(), "seqscribe-lock-"));
    const path = join(dir, "x.db");
    const mk = () =>
      betterSqlite3Handle(new Database(path), new Database(`${path}.lock`));
    const first = createSeqscribe({ writerId: "wA", storage: mk() });
    expect(() => createSeqscribe({ writerId: "wA", storage: mk() })).toThrowError(/ERR_DB_OWNED/);
    void first.close();
  });

  it("memoryHandle double-acquire is a no-op too", () => {
    const h = memoryHandle();
    h.acquireOwnerLock();
    h.acquireOwnerLock();
    h.releaseOwnerLock();
  });
});
