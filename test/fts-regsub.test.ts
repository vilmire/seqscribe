// FTS mirror tables (§9 fts) and the built-in "register" subscription family.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe } from "../src/index.js";
import type { Constants, Row, SeqscribeNode, TopicPolicy, ViewDef } from "../src/index.js";

const TEST_CONSTANTS: Partial<Constants> = {
  ANTI_ENTROPY_MS: 60_000,
  CONTROL_RETRY_MS: 200,
};

describe("fts views (§9)", () => {
  it("mirrors rows into an FTS5 table and serves MATCH queries", async () => {
    const T = "t.notes";
    const sched = new Scheduler(0);
    const node = createSeqscribe({
      writerId: "wA",
      storage: memoryHandle(),
      clock: sched.clock(),
      timers: sched.timers(),
      rng: () => 0.1,
    });
    node.defineTopic(T, {
      kind: "append",
      retention: { mode: "full" },
      replication: "full-sync",
      access: "content",
    });
    type S = { [id: string]: string };
    const def: ViewDef<S, { id: string; body: string }> = {
      version: "1",
      init: {},
      reduce: (s, e) => ({ ...s, [String(e.seq)]: String((e.payload as { text: string }).text) }),
      rows: (s) => Object.entries(s).map(([id, body]) => ({ id, body })),
      rowKey: "id",
      schema: { id: "TEXT", body: "TEXT" },
      fts: ["body"],
    };
    const h = node.view("notes", T, def);
    void node.log(T).append("note", { text: "the quick brown fox" });
    void node.log(T).append("note", { text: "lazy dogs sleep deeply" });
    void node.log(T).append("note", { text: "quick thinking wins" });
    await sched.run();

    const table = (node as unknown as { _views: { get(n: string): { table: string } } })._views.get(
      "notes",
    ).table;
    const hits = h.query<{ id: string }>(
      `SELECT id FROM "${table}_fts" WHERE "${table}_fts" MATCH 'quick' ORDER BY id`,
    );
    expect(hits.map((r) => r.id)).toEqual(["1", "3"]);
  });
});

describe("built-in register subscription", () => {
  it("serves the register table over SUB with SNAP resets on change", async () => {
    const T = "cfg.settings";
    const POLICY: TopicPolicy = {
      kind: "register",
      retention: { mode: "full" },
      replication: "full-sync",
      access: "content",
    };
    const sched = new Scheduler(0);
    const rng = new SeededRng(81);
    const mk = (w: string) => {
      const n = createSeqscribe({
        writerId: w,
        storage: memoryHandle(),
        clock: sched.clock(),
        timers: sched.timers(),
        rng: () => 0.2,
        constants: TEST_CONSTANTS,
      });
      n.defineTopic(T, POLICY);
      return n;
    };
    const server = mk("wA");
    const client = mk("wB");
    void server.register(T).set("theme", "dark");
    await sched.run({ untilMs: 200 });

    const link = new VirtualLink(sched, rng);
    server.attach(link.a, { peerId: "wB", peerClass: "content", grants: { [T]: "serve" } });
    const handle = client.attach(link.b, {
      peerId: "wA",
      peerClass: "content",
      grants: { [T]: "none" },
    });
    await sched.run({ untilMs: 500 });

    const sub = client.subscribe(handle, { view: "register", params: { topic: T } });
    const snapshots: { rows: Row[]; reset: boolean }[] = [];
    sub.onSnapshot((rows, reset) => snapshots.push({ rows, reset }));
    await sched.run({ untilMs: 900 });

    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]!.rows.map((r) => [r.key, r.value])).toEqual([["theme", '"dark"']]);

    void server.register(T).set("lang", "ko");
    await sched.run({ untilMs: 1_500 });
    expect(snapshots.length).toBeGreaterThanOrEqual(2);
    const last = snapshots[snapshots.length - 1]!;
    expect(last.reset).toBe(true);
    expect(last.rows.map((r) => r.key).sort()).toEqual(["lang", "theme"]);
  });
});
