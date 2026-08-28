// Browser and cloud storage adapters running the REAL node:
// - sqliteWasmHandle on the official @sqlite.org/sqlite-wasm build (the same
//   engine a browser worker runs — executed here under Node's wasm runtime)
// - durableObjectSqlHandle against a DO-shaped sql.exec shim
// - beaconFetchHandler (Workers/DO-deployable reference beacon)

import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";
import { Scheduler } from "../harness/scheduler.js";
import {
  beaconFetchHandler,
  coreOf,
  createSeqscribe,
  durableObjectSqlHandle,
  sqliteWasmHandle,
} from "../src/index.js";
import type {
  BeaconReport,
  DurableObjectSqlLike,
  SqliteHandle,
  SqliteWasmDbLike,
  TopicPolicy,
  ViewDef,
} from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
const REG: TopicPolicy = {
  kind: "register",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

type CountState = { [kind: string]: number };
const countView: ViewDef<CountState, { kind: string; n: number }> = {
  version: "1",
  init: {},
  reduce: (s, e) => ({ ...s, [e.kind]: (s[e.kind] ?? 0) + 1 }),
  rows: (s) => Object.entries(s).map(([kind, n]) => ({ kind, n })),
  rowKey: "kind",
  schema: { kind: "TEXT", n: "INTEGER" },
};

// the full local pipeline on an arbitrary storage adapter
async function exerciseNode(storage: SqliteHandle): Promise<void> {
  const sched = new Scheduler(0);
  const node = createSeqscribe({
    writerId: "wA",
    storage,
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.5,
  });
  node.defineTopic(T, FULL);
  node.defineTopic("t.cfg", REG);
  const h = node.view("counts", T, countView);

  for (let i = 0; i < 5; i++) void node.log(T).append(i % 2 === 0 ? "a" : "b", { i });
  void node.register("t.cfg").set("theme", "dark");
  await sched.run();

  expect(coreOf(node).getStream(T, "wA").contigSeq).toBe(5);
  const table = (node as unknown as { _views: { get(n: string): { table: string } } })._views.get(
    "counts",
  ).table;
  expect(h.query(`SELECT * FROM "${table}" ORDER BY kind`)).toEqual([
    { kind: "a", n: 3 },
    { kind: "b", n: 2 },
  ]);
  expect(node.ownerOf("t.cfg", "theme")).toBeNull(); // lww topic — no owner
  const regs = (node as unknown as { _registers: { settle(t: string): Promise<void>; snapshotState(t: string): { keys: Record<string, { value?: unknown }> } } })._registers;
  await regs.settle("t.cfg");
  expect(regs.snapshotState("t.cfg").keys.theme?.value).toBe("dark");
  await node.close();
}

describe("sqlite-wasm adapter (browser storage engine)", () => {
  it("runs the full node pipeline on the wasm build", async () => {
    const sqlite3 = await sqlite3InitModule();
    const db = new sqlite3.oo1.DB(":memory:");
    await exerciseNode(sqliteWasmHandle(db as unknown as SqliteWasmDbLike));
  });
});

describe("durable-object adapter", () => {
  it("runs the full node pipeline through a DO-shaped sql.exec", async () => {
    // shim: the DO sql surface backed by real SQLite — validates the adapter's
    // reader detection, rowid recovery, and nested-transaction folding
    const raw = new Database(":memory:");
    const doSql: DurableObjectSqlLike = {
      exec(query, ...bindings) {
        const stmt = raw.prepare(query);
        if (stmt.reader) return { toArray: () => stmt.all(...(bindings as never[])) as never };
        stmt.run(...(bindings as never[]));
        return { toArray: () => [] };
      },
    };
    const transactionSync = <T2>(fn: () => T2): T2 => raw.transaction(fn)();
    await exerciseNode(durableObjectSqlHandle(doSql, transactionSync));
  });
});

describe("beacon fetch handler (Workers/DO reference)", () => {
  const mkReq = (
    method: string,
    path: string,
    body?: unknown,
    token?: string,
  ): Parameters<ReturnType<typeof beaconFetchHandler>>[0] => ({
    method,
    url: `https://beacon.example${path}`,
    headers: { get: (n) => (n === "authorization" && token ? `Bearer ${token}` : null) },
    json: async () => body,
  });

  it("serves the §14 wire with bearer auth", async () => {
    const handler = beaconFetchHandler({ token: "s3cret" });
    expect((await handler(mkReq("GET", "/v1/a/acct/vectors"))).status).toBe(401);
    expect((await handler(mkReq("GET", "/nope", undefined, "s3cret"))).status).toBe(404);

    const report: BeaconReport = { node: "wA", at: "2026-08-26T00:00:00Z", vectors: {} };
    expect((await handler(mkReq("POST", "/v1/a/acct/vectors", report, "s3cret"))).status).toBe(204);
    const got = await handler(mkReq("GET", "/v1/a/acct/vectors", undefined, "s3cret"));
    expect(got.status).toBe(200);
    expect(JSON.parse(got.body)).toEqual([report]);

    // one report per node — overwrite
    const updated = { ...report, at: "2026-08-26T00:01:00Z" };
    await handler(mkReq("POST", "/v1/a/acct/vectors", updated, "s3cret"));
    const got2 = await handler(mkReq("GET", "/v1/a/acct/vectors", undefined, "s3cret"));
    expect(JSON.parse(got2.body)).toEqual([updated]);

    // accounts are isolated
    const other = await handler(mkReq("GET", "/v1/a/other/vectors", undefined, "s3cret"));
    expect(JSON.parse(other.body)).toEqual([]);
  });

  // proposals-v3.5 P26 — a DO hibernates and its isolate is evicted, so the
  // closure board resets to empty on the next wake unless it is backed.
  describe("board persistence (P26)", () => {
    const fakeStore = () => {
      const rows = new Map<string, Record<string, BeaconReport>>();
      let saves = 0;
      return {
        rows,
        saves: () => saves,
        store: {
          load: async (account: string) => rows.get(account) ?? null,
          save: async (account: string, b: Record<string, BeaconReport>) => {
            saves++;
            rows.set(account, b);
          },
        },
      };
    };
    const rep = (node: string, at: string): BeaconReport => ({ node, at, vectors: {} });

    it("survives the handler-instance boundary via the store", async () => {
      const f = fakeStore();
      const h1 = beaconFetchHandler({ store: f.store });
      const a = rep("wA", "2026-08-28T00:00:00Z");
      expect((await h1(mkReq("POST", "/v1/a/acct/vectors", a))).status).toBe(204);

      // the hibernation boundary: a brand-new handler, same store, empty closure
      const h2 = beaconFetchHandler({ store: f.store });
      const got = await h2(mkReq("GET", "/v1/a/acct/vectors"));
      expect(got.status).toBe(200);
      expect(JSON.parse(got.body)).toEqual([a]);

      // and the revived board still merges new reports rather than replacing
      const b = rep("wB", "2026-08-28T00:01:00Z");
      await h2(mkReq("POST", "/v1/a/acct/vectors", b));
      const both = JSON.parse((await h2(mkReq("GET", "/v1/a/acct/vectors"))).body) as BeaconReport[];
      expect(both.map((r) => r.node).sort()).toEqual(["wA", "wB"]);

      // accounts stay isolated across the store too
      expect(JSON.parse((await h2(mkReq("GET", "/v1/a/other/vectors"))).body)).toEqual([]);
    });

    it("is byte-identical to pre-P26 when no store is injected", async () => {
      const h = beaconFetchHandler();
      const a = rep("wA", "2026-08-28T00:00:00Z");
      expect((await h(mkReq("POST", "/v1/a/acct/vectors", a))).status).toBe(204);
      expect(JSON.parse((await h(mkReq("GET", "/v1/a/acct/vectors"))).body)).toEqual([a]);
      // a fresh instance with no store starts empty — the P26 failure mode,
      // still present (and correct) on the unbacked path
      const fresh = beaconFetchHandler();
      expect(JSON.parse((await fresh(mkReq("GET", "/v1/a/acct/vectors"))).body)).toEqual([]);
    });

    it("a save failure changes neither the response nor the board", async () => {
      const a = rep("wA", "2026-08-28T00:00:00Z");
      const h = beaconFetchHandler({
        store: {
          load: async () => null,
          save: async () => {
            throw new Error("storage write failed");
          },
        },
      });
      // still 204 — vectors are advisory, self-healing data (§5.7)
      expect((await h(mkReq("POST", "/v1/a/acct/vectors", a))).status).toBe(204);
      // and this isolate's board is updated regardless of the persistence outcome
      expect(JSON.parse((await h(mkReq("GET", "/v1/a/acct/vectors"))).body)).toEqual([a]);
    });

    it("a load failure degrades to empty rather than failing the request", async () => {
      const h = beaconFetchHandler({
        store: {
          load: async () => {
            throw new Error("storage read failed");
          },
          save: async () => {},
        },
      });
      const got = await h(mkReq("GET", "/v1/a/acct/vectors"));
      expect(got.status).toBe(200);
      expect(JSON.parse(got.body)).toEqual([]);
    });

    it("a POST landing during hydration is not clobbered by the seed", async () => {
      const a = rep("wA", "stored");
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const h = beaconFetchHandler({
        store: {
          load: async () => {
            await gate;
            return { wA: a };
          },
          save: async () => {},
        },
      });
      // POST wA with a NEWER report while the load is still in flight
      const newer = rep("wA", "posted-after");
      const post = h(mkReq("POST", "/v1/a/acct/vectors", newer));
      release();
      expect((await post).status).toBe(204);
      const got = JSON.parse((await h(mkReq("GET", "/v1/a/acct/vectors"))).body) as BeaconReport[];
      expect(got).toEqual([newer]); // the in-flight write wins over the stale seed
    });

    it("concurrent POSTs leave the store holding the NEWEST board", async () => {
      // saves that overlap must not commit out of order: an older snapshot
      // landing last silently drops a node that already got its 204
      const committed: string[][] = [];
      let n = 0;
      const h = beaconFetchHandler({
        store: {
          load: async () => null,
          save: async (_a, b) => {
            // make the FIRST save slow so a naive implementation commits it last
            const delay = n++ === 0 ? 20 : 0;
            await new Promise((r) => setTimeout(r, delay));
            committed.push(Object.keys(b).sort());
          },
        },
      });
      await Promise.all([
        h(mkReq("POST", "/v1/a/acct/vectors", rep("wA", "t1"))),
        h(mkReq("POST", "/v1/a/acct/vectors", rep("wB", "t2"))),
      ]);
      expect(committed.at(-1)).toEqual(["wA", "wB"]); // newest state persisted last
    });

    it("hydrates an account only once per isolate", async () => {
      let loads = 0;
      const h = beaconFetchHandler({
        store: {
          load: async () => {
            loads++;
            return null;
          },
          save: async () => {},
        },
      });
      await Promise.all([
        h(mkReq("GET", "/v1/a/acct/vectors")),
        h(mkReq("GET", "/v1/a/acct/vectors")),
        h(mkReq("POST", "/v1/a/acct/vectors", rep("wA", "t"))),
      ]);
      await h(mkReq("GET", "/v1/a/acct/vectors"));
      expect(loads).toBe(1);
    });
  });
});

describe("browser purity guard", () => {
  it("core sources import no node builtins", async () => {
    const { readFileSync, readdirSync } = await import("node:fs");
    const { join } = await import("node:path");
    const srcDir = join(import.meta.dirname, "..", "src");
    for (const f of readdirSync(srcDir)) {
      if (!f.endsWith(".ts")) continue;
      const body = readFileSync(join(srcDir, f), "utf8");
      expect(body, `${f} must stay platform-agnostic`).not.toMatch(/from "node:/);
      expect(body, `${f} must stay platform-agnostic`).not.toMatch(/require\("node:/);
    }
  });
});
