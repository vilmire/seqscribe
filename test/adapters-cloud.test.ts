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
