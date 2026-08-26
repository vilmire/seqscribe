// better-sqlite3 adapter for the harness — the core structural adapter plus a
// typed constructor. Real SQLite (not a JS map) so the §8 DDL, UNIQUE behavior,
// and rowid semantics are the ones actually exercised (docs/harness.md §2).

import Database from "better-sqlite3";
import { betterSqlite3Handle } from "../src/adapters.js";
import type { SqliteHandle } from "../src/types.js";

export function sqliteHandle(db: Database.Database): SqliteHandle {
  return betterSqlite3Handle(db);
}

export function memoryHandle(): SqliteHandle {
  return betterSqlite3Handle(new Database(":memory:"));
}

export function fileHandle(path: string): SqliteHandle {
  return betterSqlite3Handle(new Database(path), new Database(`${path}.lock`));
}
