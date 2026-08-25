// better-sqlite3 adapter implementing the SPEC §14 SqliteHandle. Real SQLite
// (not a JS map) so the §8 DDL, UNIQUE behavior, and rowid semantics are the
// ones actually exercised (docs/harness.md §2).

import Database from "better-sqlite3";
import type { SqliteHandle } from "../src/types.js";

export function sqliteHandle(db: Database.Database): SqliteHandle {
  let owned = false;
  return {
    run(sql, params = []) {
      const stmt = db.prepare(sql);
      if (stmt.reader) {
        stmt.all(...(params as never[]));
        return { changes: 0, lastInsertRowid: 0 };
      }
      const r = stmt.run(...(params as never[]));
      return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
    },
    get(sql, params = []) {
      return db.prepare(sql).get(...(params as never[])) as never;
    },
    all(sql, params = []) {
      return db.prepare(sql).all(...(params as never[])) as never[];
    },
    transaction(fn) {
      return db.transaction(fn)();
    },
    acquireOwnerLock() {
      if (owned) throw new Error("DB already owned by this process");
      owned = true;
    },
    releaseOwnerLock() {
      owned = false;
    },
  };
}

export function memoryHandle(): SqliteHandle {
  return sqliteHandle(new Database(":memory:"));
}
