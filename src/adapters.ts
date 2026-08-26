// Storage adapter for better-sqlite3-shaped drivers (SPEC §14 SqliteHandle).
// Structural typing only — the host constructs the Database and hands it over,
// so the core keeps zero native dependencies.

import type { SqliteHandle } from "./types.js";

export interface BetterSqlite3Like {
  prepare(sql: string): {
    reader: boolean;
    run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
    get(...params: unknown[]): unknown;
    all(...params: unknown[]): unknown[];
  };
  transaction<T>(fn: () => T): () => T;
}

export function betterSqlite3Handle(db: BetterSqlite3Like): SqliteHandle {
  let owned = false;
  return {
    run(sql, params = []) {
      const stmt = db.prepare(sql);
      if (stmt.reader) {
        stmt.all(...params);
        return { changes: 0, lastInsertRowid: 0 };
      }
      const r = stmt.run(...params);
      return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
    },
    get<T>(sql: string, params: unknown[] = []) {
      return db.prepare(sql).get(...params) as T | undefined;
    },
    all<T>(sql: string, params: unknown[] = []) {
      return db.prepare(sql).all(...params) as T[];
    },
    transaction(fn) {
      return db.transaction(fn)();
    },
    // §8 single-process ownership: in-process reentry guard; cross-process
    // exclusion comes from SQLite's own locking under WAL
    acquireOwnerLock() {
      if (owned) throw new Error("DB already owned by this process");
      owned = true;
    },
    releaseOwnerLock() {
      owned = false;
    },
  };
}
