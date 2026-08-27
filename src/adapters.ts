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

// Optional second connection used purely as a cross-process mutex: holding
// BEGIN EXCLUSIVE on `<db>.lock` takes an OS-level file lock that dies with
// the process — a crashed owner never wedges the DB (§8 single-process MUST).
export interface LockDbLike {
  exec(sql: string): unknown;
  close(): void;
}

export function betterSqlite3Handle(db: BetterSqlite3Like, lockDb?: LockDbLike): SqliteHandle {
  let owned = false;
  // Statement cache (proposals-v3.5 P23): every store operation used to
  // re-prepare its SQL — Statement::JS_new was a visible fraction of the P22
  // incident's per-round cost, and it taxes every healthy hot path too. SQLite
  // prepared statements (v2 semantics) recompile themselves on schema change,
  // so the cache needs no invalidation hook. The SQL census is finite (fixed
  // store paths + per-topic/view table names); the cap is a guard against
  // pathological dynamic SQL, not an LRU — a full clear just re-prepares.
  const stmts = new Map<string, ReturnType<BetterSqlite3Like["prepare"]>>();
  const prep = (sql: string) => {
    let s = stmts.get(sql);
    if (s === undefined) {
      if (stmts.size >= 512) stmts.clear();
      s = db.prepare(sql);
      stmts.set(sql, s);
    }
    return s;
  };
  return {
    run(sql, params = []) {
      const stmt = prep(sql);
      if (stmt.reader) {
        stmt.all(...params);
        return { changes: 0, lastInsertRowid: 0 };
      }
      const r = stmt.run(...params);
      return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
    },
    get<T>(sql: string, params: unknown[] = []) {
      return prep(sql).get(...params) as T | undefined;
    },
    all<T>(sql: string, params: unknown[] = []) {
      return prep(sql).all(...params) as T[];
    },
    transaction(fn) {
      return db.transaction(fn)();
    },
    // idempotent per handle (proposals-v3.5 P5): the library takes this lock
    // in Store.init, and a host that also called it must not turn every
    // successful open into a false "owned" error — real cross-process
    // conflicts surface through the lock file, not the in-process flag
    acquireOwnerLock() {
      if (owned) return;
      if (lockDb) {
        try {
          lockDb.exec("BEGIN EXCLUSIVE");
        } catch {
          throw new Error("DB owned by another process (lock file held)");
        }
      }
      owned = true;
    },
    releaseOwnerLock() {
      if (owned && lockDb) {
        try {
          lockDb.exec("ROLLBACK");
        } catch {
          // lock connection already gone
        }
      }
      owned = false;
    },
  };
}

// ---- SQLite WASM (browser) ----
//
// Adapter for the official @sqlite.org/sqlite-wasm oo1.DB API. Synchronous, so
// in a browser it must run in a worker with OPFS sync access handles (or an
// in-memory DB for pure Tier-2 subscribers). Transactions are managed with
// explicit BEGIN/SAVEPOINT so nesting behaves like the better-sqlite3 adapter.

export interface SqliteWasmDbLike {
  exec(opts: { sql: string; bind?: unknown[] }): unknown;
  selectObjects(sql: string, bind?: unknown[]): Record<string, unknown>[];
  selectValue(sql: string, bind?: unknown[]): unknown;
}

const READER_RE = /^\s*(select|pragma|with|explain)\b/i;

export function sqliteWasmHandle(db: SqliteWasmDbLike): SqliteHandle {
  let owned = false;
  let depth = 0;
  return {
    run(sql, params = []) {
      if (READER_RE.test(sql)) {
        db.selectObjects(sql, params);
        return { changes: 0, lastInsertRowid: 0 };
      }
      db.exec({ sql, bind: params });
      const rowid = db.selectValue("SELECT last_insert_rowid()");
      return { changes: 0, lastInsertRowid: Number(rowid ?? 0) };
    },
    get<T>(sql: string, params: unknown[] = []) {
      return db.selectObjects(sql, params)[0] as T | undefined;
    },
    all<T>(sql: string, params: unknown[] = []) {
      return db.selectObjects(sql, params) as T[];
    },
    transaction<T>(fn: () => T): T {
      const name = `sq_sp_${depth}`;
      db.exec({ sql: depth === 0 ? "BEGIN" : `SAVEPOINT ${name}` });
      depth++;
      try {
        const out = fn();
        depth--;
        db.exec({ sql: depth === 0 ? "COMMIT" : `RELEASE ${name}` });
        return out;
      } catch (e) {
        depth--;
        db.exec({ sql: depth === 0 ? "ROLLBACK" : `ROLLBACK TO ${name}; RELEASE ${name}` });
        throw e;
      }
    },
    // browsers: one tab/worker owns the DB; OPFS sync access handles already
    // enforce single-connection exclusivity at the file layer. Idempotent per
    // handle (P5).
    acquireOwnerLock() {
      owned = true;
    },
    releaseOwnerLock() {
      owned = false;
    },
  };
}

// ---- Cloudflare Durable Object SQLite ----
//
// Adapter for ctx.storage.sql (+ transactionSync). PRAGMAs are not supported
// by DO SQLite and are no-ops here; a DO is single-threaded and owns its
// storage exclusively, so the owner lock is the in-process guard only.

export interface DurableObjectSqlLike {
  exec(query: string, ...bindings: unknown[]): { toArray(): Record<string, unknown>[] };
}

export function durableObjectSqlHandle(
  sql: DurableObjectSqlLike,
  transactionSync?: <T>(fn: () => T) => T,
): SqliteHandle {
  let owned = false;
  let depth = 0;
  return {
    run(query, params = []) {
      if (/^\s*pragma\b/i.test(query)) return { changes: 0, lastInsertRowid: 0 };
      if (READER_RE.test(query)) {
        sql.exec(query, ...params).toArray();
        return { changes: 0, lastInsertRowid: 0 };
      }
      sql.exec(query, ...params).toArray();
      const row = sql.exec("SELECT last_insert_rowid() AS id").toArray()[0];
      return { changes: 0, lastInsertRowid: Number(row?.id ?? 0) };
    },
    get<T>(query: string, params: unknown[] = []) {
      return sql.exec(query, ...params).toArray()[0] as T | undefined;
    },
    all<T>(query: string, params: unknown[] = []) {
      return sql.exec(query, ...params).toArray() as T[];
    },
    transaction<T>(fn: () => T): T {
      if (!transactionSync || depth > 0) {
        // nested (or no txn API): fold into the outer scope — DO storage is
        // single-threaded synchronous, so atomicity holds at the outer boundary
        depth++;
        try {
          return fn();
        } finally {
          depth--;
        }
      }
      depth++;
      try {
        return transactionSync(fn);
      } finally {
        depth--;
      }
    },
    // a DO is single-threaded and owns its storage exclusively; idempotent (P5)
    acquireOwnerLock() {
      owned = true;
    },
    releaseOwnerLock() {
      owned = false;
    },
  };
}
