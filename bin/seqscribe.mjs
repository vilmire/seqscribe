#!/usr/bin/env node
// SPEC §17 — thin CLI: export | import | beacon | inspect.
// inspect/export open the DB read-only (WAL-aware; never immutable=1 on a live
// WAL DB); import runs a real node so chains verify through the ordinary path.

import { createInterface } from "node:readline";
import { createServer } from "node:http";

const [cmd, ...args] = process.argv.slice(2);

function usage(code = 1) {
  process.stderr.write(
    `usage:
  seqscribe export <db-path> <topic>            JSONL to stdout
  seqscribe import <db-path> <topic> [--register]   JSONL from stdin
  seqscribe inspect <db-path>                   summary of topics/writers/state
  seqscribe beacon [port]                       reference beacon server (default 8787)
`,
  );
  process.exit(code);
}

async function openDb(path, readonly) {
  let Database;
  try {
    ({ default: Database } = await import("better-sqlite3"));
  } catch {
    process.stderr.write("better-sqlite3 is required for this command: npm i better-sqlite3\n");
    process.exit(1);
  }
  return new Database(path, readonly ? { readonly: true } : {});
}

async function loadLib() {
  return import("../dist/index.js");
}

function handleFor(db) {
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
    get: (sql, params = []) => db.prepare(sql).get(...params),
    all: (sql, params = []) => db.prepare(sql).all(...params),
    transaction: (fn) => db.transaction(fn)(),
    acquireOwnerLock() {
      if (owned) throw new Error("DB already owned");
      owned = true;
    },
    releaseOwnerLock() {
      owned = false;
    },
  };
}

async function cmdExport([path, topic]) {
  if (!path || !topic) usage();
  const db = await openDb(path, true);
  const cert = db
    .prepare("SELECT cert FROM sq_finality WHERE topic = ?")
    .get(topic);
  let base = "genesis";
  if (cert) {
    const c = JSON.parse(cert.cert);
    const writers = db
      .prepare("SELECT DISTINCT writer FROM sq_log WHERE topic = ?")
      .all(topic);
    for (const { writer } of writers) {
      const first = db
        .prepare("SELECT seq FROM sq_log WHERE topic=? AND writer=? ORDER BY seq LIMIT 1")
        .get(topic, writer);
      if (first && first.seq > 1) {
        base = { order: c.order, cut: c.cut };
        break;
      }
    }
  }
  process.stdout.write(JSON.stringify({ seqscribe: "export/v1", topic, base }) + "\n");
  const rows = db
    .prepare("SELECT * FROM sq_log WHERE topic = ? ORDER BY rowid")
    .all(topic);
  for (const r of rows) {
    const entry = {
      topic: r.topic,
      writer: r.writer,
      seq: r.seq,
      hlc: { l: r.hlc_l, c: r.hlc_c },
      kind: r.kind,
      payload: JSON.parse(r.payload),
      chain: r.chain,
    };
    if (r.key !== null) entry.key = r.key;
    if (r.causal_w !== null) entry.causal = [r.causal_w, r.causal_s];
    if (r.ref_t !== null) entry.ref = [r.ref_t, r.ref_w, r.ref_s];
    process.stdout.write(JSON.stringify(entry) + "\n");
  }
}

async function cmdImport([path, topic, flag]) {
  if (!path || !topic) usage();
  const lib = await loadLib();
  const db = await openDb(path, false);
  const node = lib.createSeqscribe({ writerId: "cli.import", storage: handleFor(db) });
  node.defineTopic(topic, {
    kind: flag === "--register" ? "register" : "append",
    retention: { mode: "full" },
    replication: "full-sync",
    access: "content",
  });
  const rl = createInterface({ input: process.stdin });
  const applied = await node.import(
    topic,
    (async function* () {
      for await (const line of rl) yield line;
    })(),
  );
  await node.close();
  process.stderr.write(`imported ${applied} entries into ${topic}\n`);
}

async function cmdInspect([path]) {
  if (!path) usage();
  const db = await openDb(path, true);
  const writers = db.prepare("SELECT * FROM sq_writers ORDER BY topic, writer").all();
  const out = {};
  for (const w of writers) {
    const t = (out[w.topic] ??= { writers: {}, finalityGeneration: null });
    t.writers[w.writer] = {
      contig: w.contig_seq,
      sealed: w.seal_reason,
      rgen: w.rgen,
    };
  }
  for (const f of db.prepare("SELECT topic, cert FROM sq_finality").all()) {
    (out[f.topic] ??= { writers: {}, finalityGeneration: null }).finalityGeneration =
      JSON.parse(f.cert).generation;
  }
  const counts = {
    log: db.prepare("SELECT COUNT(*) n FROM sq_log").get().n,
    pending: db.prepare("SELECT COUNT(*) n FROM sq_pending").get().n,
    quarantine: db.prepare("SELECT COUNT(*) n FROM sq_quarantine").get().n,
    checkpoints: db.prepare("SELECT COUNT(*) n FROM sq_checkpoints").get().n,
  };
  process.stdout.write(JSON.stringify({ topics: out, counts }, null, 2) + "\n");
}

// Reference beacon (§14 wire): a content-free vector board, tens of lines.
function cmdBeacon([port]) {
  const board = new Map(); // account → node → report
  const server = createServer((req, res) => {
    const m = /^\/v1\/a\/([^/]+)\/vectors$/.exec(req.url ?? "");
    if (!m) {
      res.writeHead(404).end();
      return;
    }
    const account = m[1];
    if (req.method === "GET") {
      const reports = [...(board.get(account)?.values() ?? [])];
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(reports));
      return;
    }
    if (req.method === "POST") {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        try {
          const report = JSON.parse(body);
          if (typeof report.node !== "string") throw new Error("bad report");
          const acct = board.get(account) ?? new Map();
          acct.set(report.node, report); // one report per node, overwrite
          board.set(account, acct);
          res.writeHead(204).end();
        } catch {
          res.writeHead(400).end();
        }
      });
      return;
    }
    res.writeHead(405).end();
  });
  const p = Number(port ?? 8787);
  server.listen(p, () => process.stderr.write(`seqscribe beacon on :${p}\n`));
}

switch (cmd) {
  case "export":
    await cmdExport(args);
    break;
  case "import":
    await cmdImport(args);
    break;
  case "inspect":
    await cmdInspect(args);
    break;
  case "beacon":
    cmdBeacon(args);
    break;
  default:
    usage(cmd ? 1 : 0);
}
