// Browser-e2e server: static files + a real seqscribe node behind a
// WebSocketServer, plus /state for assertions. Run: node e2e-browser/server.mjs [port]
import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { join, dirname, extname } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import Database from "better-sqlite3";
import { createSeqscribe, betterSqlite3Handle, webSocketChannel } from "../dist/index.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, "..");
const T = "e2e.browser";
const POLICY = { kind: "append", retention: { mode: "full" }, replication: "full-sync", access: "content" };

const node = createSeqscribe({
  writerId: "server-w",
  storage: betterSqlite3Handle(new Database(":memory:")),
  constants: { GROUP_COMMIT_MS: 5, CONTROL_RETRY_MS: 250, ANTI_ENTROPY_MS: 2_000 },
});
node.defineTopic(T, POLICY);
// a served view for the Tier-2 subscription path (the dashboard shape)
node.view("counts", T, {
  version: "1",
  init: {},
  reduce: (s, e) => ({ ...s, [e.kind]: (s[e.kind] ?? 0) + 1 }),
  rows: (s) => Object.entries(s).map(([kind, n]) => ({ kind, n })),
  rowKey: "kind",
  schema: { kind: "TEXT", n: "INTEGER" },
  delta: (s, e) => ({ upserts: [{ kind: e.kind, n: (s[e.kind] ?? 0) + 1 }], deletes: [] }),
});
// backlog the browser must pull
for (let i = 0; i < 10; i++) node.log(T).append("server-note", { i });

const MIME = { ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript", ".wasm": "application/wasm", ".json": "application/json" };
const server = createServer((req, res) => {
  const path = new URL(req.url, "http://x").pathname;
  if (path === "/state") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ vectors: node.vectors(), stats: node.stats() }));
    return;
  }
  const file = path === "/" ? "/index.html" : path;
  const candidates = [join(HERE, file), join(ROOT, "node_modules", file.slice(1))];
  for (const c of candidates) {
    try {
      const body = readFileSync(c);
      res.writeHead(200, { "content-type": MIME[extname(c)] ?? "application/octet-stream" });
      res.end(body);
      return;
    } catch { /* try next */ }
  }
  res.writeHead(404).end();
});

const wss = new WebSocketServer({ server });
let n = 0;
wss.on("connection", (sock) => {
  node.attach(webSocketChannel(sock), {
    peerId: `browser-${n++}`,
    peerClass: "content",
    grants: { [T]: "full" },
  });
});

const port = Number(process.argv[2] ?? 8971);
server.listen(port, () => console.log(`browser-e2e server on http://127.0.0.1:${port}`));
