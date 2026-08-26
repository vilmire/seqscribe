// ADHDev-shaped dashboard demo server (DESIGN §5.5 topic table, scaled down):
//   mesh.events     append / full / full-sync   — the coordinator's event ledger
//   fleet.status    ring(50) / subscribe-only   — live machine status flushes
//   config.settings register / full / full-sync — fleet config with live edits
// The server plays the coordinator daemon; live appends keep flowing so the
// browser dashboard shows real deltas. Run: node e2e-browser/dashboard-server.mjs [port]

import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { join, dirname, extname } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import Database from "better-sqlite3";
import { createSeqscribe, betterSqlite3Handle, webSocketChannel } from "../dist/index.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, "..");

const node = createSeqscribe({
  writerId: "coordinator",
  storage: betterSqlite3Handle(new Database(":memory:")),
  constants: { GROUP_COMMIT_MS: 5, CONTROL_RETRY_MS: 250, ANTI_ENTROPY_MS: 5_000 },
});

node.defineTopic("mesh.events", {
  kind: "append", retention: { mode: "full" }, replication: "full-sync", access: "metadata",
});
node.defineTopic("fleet.status", {
  kind: "append", retention: { mode: "ring", size: 50 }, replication: "subscribe-only", access: "metadata",
});
node.defineTopic("config.settings", {
  kind: "register", retention: { mode: "full" }, replication: "full-sync", access: "metadata",
});

// the ledger view (delta-provided — the recommended shape for growing views)
node.view("ledger", "mesh.events", {
  version: "1",
  init: { tasks: {} },
  reduce: (s, e) => {
    const p = e.payload;
    return { tasks: { ...s.tasks, [p.task]: { task: p.task, status: e.kind, machine: p.machine } } };
  },
  rows: (s) => Object.values(s.tasks),
  rowKey: "task",
  schema: { task: "TEXT", status: "TEXT", machine: "TEXT" },
  delta: (s, e) => {
    const p = e.payload;
    return { upserts: [{ task: p.task, status: e.kind, machine: p.machine }], deletes: [] };
  },
});

// seed config + backlog of events
const reg = node.register("config.settings");
await reg.set("deploy.channel", "stable");
await reg.set("fleet.name", "adhdev-demo");
let task = 0;
const MACHINES = ["m1", "m2", "m3"];
for (let i = 0; i < 40; i++) {
  const t = `task-${String(task++).padStart(3, "0")}`;
  await node.log("mesh.events").append("task_created", { task: t, machine: MACHINES[i % 3] });
  if (i % 2 === 0) await node.log("mesh.events").append("task_done", { task: t, machine: MACHINES[i % 3] });
}

// live traffic: events, status flushes, config edits
setInterval(() => {
  const t = `task-${String(task++).padStart(3, "0")}`;
  const m = MACHINES[task % 3];
  void node.log("mesh.events").append("task_created", { task: t, machine: m });
  setTimeout(() => void node.log("mesh.events").append("task_done", { task: t, machine: m }), 900);
}, 400);
setInterval(() => {
  for (const m of MACHINES)
    void node.log("fleet.status").append("status", { machine: m, cpu: Math.round(Math.random() * 100), at: Date.now() });
}, 500);
setInterval(() => {
  void reg.set("deploy.channel", Math.random() < 0.5 ? "stable" : "canary");
}, 2_000);

const MIME = { ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript", ".wasm": "application/wasm", ".json": "application/json" };
const server = createServer((req, res) => {
  const path = new URL(req.url, "http://x").pathname;
  if (path === "/state") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ vectors: node.vectors(), stats: node.stats() }));
    return;
  }
  const file = path === "/" ? "/dashboard.html" : path;
  for (const c of [join(HERE, file), join(ROOT, "node_modules", file.slice(1))]) {
    try {
      const body = readFileSync(c);
      res.writeHead(200, { "content-type": MIME[extname(c)] ?? "application/octet-stream" });
      res.end(body);
      return;
    } catch { /* next */ }
  }
  res.writeHead(404).end();
});

const wss = new WebSocketServer({ server });
let n = 0;
wss.on("connection", (sock) => {
  node.attach(webSocketChannel(sock), {
    peerId: `dash-${n++}`,
    peerClass: "metadata", // all three topics are metadata-class — a cloud/DO peer could hold this role too
    grants: { "mesh.events": "full", "fleet.status": "serve", "config.settings": "full" },
  });
});

const port = Number(process.argv[2] ?? 8972);
server.listen(port, () => console.log(`dashboard server on http://127.0.0.1:${port}`));
