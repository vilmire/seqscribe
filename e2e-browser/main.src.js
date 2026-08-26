// Runs a REAL seqscribe node in the browser: sqlite-wasm storage + a real
// WebSocket channel to the Node server. Renders machine-readable results.
import { createSeqscribe, sqliteWasmHandle, webSocketChannel, coreOf } from "../src/index.ts";

// served statically by server.mjs out of node_modules; sqlite3.mjs locates its
// .wasm sibling relative to its own URL
const { default: sqlite3InitModule } = await import(
  /* @vite-ignore */ "/@sqlite.org/sqlite-wasm/dist/index.mjs"
);

const T = "e2e.browser";
const status = (m) => (document.getElementById("status").textContent = m);
const result = (obj) => (document.getElementById("result").textContent = JSON.stringify(obj));

try {
  status("initializing sqlite-wasm…");
  const sqlite3 = await sqlite3InitModule();
  const db = new sqlite3.oo1.DB(":memory:");

  status("creating node…");
  // in-memory DB = a fresh identity per page load; reusing a fixed writerId
  // across reloads would be a fork by construction (host-guide §1)
  const writerId = "browser-" + [...crypto.getRandomValues(new Uint8Array(4))].map((b) => b.toString(16).padStart(2, "0")).join("");
  const node = createSeqscribe({
    writerId,
    storage: sqliteWasmHandle(db),
    constants: { GROUP_COMMIT_MS: 5, CONTROL_RETRY_MS: 250, ANTI_ENTROPY_MS: 2_000 },
  });
  node.defineTopic(T, { kind: "append", retention: { mode: "full" }, replication: "full-sync", access: "content" });

  // local pipeline sanity in-browser: view over the topic
  const view = node.view("counts", T, {
    version: "1",
    init: {},
    reduce: (s, e) => ({ ...s, [e.kind]: (s[e.kind] ?? 0) + 1 }),
    rows: (s) => Object.entries(s).map(([kind, n]) => ({ kind, n })),
    rowKey: "kind",
    schema: { kind: "TEXT", n: "INTEGER" },
  });

  status("dialing server…");
  const ws = new WebSocket(`ws://${location.host}`);
  const peer = node.attach(webSocketChannel(ws), {
    peerId: "server",
    peerClass: "content",
    grants: { [T]: "full" },
  });

  // browser-authored entries the server must pull
  for (let i = 0; i < 5; i++) node.log(T).append("browser-note", { i });

  status("syncing…");
  const deadline = Date.now() + 15_000;
  const converged = () =>
    coreOf(node).getStream(T, "server-w").contigSeq === 10 &&
    coreOf(node).getStream(T, writerId).contigSeq === 5;
  while (!converged() && Date.now() < deadline) await new Promise((r) => setTimeout(r, 100));

  await new Promise((r) => setTimeout(r, 300)); // let the view materialize
  const table = node._views.get("counts").table;
  const rows = view.query(`SELECT * FROM "${table}" ORDER BY kind`);

  // Tier-2 path: subscribe to the SERVER's view over the same socket (the
  // dashboard shape — SNAP snapshot, then live DELTAs)
  status("subscribing…");
  const sub = node.subscribe(peer, { view: "counts", params: null });
  let subRows = null;
  let deltas = 0;
  sub.onSnapshot((r2) => (subRows = r2));
  sub.onDelta(() => deltas++);
  const subDeadline = Date.now() + 10_000;
  while (subRows === null && Date.now() < subDeadline) await new Promise((r) => setTimeout(r, 100));

  result({
    ok: converged(),
    writerId,
    serverContig: coreOf(node).getStream(T, "server-w").contigSeq,
    ownContig: coreOf(node).getStream(T, writerId).contigSeq,
    serverChain: coreOf(node).getStream(T, "server-w").contigChain,
    viewRows: rows,
    subRows: (subRows ?? []).sort((a, b) => (a.kind < b.kind ? -1 : 1)),
    subCursor: typeof sub.cursor === "string",
    stats: node.stats().topics[T],
  });
  status(converged() ? "CONVERGED" : "TIMEOUT");
} catch (err) {
  status("ERROR");
  result({ ok: false, error: String(err && err.stack || err) });
}
