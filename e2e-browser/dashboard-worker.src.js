// The dashboard node lives ENTIRELY in this worker — sqlite-wasm storage
// (OPFS-persistent when available), the WebSocket, sync, views, subscriptions.
// The main thread only renders. This is the non-blocking deployment shape the
// host-guide prescribes for browsers.
import {
  createSeqscribe,
  sqliteWasmHandle,
  webSocketChannel,
  loadOrCreateWriterId,
  coreOf,
} from "../src/index.ts";

const post = (m) => self.postMessage(m);

try {
  const { default: sqlite3InitModule } = await import(
    "/@sqlite.org/sqlite-wasm/dist/index.mjs"
  );
  const sqlite3 = await sqlite3InitModule();

  // OPFS SAH-pool VFS: persistent, worker-only, no COOP/COEP needed
  let db, vfs;
  try {
    const pool = await sqlite3.installOpfsSAHPoolVfs({});
    db = new pool.OpfsSAHPoolDb("/seqscribe-dashboard.db");
    vfs = "opfs-sahpool";
  } catch {
    db = new sqlite3.oo1.DB(":memory:");
    vfs = "memory";
  }

  const storage = sqliteWasmHandle(db);
  const writerId = loadOrCreateWriterId(storage, { prefix: "dash" });
  const node = createSeqscribe({
    writerId,
    storage,
    constants: { GROUP_COMMIT_MS: 5, CONTROL_RETRY_MS: 250, ANTI_ENTROPY_MS: 5_000 },
  });
  node.defineTopic("mesh.events", { kind: "append", retention: { mode: "full" }, replication: "full-sync", access: "metadata" });
  node.defineTopic("fleet.status", { kind: "append", retention: { mode: "ring", size: 50 }, replication: "subscribe-only", access: "metadata" });
  node.defineTopic("config.settings", { kind: "register", retention: { mode: "full" }, replication: "full-sync", access: "metadata" });

  // persistence proof: what the LOCAL replica already holds before this sync
  const resumedFromSeq = coreOf(node).getStream("mesh.events", "coordinator").contigSeq;

  // the ledger is materialized LOCALLY over the full-synced replica —
  // offline-readable, exactly the DESIGN §1 dashboard shape
  const ledger = node.view("ledger", "mesh.events", {
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

  const peer = node.attach(webSocketChannel(new WebSocket(`ws://${location.host}`)), {
    peerId: "coordinator",
    peerClass: "content",
    grants: { "mesh.events": "full", "fleet.status": "none", "config.settings": "full" },
  });

  // Tier-2 subscriptions over the same socket
  let fleetRows = new Map();
  const fleetSub = node.subscribe(peer, { view: "tail", params: { topic: "fleet.status" } });
  fleetSub.onSnapshot((rows, reset) => {
    if (reset) fleetRows = new Map();
    for (const r of rows) fleetRows.set(r.key, r);
  });
  fleetSub.onDelta((c) => {
    for (const r of c.upserts) fleetRows.set(r.key, r);
  });

  let configRows = [];
  const cfgSub = node.subscribe(peer, { view: "register", params: { topic: "config.settings" } });
  cfgSub.onSnapshot((rows) => (configRows = rows));

  const table = node._views.get("ledger").table;
  const tick = () => {
    const meshContig = coreOf(node).getStream("mesh.events", "coordinator").contigSeq;
    const ledgerRows = ledger.query(
      `SELECT * FROM "${table}" ORDER BY task DESC LIMIT 12`,
    );
    const latestByMachine = new Map();
    for (const r of [...fleetRows.values()].sort((a, b) => a.seq - b.seq)) {
      latestByMachine.set(JSON.parse(r.payload).machine, r);
    }
    post({
      type: "state",
      vfs,
      writerId,
      resumedFromSeq,
      meshContig,
      ledgerRows,
      fleet: [...latestByMachine.entries()].map(([m, r]) => ({ machine: m, payload: JSON.parse(r.payload) })),
      config: configRows.map((r) => ({ key: r.key, value: r.value })),
    });
  };
  setInterval(tick, 250);
  tick();

  // heavy-work proof: hammer the local replica with queries — main thread
  // must stay smooth because all of this happens here, in the worker
  setTimeout(() => {
    const t0 = performance.now();
    let n = 0;
    for (let i = 0; i < 400; i++) n += ledger.query(`SELECT COUNT(*) AS c FROM "${table}"`)[0].c;
    post({ type: "heavy", heavyMs: Math.round(performance.now() - t0), sampled: n });
  }, 4_000);
} catch (err) {
  post({ type: "error", error: String((err && err.stack) || err) });
}
