// End-to-end over REAL infrastructure: actual WebSockets (ws server + the
// global WebSocket client), file-backed SQLite, wall-clock timers — the first
// non-simulated validation, including a process-restart-shaped reopen.

import Database from "better-sqlite3";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocketServer } from "ws";
import { afterAll, describe, expect, it } from "vitest";
import { betterSqlite3Handle, coreOf, createSeqscribe, webSocketChannel } from "../src/index.js";
import type { Constants, SeqscribeNode, TopicPolicy, WebSocketLike } from "../src/index.js";

const T = "e2e.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
// real-time constants: fast commit/retry, generous stall
const RT: Partial<Constants> = {
  GROUP_COMMIT_MS: 5,
  CONTROL_RETRY_MS: 250,
  ANTI_ENTROPY_MS: 1_000,
  CHANNEL_STALL_MS: 10_000,
  HELLO_TIMEOUT_MS: 3_000,
};

const dir = mkdtempSync(join(tmpdir(), "seqscribe-e2e-"));
const cleanup: (() => void)[] = [];
afterAll(() => {
  for (const fn of cleanup.splice(0)) fn();
  rmSync(dir, { recursive: true, force: true });
});

function openNode(writerId: string, dbPath: string): { node: SeqscribeNode; db: Database.Database } {
  const db = new Database(dbPath);
  const node = createSeqscribe({ writerId, storage: betterSqlite3Handle(db), constants: RT });
  node.defineTopic(T, FULL);
  return { node, db };
}

async function until(cond: () => boolean, ms = 8_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error("condition not reached in time");
    await new Promise((r) => setTimeout(r, 25));
  }
}

describe("real transport + storage e2e", () => {
  it("syncs two nodes over actual WebSockets on file-backed DBs, then survives a restart", async () => {
    const aPath = join(dir, "a.db");
    const bPath = join(dir, "b.db");
    const a = openNode("wA", aPath);
    cleanup.push(() => a.db.close());

    // server side: every accepted socket becomes a channel on node A
    const wss = new WebSocketServer({ port: 0 });
    cleanup.push(() => wss.close());
    let peerN = 0;
    wss.on("connection", (sock) => {
      a.node.attach(webSocketChannel(sock as unknown as WebSocketLike), {
        peerId: `client-${peerN++}`,
        peerClass: "content",
        grants: { [T]: "full" },
      });
    });
    await until(() => wss.address() !== null);
    const port = (wss.address() as { port: number }).port;

    // backlog exists before the client ever connects
    for (let i = 0; i < 20; i++) void a.node.log(T).append("note", { i });
    await until(() => coreOf(a.node).getStream(T, "wA").contigSeq === 20);

    // client side: global WebSocket → channel on node B
    let b = openNode("wB", bPath);
    const dial = (node: SeqscribeNode) => {
      const sock = new WebSocket(`ws://127.0.0.1:${port}`);
      node.attach(webSocketChannel(sock as unknown as WebSocketLike), {
        peerId: "server",
        peerClass: "content",
        grants: { [T]: "full" },
      });
    };
    dial(b.node);
    await until(() => coreOf(b.node).getStream(T, "wA").contigSeq === 20);
    expect(coreOf(b.node).getStream(T, "wA").contigChain).toBe(
      coreOf(a.node).getStream(T, "wA").contigChain,
    );

    // reverse direction over the same socket
    for (let i = 0; i < 5; i++) void b.node.log(T).append("note", { from: "b", i });
    await until(() => coreOf(a.node).getStream(T, "wB").contigSeq === 5);

    // "process restart": close node B fully, reopen the same file
    await b.node.close();
    b.db.close();
    b = openNode("wB", bPath);
    cleanup.push(() => b.db.close());

    // durable state survived: both streams, chains, and HLC monotonicity
    expect(coreOf(b.node).getStream(T, "wA").contigSeq).toBe(20);
    expect(coreOf(b.node).getStream(T, "wB").contigSeq).toBe(5);
    const before = coreOf(b.node).entries(T, "wB", 5, 5)[0]!;
    void b.node.log(T).append("note", { after: "restart" });
    await until(() => coreOf(b.node).getStream(T, "wB").contigSeq === 6);
    const after = coreOf(b.node).entries(T, "wB", 6, 6)[0]!;
    expect(
      after.hlc.l > before.hlc.l || (after.hlc.l === before.hlc.l && after.hlc.c > before.hlc.c),
    ).toBe(true); // persisted HLC state kept the stream monotonic across the restart

    // reconnect and converge the post-restart append
    dial(b.node);
    await until(() => coreOf(a.node).getStream(T, "wB").contigSeq === 6);
    expect(coreOf(a.node).entries(T, "wB", 6, 6)).toEqual([after]);

    await b.node.close();
    await a.node.close();
  }, 30_000);
});
