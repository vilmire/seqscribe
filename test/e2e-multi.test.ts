// >2-node e2e over REAL infrastructure (extends e2e-real.test.ts): three nodes
// on file-backed SQLite with actual WebSockets in an A↔B↔C line topology — C
// never connects to A, so A⇄C convergence proves relay through B. Then B dies
// (node closed, server torn down) and A↔C open a direct channel and
// re-converge without it.

import Database from "better-sqlite3";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocketServer } from "ws";
import { afterAll, describe, expect, it } from "vitest";
import { betterSqlite3Handle, coreOf, createSeqscribe, webSocketChannel } from "../src/index.js";
import type { Constants, SeqscribeNode, TopicPolicy, WebSocketLike } from "../src/index.js";

const T = "e2e.mesh";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};
const RT: Partial<Constants> = {
  GROUP_COMMIT_MS: 5,
  CONTROL_RETRY_MS: 250,
  ANTI_ENTROPY_MS: 1_000,
  CHANNEL_STALL_MS: 10_000,
  HELLO_TIMEOUT_MS: 3_000,
};
const WRITERS = ["wA", "wB", "wC"] as const;

const dir = mkdtempSync(join(tmpdir(), "seqscribe-e2e3-"));
const cleanup: (() => void)[] = [];
afterAll(() => {
  for (const fn of cleanup.splice(0)) fn();
  rmSync(dir, { recursive: true, force: true });
});

function openNode(writerId: string): { node: SeqscribeNode; db: Database.Database } {
  const db = new Database(join(dir, `${writerId}.db`));
  const node = createSeqscribe({ writerId, storage: betterSqlite3Handle(db), constants: RT });
  node.defineTopic(T, FULL);
  return { node, db };
}

// accept every socket as a full-grant content peer on `node`
async function serve(node: SeqscribeNode, label: string): Promise<{ wss: WebSocketServer; port: number }> {
  const wss = new WebSocketServer({ port: 0 });
  cleanup.push(() => wss.close());
  let n = 0;
  wss.on("connection", (sock) => {
    node.attach(webSocketChannel(sock as unknown as WebSocketLike), {
      peerId: `${label}-in-${n++}`,
      peerClass: "content",
      grants: { [T]: "full" },
    });
  });
  await until(() => wss.address() !== null);
  return { wss, port: (wss.address() as { port: number }).port };
}

function dial(node: SeqscribeNode, port: number, peerId: string): void {
  const sock = new WebSocket(`ws://127.0.0.1:${port}`);
  node.attach(webSocketChannel(sock as unknown as WebSocketLike), {
    peerId,
    peerClass: "content",
    grants: { [T]: "full" },
  });
}

async function until(cond: () => boolean, ms = 10_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error("condition not reached in time");
    await new Promise((r) => setTimeout(r, 25));
  }
}

function contig(n: SeqscribeNode, writer: string): number {
  return coreOf(n).getStream(T, writer).contigSeq;
}

describe("3-node real-WebSocket line topology", () => {
  it("converges A↔B↔C through relay, then A↔C re-converge directly after B dies", async () => {
    const a = openNode("wA");
    const b = openNode("wB");
    const c = openNode("wC");
    cleanup.push(() => a.db.close(), () => c.db.close());

    // line topology: B dials A, C dials B — C has no channel to A
    const srvA = await serve(a.node, "a");
    const srvB = await serve(b.node, "b");
    dial(b.node, srvA.port, "serverA");
    dial(c.node, srvB.port, "serverB");

    // all three write to the one topic
    for (let i = 0; i < 10; i++) {
      void a.node.log(T).append("note", { from: "a", i });
      void b.node.log(T).append("note", { from: "b", i });
      void c.node.log(T).append("note", { from: "c", i });
    }

    // full convergence on every node, including A⇄C which can only relay via B
    const nodes = { wA: a.node, wB: b.node, wC: c.node };
    await until(() =>
      Object.values(nodes).every((n) => WRITERS.every((w) => contig(n, w) === 10)),
    );
    for (const w of WRITERS) {
      // chain-hash equality: same contig head hash on all three nodes
      const chains = Object.values(nodes).map((n) => coreOf(n).getStream(T, w).contigChain);
      expect(chains[1]).toBe(chains[0]);
      expect(chains[2]).toBe(chains[0]);
    }

    // B dies: node closed, its server torn down — both A and C lose their peer
    await b.node.close();
    srvB.wss.close();
    b.db.close();

    // writes continue on the survivors while partitioned from each other
    for (let i = 10; i < 15; i++) {
      void a.node.log(T).append("note", { from: "a", i });
      void c.node.log(T).append("note", { from: "c", i });
    }
    await until(() => contig(a.node, "wA") === 15 && contig(c.node, "wC") === 15);
    expect(contig(a.node, "wC")).toBe(10); // C's new entries can't reach A yet
    expect(contig(c.node, "wA")).toBe(10);

    // A↔C reconnect through a NEW direct channel and re-converge without B
    dial(c.node, srvA.port, "serverA-direct");
    await until(
      () =>
        contig(a.node, "wC") === 15 &&
        contig(c.node, "wA") === 15 &&
        contig(c.node, "wB") === 10, // B's history is retained, not lost with B
    );
    for (const w of WRITERS) {
      expect(coreOf(c.node).getStream(T, w).contigChain).toBe(
        coreOf(a.node).getStream(T, w).contigChain,
      );
    }

    await c.node.close();
    await a.node.close();
  }, 25_000);
});
