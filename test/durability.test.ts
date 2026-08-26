// Crash-shaped durability: a child process (test/durability-child.ts) appends
// to a file-backed node in a tight loop and is SIGKILLed mid-flight — no close,
// no flush, the WAL and the owner lock die with it. Reopening the same files
// in-process must find: a recovered store, an end-to-end verifiable chain from
// the seed, contig with no gaps below it, HLC monotonicity across the crash,
// and no wedged owner lock (proposals-v3.5 P3/P5: the BEGIN EXCLUSIVE on
// `<db>.lock` is an OS-level lock that dies with the process).

import Database from "better-sqlite3";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import { betterSqlite3Handle, chainOf, coreOf, createSeqscribe, seedOf } from "../src/index.js";
import type { SeqscribeNode, TopicPolicy } from "../src/index.js";

const T = "e2e.crash"; // must match durability-child.ts
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

const root = fileURLToPath(new URL("..", import.meta.url));
const dir = mkdtempSync(join(tmpdir(), "seqscribe-crash-"));
const cleanup: (() => void)[] = [];
afterAll(() => {
  for (const fn of cleanup.splice(0)) fn();
  rmSync(dir, { recursive: true, force: true });
});

async function until(cond: () => boolean, ms: number, what: () => string): Promise<void> {
  const deadline = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timeout: ${what()}`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

function exited(child: ChildProcess): Promise<void> {
  return new Promise((r) => child.once("exit", () => r()));
}

describe("kill -9 durability", () => {
  it("survives a SIGKILL mid-append: WAL recovers, chain verifies, contig gapless, HLC monotonic, lock not wedged", async () => {
    const dbPath = join(dir, "crash.db");

    // child: same src/ via vite-node, appending as fast as commits allow
    const child = spawn(
      process.execPath,
      [join(root, "node_modules", "vite-node", "vite-node.mjs"), join(root, "test", "durability-child.ts"), dbPath],
      { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
    );
    cleanup.push(() => child.kill("SIGKILL"));
    let lastReported = 0;
    let stderr = "";
    child.stdout.on("data", (buf: Buffer) => {
      for (const line of buf.toString().split("\n")) {
        const n = Number(line.trim());
        if (Number.isInteger(n) && n > lastReported) lastReported = n;
      }
    });
    child.stderr.on("data", (buf: Buffer) => (stderr += buf.toString()));

    // let a healthy stream of commits build up, then kill without warning
    await until(() => lastReported >= 40, 20_000, () => `child reached seq ${lastReported}; stderr: ${stderr}`);
    child.kill("SIGKILL");
    await exited(child);
    const survivedFloor = lastReported; // every reported seq was a committed txn

    // reopen the same files in THIS process — the dead child's owner lock must
    // not wedge the open (its BEGIN EXCLUSIVE died with the process)
    const db = new Database(dbPath);
    const lockDb = new Database(`${dbPath}.lock`);
    let node!: SeqscribeNode;
    expect(() => {
      node = createSeqscribe({ writerId: "wC", storage: betterSqlite3Handle(db, lockDb) });
    }).not.toThrow(); // notably not ERR_DB_OWNED
    cleanup.push(() => db.close());
    node.defineTopic(T, FULL);

    // …and the lock is functional again, not merely absent: a second opener
    // is refused while we hold it
    expect(() =>
      createSeqscribe({
        writerId: "wC",
        storage: betterSqlite3Handle(new Database(dbPath), new Database(`${dbPath}.lock`)),
      }),
    ).toThrow(/ERR_DB_OWNED/);

    // WAL recovery: everything the child saw committed survived, contiguously
    // (getStream returns the LIVE head object — snapshot the numbers now)
    const head = coreOf(node).getStream(T, "wC");
    const contigSeq = head.contigSeq;
    const contigChain = head.contigChain;
    expect(contigSeq).toBeGreaterThanOrEqual(survivedFloor);
    const entries = coreOf(node).entries(T, "wC", 1, contigSeq);
    expect(entries).toHaveLength(contigSeq); // no gap below contig
    entries.forEach((e, i) => expect(e.seq).toBe(i + 1));

    // chain verifies end-to-end from the seed, and matches the contig head
    let chain = seedOf(T, "wC");
    for (const e of entries) {
      chain = chainOf(chain, e);
      expect(e.chain).toBe(chain);
    }
    expect(contigChain).toBe(chain);

    // HLC state is monotonic vs. every surviving entry: strictly increasing
    // within the stream, and a post-crash append lands strictly above the tail
    for (let i = 1; i < entries.length; i++) {
      const prev = entries[i - 1]!.hlc;
      const cur = entries[i]!.hlc;
      expect(cur.l > prev.l || (cur.l === prev.l && cur.c > prev.c)).toBe(true);
    }
    const tail = entries[entries.length - 1]!.hlc;
    const id = await node.log(T).append("tick", { after: "crash" });
    expect(id[2]).toBe(contigSeq + 1);
    const fresh = coreOf(node).entries(T, "wC", id[2], id[2])[0]!;
    expect(fresh.hlc.l > tail.l || (fresh.hlc.l === tail.l && fresh.hlc.c > tail.c)).toBe(true);

    await node.close();
  }, 30_000);
});
