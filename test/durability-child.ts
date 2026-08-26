// Crash workload for test/durability.test.ts (NOT a test file): opens a
// file-backed node, appends in a tight loop, reports each committed seq on
// stdout, and never exits on its own — the parent SIGKILLs it mid-flight.
// Run via vite-node so it executes the same src/ the suite tests.

import Database from "better-sqlite3";
import { betterSqlite3Handle, createSeqscribe } from "../src/index.js";
import type { TopicPolicy } from "../src/index.js";

const dbPath = process.argv[2];
if (!dbPath) throw new Error("usage: durability-child <dbPath>");

const T = "e2e.crash";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

const db = new Database(dbPath);
const lockDb = new Database(`${dbPath}.lock`); // real owner lock — dies with this process
const node = createSeqscribe({
  writerId: "wC",
  storage: betterSqlite3Handle(db, lockDb),
  constants: { GROUP_COMMIT_MS: 1 },
});
node.defineTopic(T, FULL);

// each append is awaited, so a seq printed here is a COMMITTED transaction —
// the parent asserts everything it saw on stdout survives the SIGKILL
for (let i = 1; ; i++) {
  const id = await node.log(T).append("tick", { i });
  process.stdout.write(`${id[2]}\n`);
}
