// The reference beacon over real HTTP: the bin/seqscribe.mjs server (bearer
// auth) + the core httpBeaconTransport client, driven by real nodes.

import { spawn, type ChildProcess } from "node:child_process";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { createSeqscribe, httpBeaconTransport } from "../src/index.js";
import { memoryHandle } from "../harness/sqlite.js";
import type { SeqscribeNode, TopicPolicy } from "../src/index.js";

const T = "t.notes";
const FULL: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

let child: ChildProcess | null = null;
afterAll(() => child?.kill());

function startBeacon(token: string): Promise<number> {
  return new Promise((resolve, reject) => {
    child = spawn("node", [join(import.meta.dirname, "..", "bin", "seqscribe.mjs"), "beacon", "0"], {
      env: { ...process.env, SEQSCRIBE_BEACON_TOKEN: token },
      stdio: ["ignore", "ignore", "pipe"],
    });
    let buf = "";
    child.stderr!.on("data", (c: Buffer) => {
      buf += c.toString();
      const m = /beacon on :(\d+)/.exec(buf);
      if (m) resolve(Number(m[1]));
    });
    child.on("error", reject);
    setTimeout(() => reject(new Error("beacon did not start")), 5_000);
  });
}

function mkNode(writerId: string): SeqscribeNode {
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    constants: { BEACON_DEBOUNCE_MS: 100 },
  });
  node.defineTopic(T, FULL);
  return node;
}

async function until(cond: () => boolean | Promise<boolean>, ms = 8_000): Promise<void> {
  const deadline = Date.now() + ms;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > deadline) throw new Error("condition not reached in time");
    await new Promise((r) => setTimeout(r, 50));
  }
}

describe("reference beacon over HTTP (§14 wire)", () => {
  it("reports vectors with bearer auth and predicts staleness", async () => {
    const port = await startBeacon("s3cret");
    const base = `http://127.0.0.1:${port}`;

    // wrong token is refused
    await expect(httpBeaconTransport(base, "acct", "wrong").get()).rejects.toThrow(/401/);

    const a = mkNode("wA");
    const b = mkNode("wB");
    const ta = httpBeaconTransport(base, "acct", "s3cret");
    a.beacon(ta);
    b.beacon(httpBeaconTransport(base, "acct", "s3cret"));

    for (let i = 0; i < 7; i++) void a.log(T).append("note", { i });
    await until(async () => {
      const reports = await ta.get();
      const mine = reports.find((r) => r.node === "wA");
      const w = mine?.vectors[T]?.writers.wA;
      return w !== undefined && !("retired" in w) && w.contig === 7;
    });

    // B pulls the board and predicts its lag without any peer connection
    b.setKnownVectors(await ta.get());
    expect(b.staleness(T).behind.wA).toBe(7);

    await a.close();
    await b.close();
  }, 20_000);
});
