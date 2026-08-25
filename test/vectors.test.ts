// The known-answer gate (docs/test-vectors.md): the implementation must
// reproduce vectors/vectors.json byte-for-byte.

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  certHashOf,
  chainOf,
  frame,
  jcs,
  seedOf,
  snapshotIdOf,
  topicSchemaHashOf,
} from "../src/index.js";
import type { FinalityCert, LogEntry, SnapshotBody, TopicPolicy } from "../src/index.js";

interface Vectors {
  jcs: { name: string; input: unknown; jcs: string }[];
  framing: { input: string; hex: string }[];
  seeds: { topic: string; writer: string; seed: string }[];
  chains: { entry: LogEntry; prevChain: string; chain: string }[];
  certHash: { cert: FinalityCert; jcs: string; certHash: string };
  snapshotId: { body: SnapshotBody; jcs: string; snapshotId: string };
  topicSchemaHash: { name: string; policy: TopicPolicy; jcs: string; topicSchemaHash: string }[];
}

const vectors = JSON.parse(
  readFileSync(join(import.meta.dirname, "..", "vectors", "vectors.json"), "utf8"),
) as Vectors;

function toHex(b: Uint8Array): string {
  return [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
}

describe("vectors.json replay", () => {
  it("JCS", () => {
    for (const v of vectors.jcs) {
      expect(jcs(v.input as never), v.name).toBe(v.jcs);
    }
    // -0 survives JSON round-trip as 0, so assert it directly too
    expect(jcs(-0)).toBe("0");
  });

  it("F() framing", () => {
    for (const v of vectors.framing) {
      expect(toHex(frame(v.input))).toBe(v.hex);
    }
  });

  it("seeds", () => {
    for (const v of vectors.seeds) {
      expect(seedOf(v.topic, v.writer)).toBe(v.seed);
    }
  });

  it("chains", () => {
    for (const v of vectors.chains) {
      expect(chainOf(v.prevChain, v.entry), `seq ${v.entry.seq}`).toBe(v.chain);
    }
  });

  it("certHash", () => {
    expect(jcs(vectors.certHash.cert as never)).toBe(vectors.certHash.jcs);
    expect(certHashOf(vectors.certHash.cert)).toBe(vectors.certHash.certHash);
  });

  it("snapshotId", () => {
    expect(snapshotIdOf(vectors.snapshotId.body)).toBe(vectors.snapshotId.snapshotId);
  });

  it("topicSchemaHash", () => {
    for (const v of vectors.topicSchemaHash) {
      expect(topicSchemaHashOf(v.policy), v.name).toBe(v.topicSchemaHash);
    }
  });
});
