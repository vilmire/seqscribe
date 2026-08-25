import { describe, expect, it } from "vitest";
import { conflictPolicyFor, SeqscribeError, TopicRegistry } from "../src/index.js";
import type { TopicPolicy } from "../src/index.js";

const appendFull: TopicPolicy = {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
};

describe("defineTopic guards (§14)", () => {
  it("accepts a valid append/full policy", () => {
    const reg = new TopicRegistry();
    reg.define("a.topic", appendFull);
    expect(reg.get("a.topic").schemaHash).toMatch(/^[0-9a-f]{64}$/);
  });

  it("rejects ring retention with full-sync", () => {
    const reg = new TopicRegistry();
    expect(() =>
      reg.define("r.topic", {
        kind: "append",
        retention: { mode: "ring", size: 100 },
        replication: "full-sync",
        access: "content",
      }),
    ).toThrowError(SeqscribeError);
  });

  it("rejects register with ring retention", () => {
    const reg = new TopicRegistry();
    expect(() =>
      reg.define("r.topic", {
        kind: "register",
        retention: { mode: "ring", size: 100 },
        replication: "subscribe-only",
        access: "content",
      }),
    ).toThrowError(SeqscribeError);
  });

  it("rejects finalityAuthority without verifyFinality", () => {
    const reg = new TopicRegistry();
    expect(() =>
      reg.define("f.topic", { ...appendFull, finalityAuthority: "auth:x" }),
    ).toThrowError(SeqscribeError);
    reg.define(
      "f.topic",
      { ...appendFull, finalityAuthority: "auth:x" },
      { verifyFinality: () => true },
    );
  });

  it("rejects owned without takeover/directive verifiers", () => {
    const reg = new TopicRegistry();
    const owned: TopicPolicy = {
      kind: "register",
      retention: { mode: "full" },
      replication: "full-sync",
      access: "content",
      conflict: { default: "owned" },
    };
    expect(() => reg.define("o.topic", owned)).toThrowError(SeqscribeError);
    reg.define("o.topic", owned, {
      verifyTakeover: () => true,
      verifyWriterDirective: () => true,
    });
  });

  it("rejects bad override globs", () => {
    const reg = new TopicRegistry();
    expect(() =>
      reg.define("g.topic", {
        kind: "register",
        retention: { mode: "full" },
        replication: "full-sync",
        access: "content",
        conflict: { default: "lww", overrides: { "a*b": "fww" } },
      }),
    ).toThrowError(SeqscribeError);
  });

  it("is immutable per process — identical redefine ok, changed redefine throws", () => {
    const reg = new TopicRegistry();
    reg.define("i.topic", appendFull);
    reg.define("i.topic", { ...appendFull });
    expect(() => reg.define("i.topic", { ...appendFull, access: "metadata" })).toThrowError(
      SeqscribeError,
    );
  });
});

describe("conflict policy resolution (§11.7)", () => {
  const policy: TopicPolicy = {
    kind: "register",
    retention: { mode: "full" },
    replication: "full-sync",
    access: "content",
    conflict: {
      default: "lww",
      overrides: { "security.*": "fww", "security.keys.*": "resolver", "security.pin": "resolver" },
    },
  };

  it("exact match beats prefix; longest prefix wins; default otherwise", () => {
    expect(conflictPolicyFor(policy, "security.pin")).toBe("resolver");
    expect(conflictPolicyFor(policy, "security.keys.root")).toBe("resolver");
    expect(conflictPolicyFor(policy, "security.other")).toBe("fww");
    expect(conflictPolicyFor(policy, "anything.else")).toBe("lww");
    // "security.*" must not match the bare prefix without the dot boundary
    expect(conflictPolicyFor(policy, "securityX")).toBe("lww");
  });
});
