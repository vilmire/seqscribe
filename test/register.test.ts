// §11 register semantics: lww/fww/resolver folds, causal stamping, member
// sub-registers with reset generations, mirror rules, owned lifecycle.

import { describe, expect, it } from "vitest";
import { VirtualLink } from "../harness/bus.js";
import { SeededRng } from "../harness/rng.js";
import { Scheduler } from "../harness/scheduler.js";
import { memoryHandle } from "../harness/sqlite.js";
import { createSeqscribe } from "../src/index.js";
import type {
  Anomaly,
  Conflict,
  Constants,
  SeqscribeNode,
  TopicPolicy,
} from "../src/index.js";
import type { RegisterHub } from "../src/register.js";

const T = "cfg.settings";
const POLICY: TopicPolicy = {
  kind: "register",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
  conflict: { default: "lww", overrides: { "security.*": "fww" } },
};
const TEST_CONSTANTS: Partial<Constants> = {
  ANTI_ENTROPY_MS: 1_000,
  CONTROL_RETRY_MS: 200,
  CHANNEL_STALL_MS: 30_000,
};

function regOf(node: SeqscribeNode): RegisterHub {
  return (node as unknown as { _registers: RegisterHub })._registers;
}

function makeNode(
  sched: Scheduler,
  writerId: string,
  policy: TopicPolicy = POLICY,
): { node: SeqscribeNode; anomalies: Anomaly[]; conflicts: Conflict[] } {
  const anomalies: Anomaly[] = [];
  const conflicts: Conflict[] = [];
  const node = createSeqscribe({
    writerId,
    storage: memoryHandle(),
    clock: sched.clock(),
    timers: sched.timers(),
    rng: () => 0.7,
    constants: TEST_CONSTANTS,
    authority: {
      verifyTakeover: (e) =>
        (e.payload as { hostSig?: string } | null)?.hostSig === "valid-takeover",
      verifyWriterDirective: () => true,
      issueTakeover: async () => ({ hostSig: "valid-takeover" }),
    },
  });
  node.onAnomaly((a) => anomalies.push(a));
  node.defineTopic(T, policy);
  node.onConflict(T, (c) => {
    conflicts.push(c);
  });
  return { node, anomalies, conflicts };
}

function connect(sched: Scheduler, rng: SeededRng, a: SeqscribeNode, b: SeqscribeNode) {
  const link = new VirtualLink(sched, rng);
  a.attach(link.a, { peerId: "peerB", peerClass: "content", grants: { [T]: "full" } });
  b.attach(link.b, { peerId: "peerA", peerClass: "content", grants: { [T]: "full" } });
  return link;
}

async function winner(sched: Scheduler, n: SeqscribeNode, key: string) {
  await regOf(n).settle(T);
  const snap = regOf(n).snapshotState(T);
  return snap.keys[key];
}

describe("lww fold (§11.4)", () => {
  it("adopts the causal successor without conflict", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(51);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 1_300 });

    void a.node.register(T).set("theme", "dark");
    await sched.run({ untilMs: 2_500 }); // replicate to B
    void b.node.register(T).set("theme", "light"); // causal successor of A's write
    await sched.run({ untilMs: 4_000 });

    const ka = await winner(sched, a.node, "theme");
    const kb = await winner(sched, b.node, "theme");
    expect(ka?.value).toBe("light");
    expect(kb?.value).toBe("light");
    expect(ka?.frontier).toHaveLength(1);
    expect(a.conflicts).toHaveLength(0);
    expect(b.conflicts).toHaveLength(0);
  });

  it("surfaces concurrent writes and converges on the total-order winner", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(52);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");

    // partitioned concurrent writes
    void a.node.register(T).set("theme", "dark");
    await sched.run({ untilMs: 1_100 });
    void b.node.register(T).set("theme", "light"); // later hlc → lww winner
    await sched.run({ untilMs: 1_300 });

    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 4_000 });

    const ka = await winner(sched, a.node, "theme");
    const kb = await winner(sched, b.node, "theme");
    expect(ka?.value).toBe("light");
    expect(kb?.value).toEqual(ka?.value);
    expect(ka?.frontier.length).toBe(2); // both heads unresolved
    expect(a.conflicts.length + b.conflicts.length).toBeGreaterThan(0);
    expect(a.conflicts[0]?.concurrent).toBe(true);
  });
});

describe("fww override (§11.4)", () => {
  it("keeps the earlier of concurrent writes but applies causal successors", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(53);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");

    void a.node.register(T).set("security.pin", "1111"); // earlier
    await sched.run({ untilMs: 1_100 });
    void b.node.register(T).set("security.pin", "2222"); // concurrent, later
    await sched.run({ untilMs: 1_300 });
    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 4_000 });

    expect((await winner(sched, a.node, "security.pin"))?.value).toBe("1111"); // fww: earliest wins
    expect((await winner(sched, b.node, "security.pin"))?.value).toBe("1111");

    // a causal successor (B saw A's write) applies normally — not immutable-once-set
    void b.node.register(T).set("security.pin", "3333");
    await sched.run({ untilMs: 6_000 });
    expect((await winner(sched, a.node, "security.pin"))?.value).toBe("3333");
  });
});

describe("resolver flow (§5.8, §11.4)", () => {
  it("converges via an appended resolve event", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(54);
    const policy: TopicPolicy = {
      ...POLICY,
      conflict: { default: "resolver" },
    };
    const a = makeNode(sched, "wA", policy);
    const b = makeNode(sched, "wB", policy);

    void a.node.register(T).set("merged", ["a"]);
    await sched.run({ untilMs: 1_100 });
    void b.node.register(T).set("merged", ["b"]);
    await sched.run({ untilMs: 1_300 });

    // A's resolver merges on conflict
    const aReg = a.node;
    aReg.onConflict(T, async (c) => {
      if (c.member === undefined && c.entries.length === 2) {
        const values = c.entries
          .map((e) => (e.payload as { value: string[] }).value)
          .flat()
          .sort();
        await c.resolve(values);
      }
    });

    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 6_000 });

    const ka = await winner(sched, a.node, "merged");
    const kb = await winner(sched, b.node, "merged");
    expect(ka?.value).toEqual(["a", "b"]);
    expect(kb?.value).toEqual(["a", "b"]);
    expect(ka?.frontier).toHaveLength(1); // supersedes pruned the heads
  });
});

describe("member ops (§11.5)", () => {
  it("keeps concurrent adds of different members", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(55);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");
    void a.node.register(T).add("tags", "x");
    await sched.run({ untilMs: 1_100 });
    void b.node.register(T).add("tags", "y");
    await sched.run({ untilMs: 1_300 });
    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 4_000 });

    const ka = await winner(sched, a.node, "tags");
    expect(Object.keys(ka?.members ?? {}).sort()).toEqual(["x", "y"]);
    expect(ka?.members?.x?.present).toBe(true);
    expect(ka?.members?.y?.present).toBe(true);
  });

  it("del is a reset epoch — earlier member ops never resurrect", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(56);
    const a = makeNode(sched, "wA");
    const b = makeNode(sched, "wB");

    // B adds early (low hlc) while partitioned; A adds then dels later
    void b.node.register(T).add("tags", "ghost");
    await sched.run({ untilMs: 1_200 });
    void a.node.register(T).add("tags", "keep");
    await sched.run({ untilMs: 1_500 });
    void a.node.register(T).del("tags"); // total order: after both adds
    await sched.run({ untilMs: 1_800 });

    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 5_000 });

    // B's late-arriving pre-del add lands before the del in total order → dead
    const ka = await winner(sched, a.node, "tags");
    const kb = await winner(sched, b.node, "tags");
    expect(ka?.members ?? {}).toEqual({});
    expect(kb?.members ?? {}).toEqual({});
    expect(ka?.resetGen).toBeGreaterThanOrEqual(1);
  });

  it("mirror rules: set clears members; member op clears scalar", async () => {
    const sched = new Scheduler(1_000);
    const a = makeNode(sched, "wA");
    const reg = a.node.register(T);
    void reg.add("k", "m1");
    await sched.run({ untilMs: 1_200 });
    void reg.set("k", "scalar-now");
    await sched.run({ untilMs: 1_400 });
    let ks = await winner(sched, a.node, "k");
    expect(ks?.value).toBe("scalar-now");
    expect(ks?.members ?? {}).toEqual({});

    void reg.add("k", "m2");
    await sched.run({ untilMs: 1_600 });
    ks = await winner(sched, a.node, "k");
    expect(ks?.value).toBeUndefined();
    expect(ks?.members?.m2?.present).toBe(true);
  });
});

describe("owned (§5.8, §11.4)", () => {
  const ownedPolicy: TopicPolicy = {
    kind: "register",
    retention: { mode: "full" },
    replication: "full-sync",
    access: "content",
    conflict: { default: "owned" },
  };

  it("first author owns; non-owner writes are violations; requests approve", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(57);
    const a = makeNode(sched, "wA", ownedPolicy);
    const b = makeNode(sched, "wB", ownedPolicy);
    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 1_300 });

    void a.node.register(T).set("machine.name", "alpha");
    await sched.run({ untilMs: 2_500 });
    await regOf(b.node).settle(T);
    expect(b.node.ownerOf(T, "machine.name")).toBe("wA");

    // non-owner write → violation, not materialized
    void b.node.register(T).set("machine.name", "hijacked");
    await sched.run({ untilMs: 4_000 });
    expect((await winner(sched, a.node, "machine.name"))?.value).toBe("alpha");
    expect(a.anomalies.some((x) => x.kind === "owned_violation")).toBe(true);

    // request → owner approves → applied with ref
    a.node.onOwnedRequest(T, async () => "approve");
    void b.node.register(T).request("machine.name", "beta");
    await sched.run({ untilMs: 8_000 });
    expect((await winner(sched, a.node, "machine.name"))?.value).toBe("beta");
    expect((await winner(sched, b.node, "machine.name"))?.value).toBe("beta");
    expect(b.node.pendingRequests(T).length).toBe(0); // approved — no longer pending
  });

  it("chown transfers; takeover seizes with a host signature", async () => {
    const sched = new Scheduler(1_000);
    const rng = new SeededRng(58);
    const a = makeNode(sched, "wA", ownedPolicy);
    const b = makeNode(sched, "wB", ownedPolicy);
    connect(sched, rng, a.node, b.node);
    await sched.run({ untilMs: 1_300 });

    void a.node.register(T).set("svc.role", "primary");
    await sched.run({ untilMs: 2_500 });
    void a.node.register(T).chown("svc.role", "wB");
    await sched.run({ untilMs: 4_000 });
    await regOf(b.node).settle(T);
    expect(b.node.ownerOf(T, "svc.role")).toBe("wB");
    void b.node.register(T).set("svc.role", "standby"); // new owner writes fine
    await sched.run({ untilMs: 5_500 });
    expect((await winner(sched, a.node, "svc.role"))?.value).toBe("standby");

    // owner wB "dies" — wA takes over via the host-signed path
    const id = await (async () => {
      const p = a.node.register(T).takeover("svc.role", "wA");
      await sched.run({ untilMs: 7_000 });
      return p;
    })();
    expect(id[1]).toBe("wA");
    await regOf(a.node).settle(T);
    expect(a.node.ownerOf(T, "svc.role")).toBe("wA");
  });
});
