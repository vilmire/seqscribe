# seqscribe

> Pronounced **"seek-scribe"** — **seq**uence + sub**scribe** + *scribe* (every stream has exactly one writer).

**Subscribe to sequences.** seqscribe replicates writer-owned append-only logs peer-to-peer and materializes them into derived SQLite views. Embeddable, transport-agnostic, no consensus, no master, partition-tolerant by construction.

- **Writer-owned streams** — every stream has exactly one author, so replication never needs agreement about *what happened*, only about *what has arrived*. Entries are seq-numbered and hash-chained per writer; a peer that is behind asks for a range and resumes from its cursor.
- **No consensus, no master** — peers exchange what they have and pull what they lack. There is no leader to elect and no quorum to lose, so a partitioned node keeps accepting local writes and converges on heal.
- **Bring your own transport** — the library speaks to an abstract bidirectional `Channel`. WebSocket and WebRTC DataChannel adapters ship in-tree; anything that moves bytes both ways works.
- **SQLite at both ends** — logs are durable in SQLite, and views are reducers you register over a topic, materialized into real queryable tables (with FTS5 mirrors, checkpoints, and rebuild).
- **Conflict policy where conflicts actually live** — append logs never conflict, so policy applies only to `register` topics, and there it is per-key-pattern (exact match, then longest prefix, then the topic default): `owned`, `fww`, `lww`, or a custom `resolver`.

## Quickstart

Not yet on npm — see [Status](#status).

```bash
git clone https://github.com/vilmire/seqscribe && cd seqscribe
npm install && npm run check     # typecheck + 275 tests + build + consumability fixture
```

```ts
import { createSeqscribe, betterSqlite3Handle, webSocketChannel } from "seqscribe";
import Database from "better-sqlite3";

const node = createSeqscribe({
  writerId: "node-a",                                    // this node's one stream identity
  storage: betterSqlite3Handle(new Database("seqscribe.db")),
});

node.defineTopic("journal", {
  kind: "append",
  retention: { mode: "full" },
  replication: "full-sync",
  access: "content",
});

// a view is a reducer over the topic, materialized into a real SQLite table
const counts = node.view<Record<string, number>, { kind: string; n: number }>(
  "counts",
  "journal",
  {
    version: "1",
    init: {},
    reduce: (s, e) => ({ ...s, [e.kind]: (s[e.kind] ?? 0) + 1 }),
    rows: (s) => Object.entries(s).map(([kind, n]) => ({ kind, n })),
    rowKey: "kind",
    schema: { kind: "TEXT", n: "INTEGER" },
  },
);

// append to our own stream
await node.log("journal").append("note", { text: "hello" });

// sync with a peer over any bidirectional channel
const peer = node.attach(webSocketChannel(new WebSocket("wss://peer.example")), {
  peerId: "node-b",
  peerClass: "content",
  grants: { journal: "full" },
});

// …entries converge into the view's materialized SQLite table
counts.query(`SELECT kind, n FROM "${counts.table}" ORDER BY n DESC`);
```

A thin consumer can skip replication entirely and subscribe to a peer's *view* instead — snapshot then live deltas, no local log:

```ts
const sub = node.subscribe(peer, { view: "counts", params: null });
sub.onSnapshot((rows) => render(rows));
sub.onDelta((delta) => apply(delta));
```

Durable cursor-based consumption (`onEntry` with at-least-once delivery and a persisted cursor), registers, finality certificates, and fork recovery are covered in [SPEC.md](SPEC.md) §9–§12 and the [host guide](docs/host-guide.md).

## Status

**Implementation complete through the pre-integration tier; not yet released.** Current contract is **SPEC v3.7**; package version is `0.1.0-dev.0`.

- All five §19 milestones implemented. 275 tests green across 31 files, plus two env-gated profiles (`SOAK=1`, `FRESH_SEEDS=n`).
- The full §19 **P7 catch-up gate passes**: 100 nodes / 200 topics / 10 writes per second / 60 s host-visible partition / 1% loss converge within the ≤120 s gate. The gate is the claim; per-run timing varies (heal+40–50 s observed).
- Real-infrastructure e2e is green — actual WebSockets, file-backed SQLite, wall-clock timers, SIGKILL durability, and a restart-shaped reopen that proves HLC and contig persistence.
- Verified in a real browser: a full node on sqlite-wasm bidirectionally full-syncs with a Node peer over a WebSocket and consumes its view over a subscription (`npm run e2e:browser`, `npm run e2e:dashboard`).

Extracted from and dogfooded by [ADHDev](https://adhf.dev) — a mixed-platform fleet of daemons, browsers and serverless workers, which is currently the main source of amendments (the v3.5/v3.6 cycles were entirely integration-surfaced). A versioned release follows that production validation. Exact coverage and the remaining tracked gaps are in [docs/implementation.md](docs/implementation.md).

**`npm install seqscribe` currently resolves to the pre-implementation `0.0.1` placeholder** (published under MIT, before any of this existed). Install from git until the first real release.

## Storage and transports

Core is pure TypeScript with two runtime dependencies (a vetted JCS canonicalizer and `@noble/hashes`). Everything platform-specific is an adapter:

| | Shipped in-tree |
|---|---|
| Storage | `betterSqlite3Handle` (Node, with crash-safe cross-process ownership) · `sqliteWasmHandle` (browser, official sqlite-wasm) · `durableObjectSqlHandle` (Cloudflare) |
| Transport | `webSocketChannel` · `dataChannelChannel` (WebRTC) — or implement the §14 `Channel` interface yourself |
| Beacon | `httpBeaconTransport` client · `beaconFetchHandler` reference server (staleness hints only — never a correctness gate) |

## Documents

The design contract:

- [DESIGN.md](DESIGN.md) — architecture contract: positioning, design envelope, principles, competitive map, topic policies, conflict policy spectrum, beacon, 15 stamped decisions
- [SPEC.md](SPEC.md) — implementation spec: wire protocol, HLC rules, SQLite DDL, state machines, register semantics, full API surface, constants, error codes, deterministic simulation acceptance criteria (P1–P10)

Companion documents (non-normative):

- [docs/host-guide.md](docs/host-guide.md) — the host's contract obligations: identity issuance, authority operations, fork adjudication runbook, backup truth table, anomaly triage
- [docs/implementation.md](docs/implementation.md) — module decomposition mapped to the §19 milestones, with current coverage
- [docs/harness.md](docs/harness.md) — simulation harness design: virtual time, fault model, quiescence, P1–P10 checker algorithms, CI shape
- [docs/test-vectors.md](docs/test-vectors.md) — known-answer vectors for chains, seeds, certHash, snapshotId, JCS edges ([`vectors/vectors.json`](vectors/vectors.json), regenerated by [`tools/gen-vectors.mjs`](tools/gen-vectors.mjs))
- [CHANGELOG.md](CHANGELOG.md) — every SPEC amendment, what it changed and why
- Amendment rationale records, newest first: [P30–P34](docs/proposals-v3.8.md) (host-surface gaps found by reading an embedder's glue — P30–P33 ratified into v3.7, P34 open) · [P29](docs/proposals-v3.7.md) (ratified into v3.7) · [v3.5/v3.6](docs/proposals-v3.5.md) (P1–P28, integration-surfaced) · [v3.4](docs/proposals-v3.4.md) (surfaced by failing P7 runs) · [v3.3](docs/proposals-v3.3.md) (vector/harness work)

A general-purpose library, maintained for our needs first — see [CONTRIBUTING.md](CONTRIBUTING.md) for what that means for a PR, and [SECURITY.md](SECURITY.md) to report a vulnerability.

## License

Dual-licensed — **[FSL-1.1-Apache-2.0](LICENSE-FSL) OR [AGPL-3.0-only](LICENSE-AGPL), at your option** ([LICENSE](LICENSE)):

- **FSL** for embedders: use it in anything, build products and internal systems freely; the only restricted use is offering seqscribe itself as a competing commercial product or service. Each release automatically becomes Apache-2.0 two years after publication.
- **AGPL** for copyleft works: combine with AGPL projects (including the ADHDev OSS tier) with full open-source compatibility.

Free-riding is blocked on both legs — FSL forbids competing offerings outright; AGPL forces them fully open. (The `0.0.1` npm placeholder was published under MIT and remains so; every version from the implementation onward is dual FSL/AGPL.)
