# seqscribe implementation map — modules, boundaries, build order

> Status: **non-normative planning document** (2026-08-26; progress annotated same day). The SPEC defines the contract; this maps it onto a module decomposition and the §19 milestone order. Nothing here overrides the SPEC; section references are the authority for every behavior named.
>
> **Progress (2026-08-26, second pass)**: milestones ①–⑤ plus the operational tier — 100 tests (98 gating + 2 env-gated: `SOAK=1` sustained-load profile, `FRESH_SEEDS=n` seed exploration). **The full §19 P7 gate passes**: 100 nodes / 200 topics / 10 writes/s / 60 s host-visible partition / 1% loss converge at heal+40 s (≤120 s gate) — `test/p7.test.ts`. Getting there surfaced four spec defects, recorded with evidence in [proposals-v3.4.md](proposals-v3.4.md): hlc-window relay cannot gossip partition backlogs (→ knowledge-based push), data-frame loss head-of-line blocks until the stall close (→ same-mid retransmission), silent partitions heal only at anti-entropy cadence (documented bound), and `sq_log.rowid` reuse after archiving breaks every cursor (→ AUTOINCREMENT). Also landed since the first pass: cold archiving §7.6 (consumer-gated, `consumer_abandoned`, post-cut resume, base-checkpoint/`regbase` rebuilds, checkpoint pruning), the failing-seed corpus (`harness/seeds.json`), FTS5 mirror views, the built-in `register` subscription family, PROBE-bounded §12 recovery, and per-topic `flushThrottleMs` on eager push.
>
> **Third pass (2026-08-26)**: SPEC stamped to **v3.4** (the four P7 amendments are now normative; RELAY_WINDOW_MS/RELAY_FANOUT retired). Landed: reference WebSocket transport (`webSocketChannel` — wraps any connected browser/Node/`ws` socket into the §14 Channel), `betterSqlite3Handle` storage adapter (structural, zero native deps in core), `httpBeaconTransport` (§14 beacon wire client) + bearer auth on the reference beacon server, and the first **real-infrastructure e2e**: actual WebSockets + file-backed SQLite + wall-clock timers, including a restart-shaped reopen that proves HLC/contig persistence (`test/e2e-real.test.ts`, `test/beacon-http.test.ts`).
>
> **Fourth pass (2026-08-26)** — integration-readiness: attach now rejects `full` grants on subscribe-only topics; the better-sqlite3 adapter enforces crash-safe cross-process ownership (`BEGIN EXCLUSIVE` on `<db>.lock`); `stats()` exposes the host-guide metrics baseline; host helpers landed (`hmacAuthority`, `startFinalityLoop`, `manageReconnect`, `loadOrCreateWriterId`, `migrateLegacyJsonl`); browser storage proven — `sqliteWasmHandle` runs the full pipeline on the official sqlite-wasm build (plus a source guard: no `node:` imports in src/), **and verified in a real Chromium browser**: `npm run e2e:browser` serves a page that runs a full node on sqlite-wasm, bidirectionally full-syncs with a Node server over a real WebSocket (chain-hash equality asserted), and consumes the server's view via a Tier-2 SUB/SNAP subscription — the dashboard shape end to end; `durableObjectSqlHandle` + `beaconFetchHandler` cover the Cloudflare tier. Spec-adjacent additions recorded in [proposals-v3.5.md](proposals-v3.5.md). 114 tests (112 gating + 2 env-gated).
>
> **Remaining (small, tracked)**: pre-cut register *requests* are not carried in snapshots (RegisterSnapshotState has no requests field — a spec-level decision) · archive lives in `sq_archive` rather than the spec's `archive/<topic>.jsonl.gz` file form (adapter concern; export can drain it) · package split (`seqscribe-ws` etc. currently ship inside core — harmless until release) · browser worker/OPFS reference lives in e2e-browser/ (`npm run e2e:dashboard` — verified in Chromium: OPFS persistence across reloads, zero main-thread long tasks) · **ADHDev integration** (the next phase).

## 1. Package layout (from DESIGN §8)

```
seqscribe/            core, pure TS, zero runtime deps except the vetted JCS lib
seqscribe-ws/         reference WebSocket transport (Channel impl)
seqscribe-beacon/     reference beacon (tens of lines; §14 wire)
@seqscribe/*          storage adapters (better-sqlite3 first)
```

Core ships as one package; the module boundaries below are internal directories, not packages.

## 2. Core modules

Dependency direction is top-to-bottom within each layer block; nothing below depends on anything above it.

### Layer 0 — pure functions (no state, no I/O; property: fully vector-testable)

| Module | SPEC | Contents |
|---|---|---|
| `encoding` | §4 | JCS wrapper (vetted dep + integration guards), `F()`, `dec()`, `seed()`, `chain()`, sha256 helpers, `certHash`, `snapshotId`, `topicSchemaHash` normalization. **Gate: replays [`vectors/vectors.json`](../vectors/vectors.json) byte-for-byte.** |
| `hlc` | §1, §3 | stamp, merge (first-sight only), carry rule, ε checks, total-order comparator `order(a,b)` |
| `codec` | §5.4, §2 | message & LogEntry parse/validate (charter regexes, size limits, structural checks) → typed values or `ERR_ENTRY_ENCODING`/protocol errors. Validation lives here so every ingress path shares it |

### Layer 1 — local node (single process, no network)

| Module | SPEC | Contents |
|---|---|---|
| `store` | §8 | DDL, migrations-from-empty, `SqliteHandle` discipline, ownership lock, sq_* table accessors |
| `log` | §8 write path, §6.1 | the single append queue + group commit; contig state; chain verify at apply; pending buffer + drain; **cert/directive application serialized through the same queue**; re-entrancy rule (async enqueue from callbacks) |
| `topics` | §14 | TopicPolicy validation, schemaHash, immutability-per-process, defineTopic guards (authority-hook presence checks) |
| `views` | §9 | reducer runtime, materialization pipeline (async, batched), checkpoints, delta verification at checkpoints, faulted-view state, rebuild, late-arrival suffix recompute (restore checkpoint < p, recompute, atomic swap, epoch bump) |
| `consume` | §9 `onEntry`, §7.6 | per-consumer serial dispatch in rowid order, cursor persistence, backoff retry, abandonment + post-cut reset |
| `register` | §11 | the register reducer as a **pure fold** over totally-ordered entries (policies lww/fww/resolver/owned, member sub-registers, resetGen derivation, mirror rules, frontier/superseded tracking) + helper append paths (causal stamping incl. uncommitted tail, §11.2) + conflict surfacing/resolution API. The fold purity matters: snapshot state (§11.6) and live state must be products of the same function |

### Layer 2 — protocol (per-connection state machines)

| Module | SPEC | Contents |
|---|---|---|
| `session` | §5.1–5.3, §18 | attach lifecycle, HELLO negotiation, grants/peerClass enforcement (re-checked per message), control-lane priority queue + retries, data-lane mids (assigned at transmission), ACK-to-contig, credits, stall detection, SEND_QUEUE_CAP tail-drop |
| `sync` | §6, §5.4 | HAVE_GET/HAVE round capture semantics, WANT scheduling (≤4 concurrent, fromSeq=contig+1), ENTRIES batching with captured toSeq, fork detection (4 paths) + PROBE, eager push (K-peer selection, deterministic tiebreak) + relay window |
| `finality` | §7 | cert verify/accept (strictly-increasing generation, single authority), enforcement + quarantine transitions (§7.5a/b — including contig rewind and 'fork'-equivalent sealing), fgen repair, proposeFinality computation, cold-archive discipline |
| `snapshot` | §7.7–7.8 | SnapshotBody build (directives originals + register state + view states), byte-level chunking (shared with SNAP), adoption (certHash verify, no-cert refusal, directive signature verify → rgen fold → lifecycle rebuild), bootstrap ordering rule |
| `directives` | §12, §13 | directive verify/store/push/repair, rgen conflict handling, retire/unretire sugar (per-topic fan-out via issuer hook + match verification), five-step fork recovery incl. seal pass-through and the absorbing state |
| `subs` | §10, §5.4 | server side: serving groups, delta journal, epoch/cursor logic, SNAP chunking, DELTA-overflow downgrade; client side: `subscribe()` handle, re-SUB on reconnect |
| `beacon` | §5.7 (DESIGN), §14 | client only in core: debounced PUT, `setKnownVectors`, `staleness()` derivation |

### Layer 3 — assembly

| Module | SPEC | Contents |
|---|---|---|
| `node` | §14 | `createSeqscribe` wiring: injects clock/rng/timers/authority/constants into everything above; the public `SeqscribeNode` façade; `SeqscribeError` discipline (sync-throw / async-reject) |
| `export` | §15 | JSONL export/import, header/base handling, chain verification on import (identity-preserving) |
| `cli` | §17 | thin wrapper: export/import/beacon/inspect (read-only WAL-aware connection) |

## 3. Build order = §19 milestones, with the harness in front

The harness ([harness.md](harness.md)) is built *with* milestone ①, not after ⑤ — every milestone lands with its properties running.

| Milestone | Modules | Properties runnable |
|---|---|---|
| ⓪ prerequisite | ratify [proposals-v3.3.md](proposals-v3.3.md) P1 (timer seam); harness skeleton (scheduler, rng substreams, virtual bus, sim storage) | vector replay |
| ① core log | `encoding`, `hlc`, `codec`, `store`, `log`, `topics` | P5 (trace determinism on local workloads); vector gate; types.d.ts gate |
| ② sync | `session`, `sync` | P1, P4, P5; first fork-detection scenarios of P9 |
| ③ finality & views | `views`, `consume`, `finality`, `snapshot` | P2, P3, P6, P10; P7 first run |
| ④ subscriptions | `subs`, ring retention paths | P4/P6 subscription scenarios; SNAP/DELTA edges of §18 |
| ⑤ register & directives | `register`, `directives`, `beacon`, `export`, `cli` | P8, full P9; full §18 edge coverage; P7 gate + soak profile |

Release remains **one complete unit** (DESIGN §9) — milestones are internal sequencing only.

## 4. Cross-cutting disciplines (decided once, here)

- **Injection completeness**: modules receive `{clock, rng, timers, constants}` from `node` — none may import a global time/random source. Enforced by lint (see harness memo §2).
- **Single writer queue**: all state mutations that touch `sq_log`/`sq_writers` (appends, cert application, directive application, quarantine) flow through `log`'s queue. Other modules submit intents; nothing else writes those tables.
- **Errors**: every thrown/rejected error from public API or wire handling is a `SeqscribeError` with a §14 code; internal invariant violations crash loudly (they are harness bugs to find, not conditions to handle).
- **Anomalies are fire-and-forget**: `onAnomaly` must never be load-bearing for correctness (§7.5 already says so for quarantine; adopt it as the general rule).
- **No hidden wire fields**: `codec` rejects unknown message types and unknown *required* semantics but tolerates unknown optional fields — proto v1 forward-compat posture (version negotiation is the real gate, §5.4 HELLO).

## 5. Dependency policy

Runtime deps of core: the vetted JCS implementation, nothing else (crypto from the platform; SQLite via the injected adapter). Dev deps: TypeScript, the harness's SQLite driver, test runner. Anything further needs a written justification in this file.
