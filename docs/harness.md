# seqscribe simulation harness — design memo

> Status: **design memo, non-normative** (2026-08-26). SPEC §19 defines *what* the harness must prove (P1–P10); the SPEC header declares the harness — not further document review — as the mechanism by which remaining defects surface. This memo designs the harness itself so milestone ① can start without re-deriving these decisions. Where this design found SPEC gaps, they are cross-referenced to [proposals-v3.3.md](proposals-v3.3.md).

## 1. Stance

- The harness is **the first consumer of the library**, built in milestone ① alongside core log/HLC/chain — not retrofitted.
- One integer seed determines the entire run: topology, workload, faults, clock skew, epochs, peer selection. "Same seed twice → byte-identical event trace" is itself a gated property (P5).
- The harness runs the **real library code** against injected adapters. Nothing is mocked below the injection seams the SPEC already defines (storage `SqliteHandle`, transport `Channel`, `clock`, `rng`) plus one seam the SPEC is missing (timers — §3 below).

## 2. Architecture

```
┌─────────────────────────── Simulation (one seed) ───────────────────────────┐
│  Scheduler (virtual time, priority queue)                                   │
│  ├── SeededRng root ──┬── per-node substream (epochs, peer selection)       │
│  │                    ├── per-link substream (loss/dup/delay draws)         │
│  │                    └── workload substream (op mix, timing)               │
│  ├── VirtualClock per node = virtualNow + offset(node) + drift(node)·t      │
│  ├── VirtualBus: Channel pairs with per-link FaultModel                     │
│  ├── Node[i]: createSeqscribe({storage: sqlite(:memory:), clock, rng,       │
│  │            timers, authority: simAuthority, constants: scaled})          │
│  ├── WorkloadDriver: appends, register ops, sub churn, restarts, directives │
│  └── Checkers: P1..P10 + invariant probes at quiescence points              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Storage**: real SQLite in-memory (`better-sqlite3` `:memory:`), not a JS map — the §8 DDL, UNIQUE constraint behavior, and rowid semantics are normative and must be exercised. SQLite is deterministic; determinism is preserved. A crash-restart is simulated by serializing the DB (`.serialize()`) at the last *committed* point and reopening — giving a real durability model (uncommitted group-commit batches are lost, matching `durability:"normal"` semantics).

**RNG**: splitmix64-derived substreams. Each consumer (node, link, workload) gets an independent substream keyed by a stable label, so inserting a new draw site in one component cannot shift every other component's sequence — without this, every code change invalidates the failing-seed corpus.

**Determinism ground rules**: no `Date.now`, `Math.random`, real timers, or real I/O anywhere in library or harness paths (enforced by lint rule + a harness-time monkeypatch that throws). JS single-threaded execution plus deterministic inputs yields deterministic promise interleaving.

## 3. The timer seam — SPEC gap (→ proposal P1)

`createSeqscribe` currently injects `clock()` and `rng()` only, but the library *schedules future work*: CONTROL_RETRY_MS retries, ANTI_ENTROPY_MS rounds, GROUP_COMMIT_MS flushes, CHANNEL_STALL_MS checks, HELLO_TIMEOUT_MS, beacon debounce. With real `setTimeout` those fire in wall time — **P5 (seed determinism) and P7 (virtual-time measurement) are unimplementable as specced.**

Required seam (exact TS in proposals-v3.3.md):

```ts
timers?: { setTimeout(cb, ms): unknown; clearTimeout(h): unknown }
```

Production default = globals. The harness supplies handles that enqueue onto the virtual-time scheduler. This is the single largest prerequisite for milestone ①.

## 4. Virtual time & scheduling

- Event = `{at: virtualMs, seqNo, fn}`; ties broken by insertion `seqNo` (deterministic FIFO within a timestamp).
- The loop: pop earliest event → run it → **drain all resulting microtasks to completion** (`await` a macrotask-boundary flush) → repeat. Library-internal promise chains thus settle "instantaneously" in virtual time, which is the correct model (compute is free; only scheduled delays and message latency advance the clock).
- Message delivery = an event at `sendTime + latencyDraw(link)`; timer callbacks = events at their due time. Nothing else advances time.

## 5. Fault model (all draws from per-link substreams)

| Fault | Model |
|---|---|
| Loss | per-message Bernoulli(p_loss) |
| Duplication | Bernoulli(p_dup) → second delivery at an independent later time |
| Reordering | emergent from i.i.d. latency draws (uniform or lognormal per link) — no separate mechanism |
| Partition | schedule of `(t_start, t_end, cut: Set<link>)` windows; messages in a cut window are dropped; `onClose` fires per SPEC channel semantics when the host layer would notice |
| Churn | node stop/start events; restart = reopen from serialized committed DB (§2), new process ⇒ new epochs |
| Crash | stop without clean close (in-flight and uncommitted batches lost) |
| Clock skew | per-node `offset + drift·t`, bounded within/beyond HLC_EPSILON_MS by scenario — outlier scenarios deliberately exceed ε to exercise §3 |

The Channel contract "guarantees neither delivery nor ordering" (§5.1) means loss/dup/reorder apply *inside* a live channel without closing it — that is the interesting regime for mid/ACK/retry logic.

**Constant scaling**: scenarios override `constants` to compress time (e.g. ANTI_ENTROPY_MS 300_000 → 3_000 virtual ms) — legitimate because the library reads all timing from `Constants`. P7 alone runs with production-shaped ratios.

## 6. Sim authority & host surface

The harness plays the host: a `simAuthority` implements all `AuthorityHooks` with an HMAC over JCS bytes keyed by a seed-derived secret (real asymmetric crypto adds nothing to the properties under test). It runs the full host loops so host-side protocol is exercised, not stubbed:

- finality: periodically `proposeFinality` on the authority node → "sign" → `ingestFinality`; scenarios inject cert loss (P4), stale-generation replays (`bad_cert`), and an authority restore-from-backup that must re-learn fgen (§20.9).
- directives: scripted retire/unretire, fork canonicalization after harness-injected forks (below), takeover issuance, rgen races (P8: two directives same rgen).
- fork injection: a **byzantine writer shim** that appends divergent entries at the same seq to different peers — forging at the transport level, below the library's own append path, since the library itself refuses to fork.

## 7. Quiescence

Quiescence(component) := no undelivered messages on any live link ∧ no pending timer that can produce new state (periodic idle rounds are run to fixpoint: advance through one full ANTI_ENTROPY_MS round with no state delta) ∧ all append queues, pending buffers, and retry queues empty or stable. Detected structurally by the scheduler (it owns every event source). Convergence checks (P1/P2) run only at quiescence points; availability checks (P6) run mid-turbulence.

## 8. Property checkers

| # | Check implementation |
|---|---|
| P1 | at quiescence, per partition component: equal `(topic, writer) → (contig, chain)` maps across members, excluding `'fork'`-sealed streams pre-directive |
| P2 | dump each `sqv_*` table sorted by rowKey → `sha256(JCS(rows))`, compare across schema-hash-matched peers; scope: post-watermark region on the same (cut, view-version) basis |
| P3 | harness ledger of every acked `append()` vs. union over surviving nodes of applied ∪ quarantined ∪ finality-rejected-with-anomaly; a ledger entry accounted nowhere = loss = failure |
| P4 | fault-model scenarios incl. FINALITY/WRITER_DIRECTIVE loss; assert eventual fgen/rgen repair via anti-entropy within a bounded number of rounds |
| P5 | run seed twice, compare sha256 of the full event trace (every message, timer, state transition, anomaly, in order) |
| P6 | scripted queries during recompute/unresolved conflicts must answer (provisional values) — never block or throw |
| P7 | drive the profile (100 nodes, 200 topics, 10 w/s fleet-wide, 60 s partition, 1% loss); measure virtual time from heal to P1-quiescence ≤ 120 s |
| P8 | owned lifecycle scripts: request/approve/deny/expiry, takeover, rgen races, chown chains |
| P9 | byzantine shim scenarios: all 4 detection paths, PROBE (+`unavailable`), first-applied retention, five-step §12 recovery to convergence, covered-region cert mismatch (§7.5b), `canonical_unavailable` absorbing state + superseding-directive escape |
| P10 | late-arrival scripts around the watermark: post-watermark suffix recompute; pre-watermark rejection; quarantine incl. the lost-cert window (entries applied before the cert arrives, quarantined after) |

**Trace**: every run appends structured events to an in-memory trace (dumped to JSONL on failure). The trace is both the P5 fingerprint and the debugging artifact: a failing seed replays to an identical trace, and a `--until <eventNo>` flag stops the world at any point for inspection.

## 9. Workload generator

Seed-derived scenario matrix over: topology (pair / star / mesh-K / two-cluster bridge), topic mix (append+full-sync, register with each conflict policy, ring subscribe-only, none), op mix (appends, set/del/add/remove, concurrent same-key writes from partitioned nodes — the conflict factory), subscription churn (SUB/UNSUB/reconnect with stale cursors, future cursors, beyond-ring cursors), and the fault schedule. Named scenarios (fixed seeds) cover each SPEC state machine edge in §18 at least once — the edge list is the coverage checklist; randomized scenarios explore combinations.

## 10. CI shape

1. **vectors**: replay `vectors/vectors.json` (cheapest gate, runs first — see [test-vectors.md](test-vectors.md)).
2. **types**: generate `types.d.ts` per §14 rule; typecheck + fixture-package import.
3. **fixed seeds**: the permanent regression corpus (`harness/seeds.json` — failing seeds join forever, per §19). Each entry records seed + scenario name + the property it once broke.
4. **fresh seeds**: N new random seeds per run (N sized to CI budget); failures are minimized before joining the corpus — implemented as `node tools/minimize-seed.mjs <seed> [--scenario <name>]` (greedy schedule-slicing per `harness/minimize.ts`: drop fault windows / workload segments, shrink survivors by halves, reduce node count — while the same property still fails; emits a seeds.json-shaped descriptor on stdout).
5. **P7 gate** + the separate non-gating soak profile (§19 note 10), reported as a trend metric.

## 11. Open items for milestone ①

- [ ] P1 timer seam ratified ([proposals-v3.3.md](proposals-v3.3.md)) — blocking.
- [ ] Decide `better-sqlite3` vs `node:sqlite` for the harness storage adapter (both satisfy §2; `node:sqlite` avoids a native dep at the cost of a newer Node floor).
- [ ] Trace event schema (fields per event type) — fix before P5 fingerprinting, since schema changes invalidate recorded fingerprints (values, not schema, should be the fingerprint input).
- [ ] Microtask-drain mechanism: validate the macrotask-boundary flush against `better-sqlite3`'s synchronous API (likely trivial) and against any future async storage adapter (not trivial; keep storage synchronous in v1 as SqliteHandle already implies).
