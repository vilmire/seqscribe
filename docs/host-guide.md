# seqscribe host integration guide — the host's contract obligations

> Status: **non-normative companion to SPEC v3.2** (2026-08-26). The SPEC deliberately pushes identity, trust, signing, and adjudication to the host; those obligations are stated where each mechanism is defined and are therefore scattered. This document collects them into one place, per host role, with runbooks for the situations where the host is the only actor that can act. Where this guide and the SPEC disagree, the SPEC wins. This is the *generic* host contract — ADHDev-specific integration lives in the ADHDev repo (DESIGN §9.5).

## 0. Helpers that implement this guide (src/host.ts)

Every obligation below remains yours, but the common shapes ship as helpers — a shared-secret fleet wires up in a few lines:

| Helper | Implements |
|---|---|
| `hmacAuthority({authorityId, secret, governs?})` | the full `AuthorityHooks` set (sign/verify finality, directives incl. §14 role binding via `governs`, takeover) with HMAC-SHA256 over JCS bytes |
| `startFinalityLoop(node, {topics, authority, intervalMs})` | the §7.4 propose→sign→ingest cadence (authority host only) |
| `manageReconnect(node, {dial, peerId, peerClass, grants})` | §5.2's "host reconnects": jittered exponential backoff, reset on healthy sessions |
| `loadOrCreateWriterId(storage, {prefix})` | stable per-machine id persisted in sq_meta (clone/restore procedures must delete the row — see §7) |
| `migrateLegacyJsonl(node, topic, lines, {kind})` | genesis migration of pre-seqscribe JSONL logs as fresh appends |
| `webSocketChannel(ws)` / `betterSqlite3Handle(db, lockDb?)` / `sqliteWasmHandle(db)` / `durableObjectSqlHandle(sql, txn)` | transport + storage adapters (Node, browser wasm, Durable Objects); `lockDb` enables the crash-safe cross-process owner lock |
| `httpBeaconTransport(base, account, token?)` / `beaconFetchHandler({token})` | the §14 beacon wire, client and Workers/DO-deployable server |
| `node.stats()` | the §8 monitoring baseline below, one call |

## 1. What the host must bring (before the first byte syncs)

| Obligation | Contract point | Notes |
|---|---|---|
| **writerId issuance** | §1, §4.1 | Canonical, stable, ASCII-chartered ids. The host is the identity authority: never reuse an id across machines; never re-issue an id after quarantine/retirement (a returning id is recognized forever — §8 permanent registry). Machine clone/restore ⇒ new writerId, or you have manufactured a fork by construction (§6.4). |
| **Connected, authenticated channels** | §5, `Channel` | The library sees only `send/onMessage/onClose`. Discovery, auth, NAT traversal, reconnect-with-backoff are yours. On stall-close (§5.2) the library closes; *you* reconnect. |
| **Grants & peerClass per attach** | §4.1, §14 `attach` | **Granting `full` on a topic = trusting that peer to write arbitrary content under any writerId.** Entries are unsigned; grants are the only containment boundary. `metadata`-class peers (e.g. cloud relays) must never be granted content topics — the library throws, but the classification decision is yours. |
| **Storage adapter + single-process ownership** | §8, `SqliteHandle` | Provide the adapter and honor the ownership lock. Two processes on one DB is corruption, not degraded mode. |
| **Topic definitions before attach** | §14 | Policies are immutable per process; changing one is a fleet schema change (§6 below). |

## 2. Authority operations (the heaviest role)

An authority id is a **role, not a machine** (§7.3). Operating one means:

- **Full replica requirement**: the authority must hold every entry it certifies. A content topic cannot be certified by a metadata-class peer. Finality stalls when the authority's replica is stale — that's the first thing to check when cuts stop advancing (§7.3 diagnosis order: replica stale? → empty window? → outlier exclusion?).
- **Key management behind a stable id**: rotate *keys* freely behind the id (your `verifyFinality`/`verifyWriterDirective` implementations define what verifies). Rotating the *id* is deliberately expensive — it is part of topicSchemaHash and requires a coordinated fleet upgrade (§20.9). Plan key custody so id rotation never becomes necessary.
- **Role binding in verifiers**: `verifyWriterDirective` MUST check that *this* authority governs *this* (topic, writer) — signature-only verification lets any valid signer retire arbitrary writers (§14). Same discipline for `verifyFinality` and `verifyTakeover`.
- **Generation counters are durable authority state** (§20.9). Restoring the authority host from backup rewinds `generation` and the fleet rejects everything as `bad_cert`. **Recovery procedure**: before the first post-recovery issuance, read the fleet's highest `fgen` from HAVE exchanges and continue strictly above it. Automate this — it is not optional hygiene, it is the difference between recovery and a bricked topic.
- **Issuance loop**: on a cadence (or watermark-lag trigger), `proposeFinality(topic)` → sign → `ingestFinality(cert)`. Never reimplement the §7.2 P computation — the library computes, you sign. `null` proposal = empty qualifying window = correct silence, not an error.
- **Sizing FINALITY_WINDOW_MS**: two opposing pressures — every full replica stores the whole window (~26M rows/topic at the P7 profile, §20.8), but a writer asleep past the window loses its finalized-out tail *and its identity* (§20.5: post-cut quarantine → new writerId, owned keys bulk-chown, causal history reset). Size for your fleet's worst realistic sleep, then provision storage for it.

## 3. Fork adjudication runbook (§12 — the host is the judge)

The library detects, freezes, and gathers evidence; it never chooses a branch. When `onAnomaly('writer_forked')` fires:

1. **Collect** — from every affected node: its PROBE evidence (last common point per node) and anomaly reports. *Collection transport is explicitly your job* (§12) — the fleet's forked views of the stream cannot agree by themselves, so evidence must travel out-of-band (your control plane, not seqscribe).
2. **Decide the canonical branch** — usually the majority/coordinator-held branch; the SPEC does not care which, only that you pick one.
3. **Compute finalSeq = the minimum divergence point across affected nodes** — a directive above some node's divergence point leaves that node unable to comply.
4. **Issue** `WriterDirective {state:"retired", finalSeq, finalChain, rgen: prev+1}` via `publishWriterDirective`. Nodes run the five-step recovery; the writer's *owner* continues under a **new writerId** you issue (the old id is permanently sealed).
5. **If `canonical_unavailable` fires** (no peer holds the canonical range): issue a superseding directive (rgen+1) with finalSeq = the actually-available point — shrink canonical to reality — or accept a permanently sealed stream.
6. **Leaving a fork unresolved is legitimate** (§20.4) for topics where divergence is tolerable; it is a standing exclusion from P1/P2 convergence, so record the decision.

Post-cut quarantine (§7.5a — a writer's applied tail fell outside a late-arriving cut) resolves through the *same* directive machinery: canonicalize at the cut, new writerId for the author.

## 3.5 View reducer discipline (measured)

`reduce` is pure and non-mutating (§9), so a naive `{...state, key: v}` reducer copies the whole state per entry — **O(state) per entry, quadratic per topic**. Measured: a 20k-key spread-copy reducer costs ~35 s for 20k entries, of which the library's share is noise (table writes are diffed to O(changes) per batch). For views whose state grows with the log:

- shape state so each entry touches a small sub-object (e.g. `{buckets: {…}}` and copy only the affected bucket), or
- provide `delta` and keep rows-per-entry small — `rows()` is still authoritative and verified at checkpoints.

Small/bounded views (config mirrors, counters) can ignore this entirely.

## 3.6 Browser deployment shape (verified)

Run the node in a **dedicated module worker**: sqlite-wasm with the OPFS
SAH-pool VFS (persistent, no COOP/COEP headers needed), the WebSocket, sync,
views, and subscriptions all live there; the main thread only renders worker
messages. Verified in Chromium (`npm run e2e:dashboard`, e2e-browser/): a
live ADHDev-shaped dashboard — full-synced `mesh.events` with a LOCAL ledger
view (offline-readable, resumes from OPFS across reloads), `fleet.status`
ring-tail SUB, `config.settings` register SUB — sustained a 400-query burst
in the worker with **zero main-thread long tasks** (Long Tasks API). Running
the node on the main thread works but blocks the UI for the duration of every
query and commit — don't ship that shape.

## 4. Consumer discipline: provisional vs finalized-only

Every `onEntry` delivery is **provisional until covered by a certificate** (§7.5). Choose per consumer:

| Consumer profile | Mode | Mechanism |
|---|---|---|
| Idempotent / rebuildable effects (view-like, caches, UI) | provisional (default) | just consume; quarantines self-heal via rebuilds |
| Irreversible side effects, latency-tolerant | finalized-only | gate on `finality(topic)`; consume only entries with order ≤ watermark; accept up to FINALITY_WINDOW_MS latency (default 30 d) |
| Irreversible **and** timely | provisional + **your own compensation** | `entry_quarantined` is best-effort hygiene, not a correctness channel (offline consumers miss it, §7.5); no spec mechanism removes this trade-off — budget for host-level compensation logic |

Also: keep consumers draining. A consumer idle past FINALITY_WINDOW_MS is dropped (`consumer_abandoned`) and resumes with a cursor reset to post-cut — silently skipping the archived middle.

## 5. owned keys, requests, takeover

- Configure `verifyTakeover` + `verifyWriterDirective` before defining any topic with an `owned` policy (defineTopic throws otherwise).
- Approval flow: `onOwnedRequest` callback on the owner node returns approve/deny; approval appends the set with `ref` = request id. Requests expire after REQUEST_TTL_MS (query-time derivation — no timer to operate).
- **Takeover is your emergency power** (owner machine dead): `RegisterHandle.takeover` builds the entry, `issueTakeover` signs it. The same trust caveat as directives: your signer decides who may seize which keys — bind roles, not just signatures.
- Remember the cost you opted into: owner offline = key unwritable (per-key CP). The SPEC's advice stands — natural-affinity keys only, never the default.

## 6. Fleet schema changes (the coordinated-upgrade list)

These are **not** rolling-deploy-safe; mismatched peers refuse the topic (`ERR_SCHEMA_MISMATCH`) until the fleet converges:

- conflict policy (default or overrides, incl. glob set), register semantics version
- `finalityAuthority` id (§20.9 — includes replacing a dead authority)
- topic kind

Grants, retention, replication mode, access class, throttles, and constants are *not* part of topicSchemaHash — but retention/replication/access changes still deserve fleet coordination for operational sanity, and **view `version` bumps** need a state-provisioning plan: a version bump with no pre-cut logs anywhere makes `bootstrapPartial` permanent (§7.8).

## 7. Backup & restore truth table

| Asset | Restore safety |
|---|---|
| A replica's DB | safe — worst case it re-syncs; its *own* writer streams are the exception below |
| A node's own writer stream | restoring an old copy and appending = **fork by construction**. After any restore, either verify the stream head matches the fleet (HAVE) before appending, or issue a new writerId |
| Authority durable state | **dangerous** — generation rewind bricks certification until the fgen re-learn procedure (§2 above) runs |
| Derived views / checkpoints | free — always rebuildable |
| Sole-copy entries | the real loss risk: a writer's unsynced tail exists nowhere else. `staleness()` + beacon sole-copy awareness (DESIGN §5.7) exist to make this visible before shutdown |

## 8. Monitoring — anomaly severity guide

Wire `onAnomaly` into your alerting with roughly this triage:

| Anomaly | Severity | Meaning / action |
|---|---|---|
| `writer_forked` | **page** | adjudication runbook (§3) — requires a human/host decision |
| `bad_cert`, `bad_directive` | **page** | authority key compromise, generation rewind, or a bug — never expected in healthy operation |
| `canonical_unavailable` | **page** | recovery is stuck in the absorbing state; only a superseding directive escapes |
| `entry_quarantined`, `pre_finality_rejected` | warn | data fell outside finality — expected under long sleeps; investigate window sizing if frequent |
| `clock_outlier` | warn | a peer's clock is broken (> ε); also alarm on `hlc.l − now > CLOCK_WARN_MS` locally |
| `owned_violation`, `takeover_invalid` | warn | misconfigured writer or attempted overreach |
| `consumer_abandoned` | warn | a consumer slept past the window and lost its place |
| `delta_mismatch`, `view_faulted` | error (dev) | a ViewDef bug — delta disagrees with rows(), or a row violates limits; fix the definition, rebuild |

Baseline metrics worth exporting: per-topic fgen age (finality staleness), per-writer contig lag vs. fleet max, pending/quarantine table sizes, send-queue drop counts.
