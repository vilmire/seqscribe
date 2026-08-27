# seqscribe host integration guide — the host's contract obligations

> Status: **non-normative companion to SPEC v3.4** (2026-08-26). The SPEC deliberately pushes identity, trust, signing, and adjudication to the host; those obligations are stated where each mechanism is defined and are therefore scattered. This document collects them into one place, per host role, with runbooks for the situations where the host is the only actor that can act. Where this guide and the SPEC disagree, the SPEC wins. This is the *generic* host contract — ADHDev-specific integration lives in the ADHDev repo (DESIGN §9.5).

## 0. Helpers that implement this guide (src/host.ts)

Every obligation below remains yours, but the common shapes ship as helpers — a shared-secret fleet wires up in a few lines:

| Helper | Implements |
|---|---|
| `hmacAuthority({authorityId, secret, governs?})` | the full `AuthorityHooks` set (sign/verify finality, directives incl. §14 role binding via `governs`, takeover) with HMAC-SHA256 over JCS bytes |
| `startFinalityLoop(node, {topics, authority, intervalMs})` | the §7.4 propose→sign→ingest cadence (authority host only) |
| `manageReconnect(node, {dial, peerId, peerClass, grants, onPeerUnresponsive?, onEvent?})` | §5.2's "host reconnects": jittered exponential backoff, reset on healthy sessions; `onPeerUnresponsive` flags an endpoint that dials but never speaks seqscribe (v3.5-P10), `onEvent` forwards the reasoned lifecycle feed (v3.5-P16), `grants` accepts a thunk and the handle has `updateGrants` for runtime topics (v3.5-P15) |
| `sanitizeJson(value)` | opt-in normalizer for loose payloads: drops undefined object properties, rejects undefined array elements; output passes `assertJsonValue`/JCS unchanged (v3.5-P12) |
| `estimateEntryBytes(shape, ctx?)` | append-oriented size check BEFORE enqueue — exact with writer/seq/hlc context, conservative upper bound without; compare against `MAX_ENTRY_BYTES` (v3.5-P13) |
| `node.resetConsumer` / `deleteConsumer` / `listConsumers` / `pruneConsumers` | durable-consumer lifecycle: same-name rebuild replay (with archived-coverage metadata), explicit cursor deletion + GC — all inactive-consumer operations (v3.5-P17/P18) |
| `node.consumerCaughtUp(topic, consumer)` | deterministic readiness gate: resolves once every entry through the call-time head has completed its callback — replaces `lagRows === 0` polling (v3.5-P19) |
| `node.scanEntries(topic, opts)` / `node.headOrder(topic)` | bounded read-only scans: canonical order with a pinned `through` interval, or per-writer seq ranges spanning the cold archive; never creates a cursor (v3.5-P21) |
| `loadOrCreateWriterId(storage, {prefix})` | stable per-machine id persisted in sq_meta (clone/restore procedures must delete the row — see §7) |
| `migrateLegacyJsonl(node, topic, lines, {kind})` | genesis migration of pre-seqscribe JSONL logs as fresh appends |
| `webSocketChannel(ws)` (= `dataChannelChannel`) / `betterSqlite3Handle(db, lockDb?)` / `sqliteWasmHandle(db)` / `durableObjectSqlHandle(sql, txn)` | transport + storage adapters — the socket channel accepts WebSocket, RTCDataChannel, and `isOpen()` wrappers (v3.5-P4); `lockDb` enables the crash-safe cross-process owner lock |
| `httpBeaconTransport(base, account, token?)` / `beaconFetchHandler({token})` | the §14 beacon wire, client and Workers/DO-deployable server |
| `node.stats()` | the §8 monitoring baseline below, one call |

## 1. What the host must bring (before the first byte syncs)

| Obligation | Contract point | Notes |
|---|---|---|
| **writerId issuance** | §1, §4.1 | Canonical, stable, ASCII-chartered ids. The host is the identity authority: never reuse an id across machines; never re-issue an id after quarantine/retirement (a returning id is recognized forever — §8 permanent registry). Machine clone/restore ⇒ new writerId, or you have manufactured a fork by construction (§6.4). |
| **Connected, authenticated channels** | §5, `Channel` | The library sees only `send/onMessage/onClose`. Discovery, auth, NAT traversal, reconnect-with-backoff are yours. On stall-close (§5.2) the library closes; *you* reconnect. |
| **Grants & peerClass per attach** | §4.1, §14 `attach` | **Granting `full` on a topic = trusting that peer to write arbitrary content under any writerId.** Entries are unsigned; grants are the only containment boundary. `metadata`-class peers (e.g. cloud relays) must never be granted content topics — the library throws, but the classification decision is yours. |
| **Storage adapter + single-process ownership** | §8, `SqliteHandle` | Provide the adapter — **the library takes and releases the ownership lock itself** (`createSeqscribe` acquires, `node.close()` releases); do NOT call `acquireOwnerLock`/`releaseOwnerLock` yourself (a stray call is a harmless no-op since v3.5-P5, but it is never needed). Two processes on one DB is corruption, not degraded mode — catch `ERR_DB_OWNED` from `createSeqscribe` as the real-conflict signal. |
| **Topic definitions before grant** | §14 | Every topic named in a grant map must be defined first — at attach, and equally at `updateGrants` (v3.5-P15). Policies are immutable per process; changing one is a fleet schema change (§6 below). Topics *discovered after boot* no longer force a reattach — see §1.5. |

## 1.5 Host ergonomics added by v3.5 P10–P16 (verified under the ADHDev fleet)

**Append failure contract (P11).** Every data-dependent failure of `append` and the register mutation helpers — closed node, unknown topic, encoding, sealed writer, oversized entry — **rejects the returned Promise**. The one safe calling pattern is therefore just `await node.log(t).append(...)` inside your normal async error handling (or `.catch`); no synchronous `try/catch` is needed for runtime conditions. The only synchronous throws left on the write path are *static API misuse*, knowable while you write the code: raw `append` on a register topic (§11.1 — SPEC-pinned), `register()` on a non-register topic, and `takeover` without `issueTakeover` configured. Payloads with explicit `undefined` properties are still rejected (canonical hashing demands it) — run `sanitizeJson` first when your payloads are loosely shaped, and use `estimateEntryBytes` when you need a digest-downgrade path *before* an `ERR_ENTRY_TOO_LARGE` rejection.

**Runtime topic activation (P14/P15).** A topic discovered after boot activates on live sessions without any detach/reattach:

1. `defineTopic(topic, policy)` on **both** endpoints (your control plane coordinates this — same policy, or the topic will be refused with `ERR_SCHEMA_MISMATCH` while the session stays ready).
2. `handle.updateGrants(fullNewGrantMap)` on **each side's** handle — it is a full replacement of the attach-time map, validated exactly like attach (defined-before-grant, metadata/content, no `full` on subscribe-only). Under `manageReconnect`, call `ReconnectHandle.updateGrants` instead (or pass `grants` as a thunk) so redials carry the new map too.

Ordering between the two sides is free: the topic becomes mutual when the second side's update lands, and the library immediately runs a HAVE round so existing backlogs converge without waiting for anti-entropy. The re-advertisement is retried until acknowledged, so frame loss cannot strand it. Mutating the grants object you passed to `attach` does nothing (the session copies it) — `updateGrants` is the only path. Both endpoints must run a v3.5-P15 build.

**Session lifecycle & the unresponsive-peer signal (P10/P16).** `attach` returns a `PeerHandleExt`: `onLifecycle(cb)` replays history on subscribe (you always see `attached`) and every `closed` names its `SessionCloseReason` — wire this into your logging; it is the instrumentation ADHDev had to hand-roll. Triage: `hello_timeout` = the endpoint is reachable but not speaking seqscribe (misclassified URL, wrong port — check your peer classification, not the network); `transport` = the channel died (ordinary — reconnect handles it); `protocol` = the peer violated the wire contract (a bug or a hostile peer — worth a log line with the preceding ERR frame); `stall` = no ACK progress for CHANNEL_STALL_MS; `detach`/`node_closed` = you. Under `manageReconnect`, set `onPeerUnresponsive` (default threshold 3 consecutive HELLO timeouts) and alert from it — return `"stop"` to stop dialing a misclassified endpoint instead of flapping forever.

## 1.6 Multi-process hosts — one owner, delegated writes (v3.5-P20)

The ownership rule (§1, §8) is absolute: **exactly one process opens `seqscribe.db`**; two full nodes on one database is corruption, not degraded mode, and a second "read-only" SQLite open of the same file is equally unsupported (the owner's caches and WAL assumptions do not survive a second reader). When your application's legitimate writers live in several local processes, the supported shape is **delegation to the owner over IPC** — verified as the gap in ADHDev's daemon/mcp-server split, where a bypassed second-process write degraded into parity-driven repair work:

- **Pick the owner.** One long-lived process (usually your daemon) calls `createSeqscribe` and holds the node. Every other process holds an IPC client, never the DB path.
- **Append intents, not appends.** The client sends `{topic, kind, payload, idempotencyKey}` over any local IPC (unix socket, pipe, localhost HTTP). The owner appends and returns the `EntryId`. The **idempotency key is mandatory**: IPC is at-least-once under timeouts/retries, so the owner keeps a bounded `idempotencyKey → EntryId` map (a small table or LRU) and answers duplicates with the original id — that turns retried intents into exactly-once appends. Run intent payloads through `sanitizeJson` and `estimateEntryBytes` on the client so oversize/encoding failures are rejected before the hop.
- **Backpressure.** The owner should bound its intent queue (`stats()` exposes pending/queue depths) and answer over-bound intents with a retryable rejection; clients treat any non-duplicate failure as retryable-with-backoff, never as "write directly".
- **Owner unavailable.** Either fail fast to the caller or spool intents durably (append-only file) and drain on reconnect — the spool preserves the idempotency keys. **Never** fall back to opening the DB; `ERR_DB_OWNED` from such an attempt is the guard working, not a bug.
- **Reads.** Delegate them the same way: the owner can serve `scanEntries` pages, subscription snapshots, or view queries over the IPC. This keeps the single-process invariant while giving auxiliary processes bounded, race-free reads (P21's pinned `through` works across the hop).

A library-level auxiliary-writer client and a true read-only open are deliberately not shipped yet — the pattern above covers the known deployments, and their surface should be shaped by the first production consumer that outgrows it (proposals-v3.5 P20 status).

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

Lifecycle (v3.5-P17–P19): rebuild a consumer's derived state by `unsubscribe → resetConsumer(topic, name) → onEntry(topic, name, …)` — the same stable name replays, and the result's `archivedRows` tells you when "start" is the archive floor rather than genesis (completeness below the cut is the snapshot's job, not replay's). Gate cutovers on `consumerCaughtUp(topic, name)` instead of polling `lagRows`; it resolves only after every entry through the call-time head has completed its callback. Retire versioned/renamed consumers with `deleteConsumer`/`pruneConsumers` — leftover cursors hold the archive floor until they age into `consumer_abandoned`.

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
| `sync_stalled` | warn | WANT rounds toward a peer stopped progressing (v3.5-P22) — typically the peer serves rows your finality certificate rejects (voided rows, a pre-finality build's leftovers); check `stats().topics[t].applyRejects` for the reject histogram and `stats().peers[].stalledStreams`. The stream retries at anti-entropy cadence; resolution usually needs the peer repaired or re-based |
| `delta_mismatch`, `view_faulted` | error (dev) | a ViewDef bug — delta disagrees with rows(), or a row violates limits; fix the definition, rebuild |

Baseline metrics worth exporting: per-topic fgen age (finality staleness), per-writer contig lag vs. fleet max, pending/quarantine table sizes, send-queue drop counts, and (v3.5-P22) per-topic `applyRejects` counters plus per-peer `stalledStreams` — a growing `rejected_finality` count is the early signature of a peer serving rows your certificate voids.
