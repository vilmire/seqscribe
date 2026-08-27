# Proposed SPEC amendments — v3.5 candidates (pending owner stamp)

> Status: **proposals only — not normative** (2026-08-26). Surfaced while preparing the ADHDev integration surface. The implementation ships all three as extensions (nothing in v3.4 forbids them); ratification would pin them for second implementations.

## P1 — attach MUST reject a "full" grant on a subscribe-only topic (§14)

A `full` grant on a `replication: "subscribe-only"` topic negotiates mutual full-sync against a topic with no durable log: HAVE advertises the persistent stream heads (§14 ring head rule), the peer WANTs ranges that no responder can serve, and the deficit loop spins until anti-entropy noise settles. The topic table shape ADHDev uses (mixed full-sync and subscribe-only topics in one grants map) makes this an easy host mistake. Amendment: add to §14 `attach` — "a grant of `full` for a topic whose policy is `subscribe-only` throws (grant `serve` instead)". Implemented.

## P2 — `stats()` observability surface (§14, additive)

The host-guide's baseline metrics (per-topic fgen age, log/pending/quarantine/archive sizes, consumer cursor lag, per-peer queue depth and dirty-stream counts) need a supported API rather than SQL spelunking. Implemented as `SeqscribeNodeExt.stats(): NodeStats` — exact shape in `src/node.ts`. Amendment: add to §14 as OPTIONAL (implementations MAY omit; shape normative when present).

## P3 — cross-process ownership enforcement note (§8)

§8's "single-process ownership MUST" names the requirement but no mechanism, and SQLite's own WAL locking does not deliver it (two seqscribe processes corrupt each other's caches silently). The better-sqlite3 adapter now enforces it by holding `BEGIN EXCLUSIVE` on a sibling `<db>.lock` database — an OS-level file lock that dies with the process, so a crashed owner never wedges the DB. Amendment: one sentence in §8 — "storage adapters MUST enforce single ownership with a mechanism that survives owner crash (e.g. an OS file lock); an in-process flag alone does not satisfy this section."

---

> The two below were surfaced 2026-08-26 while implementing ADHDev integration Phase 0 (the daemon node lifecycle + the WebRTC transport binding). Both are host-resolvable, so neither blocked the integration; they are recorded because the host-side workaround is non-obvious and every embedder will hit them.

## P4 — `webSocketChannel`'s readyState test excludes every non-WebSocket transport (§14 `Channel`, DESIGN §8)

`webSocketChannel` gates sends on `ws.readyState === OPEN` where `OPEN = 1` (`src/ws.ts`). That numeric constant is a WebSocket API detail, and the two other transports a host is most likely to bring do not match it:

- **`RTCDataChannel`** — `readyState` is the **string** `"open"`. `"open" === 1` is false.
- **wrapper channel objects** (e.g. node-datachannel wrappers, as used by ADHDev's daemon) — often expose no `readyState` at all, only a predicate such as `isOpen()`. `undefined === 1` is false.

In both cases the check is *permanently* false, so `send()` never takes the direct branch and every frame — including HELLO — accumulates in the `preOpen` buffer. The `"open"` event listener that would flush it is also WebSocket-specific, so on an `RTCDataChannel` the buffer is never drained and the session never leaves the handshake. The failure is silent: no throw, no close, just a session that never reaches `ready`.

Evidence: ADHDev could not reuse `webSocketChannel` for its WebRTC path and wrote `seqscribeDataChannel` (`packages/daemon-cloud/src/daemon-p2p/seqscribe-channel.ts`) — an adapter that differs from `webSocketChannel` almost exclusively in the readiness predicate and the pre-open flush trigger. Its unit suite pins the behavior: an open channel must send on the first `send()`, and pre-open frames must flush **in order** ahead of the frame that triggered the flush.

Two candidate amendments (not mutually exclusive):

1. **Generalize the predicate.** Treat a channel as open when `readyState === 1 || readyState === "open" || (typeof isOpen === "function" && isOpen())`. Keeps one adapter for all three shapes; costs a slightly looser `WebSocketLike`.
2. **Ship `dataChannelChannel()`** as a sibling export for the `RTCDataChannel`/predicate shape, and add one sentence to DESIGN §8 stating that `webSocketChannel` is WebSocket-specific and other transports need their own adapter.

Either way, the pre-open buffer deserves a **bound**. Today it is an unbounded array, so a transport that never opens grows it without limit — a slow leak in exactly the failure mode above. ADHDev's adapter caps it (dropping with a reason callback), which is safe because sync already recovers dropped frames via anti-entropy.

**Status: implemented 2026-08-26** — predicate generalized per (1) with `dataChannelChannel` aliased per (2); pre-open buffer capped at 1,024 frames; flush also fires on the first post-open send so a transport whose `open` event predates wiring still drains, in order. Regression suite: `test/channel-adapter.test.ts` (ws/rtc/predicate shapes × immediate-send, ordered flush, silent-open, bounding). ADHDev's `seqscribeDataChannel` can now be retired in favor of the library adapter.

## P5 — `Store.init` acquires the owner lock, which makes a host's own `acquireOwnerLock()` a footgun (§8)

`SqliteHandle` exposes `acquireOwnerLock()`/`releaseOwnerLock()` as part of the public adapter surface, and host-guide §1 tells hosts to "provide the adapter and honor the ownership lock" — which reads as an instruction to take the lock. But `Store.init` (`src/store.ts`) *already* calls `acquireOwnerLock()` inside `createSeqscribe`, and `betterSqlite3Handle` tracks ownership with an in-process `owned` flag that throws `DB already owned by this process` on a second acquire.

So a host that follows the guide's apparent advice and acquires the lock before constructing the node turns **every successful open** into `ERR_DB_OWNED` — an error whose message ("owned by another process") states the opposite of what happened. It is easily misread as a real conflict, and the natural "fix" (retry, or delete the lock file) is worse than the bug.

Encountered exactly this while wiring ADHDev's `openSeqscribeNode`: the acquire looked like host diligence and produced a self-inflicted ownership error on a DB no other process had touched. The resolution is to let the library own the lock entirely and catch `ERR_DB_OWNED` from `createSeqscribe`.

Proposed, in decreasing order of preference:

1. **Make `acquireOwnerLock()` idempotent per handle** — a second acquire from the same handle is a no-op rather than a throw. The distinction the flag is trying to make (this process vs. another) is already carried by the underlying file lock.
2. **Document ownership as library-managed** in host-guide §1: hosts supply the adapter and MUST NOT call `acquire`/`releaseOwnerLock` themselves; the lock is taken by `Store.init` and released by `node.close()`.
3. **Distinguish the messages** — `ERR_DB_OWNED` for a real cross-process conflict, a distinct `ERR_MISUSE` for a double acquire from one handle, so the error text stops pointing at the wrong cause.

**Status: implemented 2026-08-26** — (1) applied to all three adapters (better-sqlite3, sqlite-wasm, Durable Object); (2) host-guide §1 rewritten (library-managed, host calls are needless no-ops, `ERR_DB_OWNED` = real conflict only); (3) subsumed by (1) — the only remaining throw is the genuine cross-process case ("lock file held"). Regression: `test/channel-adapter.test.ts` P5 block, including host-acquires-first and second-process-refused.

---

> The four below were surfaced 2026-08-26 by the wire-boundary hardening pass (structural validation extended to all 19 message types — `src/messages.ts`, regression in `test/wire-hardening.test.ts`). Each is a place where the SPEC states the sender's obligation but not the receiver's remedy, or leaves a bound unstated that a hostile peer can exploit.

## P6 — §5.2 names the credit obligation but no receiver remedy, and no flow-control ErrCode exists (§5.2, §16 error codes)

A conforming sender keeps unACKed data frames ≤ INFLIGHT_CREDITS and ACK advances only to contiguous, so any legitimate frame satisfies `mid ≤ recvContigMid + INFLIGHT_CREDITS`. A frame beyond that window is not congestion — it is a protocol violation — yet §5.2 specifies nothing for the receiver, and unbounded tolerance means unbounded `recvBuffer` growth. Implemented: over-credit frame → ERR + session close (mids and credits reset on redial, so a buggy-but-honest peer recovers). The ERR rides `ERR_ENTRY_ENCODING` because no flow-control/protocol-violation code exists. Amendment: state the remedy in §5.2 and add a distinct code (e.g. `ERR_PROTOCOL`) to the error table.

## P7 — chunked reassembly has no total bound: `of` is unbounded (§5.4 SNAP/SNAPSHOT/HAVE, §16)

Every frame is individually capped by MAX_FRAME_BYTES, but SNAP/SNAPSHOT `of` and HAVE `of` (pagination) are unbounded, so a hostile peer can stream arbitrarily many distinct in-range chunks into one reassembly map — memory bounded only by the peer's patience. PROBE `seqs` length is likewise frame-capped only (our emitter stops at 32). Implemented: chunk indices are validated into [1, of], but `of` itself is taken on faith. Amendment: a §16 constant (e.g. `MAX_REASSEMBLY_BYTES`, with reassembly abandoned + ERR beyond it) and an explicit PROBE seqs cap.

**Status: implemented 2026-08-26** — `MAX_REASSEMBLY_BYTES` (default 67_108_864 = 256 × MAX_FRAME_BYTES; ≥ the largest library-bounded legitimate body, a ring-tail SNAP of RING_DEFAULT × MAX_ROW_BYTES ≈ 43.7 MiB after base64; tunable via `resolveConstants`) enforced delta-aware on all three reassemblies — SNAP chunk maps (`src/subs.ts`), SNAPSHOT assemblies (`src/snapshot.ts`, which also un-marks the content-derived snapshotId as requested so an honest peer can re-serve it after redial), and HAVE page accumulation (`src/sync.ts`). Overflow = protocol violation: ERR + session close at every site, the same discipline as the §5.2 credit-window bound, with the abandoned map freed. PROBE `seqs` is capped at 64 at parse (`src/messages.ts` MAX_PROBE_SEQS — 2× the emitter's 32-point sweep). Regression: `test/wire-hardening.test.ts` "chunked reassembly total bound" block (under-cap completion, over-cap SNAP/HAVE/SNAPSHOT close-and-free, PROBE parse bound).

## P8 — the §1 charters admit `__proto__` (§1, §5.4)

`TOPIC_RE` and `WRITER_RE` both match `__proto__`, and wire record maps keyed by charter names (grants, HAVE vectors, cut maps) are plain objects at most merge sites — an own `__proto__` key arriving from JSON.parse becomes a prototype *set* under index-assignment or `Object.assign`. Implemented: wire-level rejection of an own `__proto__` key in every record map. Amendment: exclude `__proto__` in the §1 charters themselves so the name is invalid at every layer (author append and import included), not just on the wire.

## P9 — `SUB.params` is typed required but the wire form is optional (§5.4)

`MsgSub.params: JsonValue` is required in the §5.4 table, but the emitter passes `undefined` through `JSON.stringify`, which drops the property — so the on-wire reality is optional and a strict second implementation would reject frames ours emits. Implemented: the validator accepts an absent `params`. Amendment: mark it optional in the §5.4 table (absent ≡ no params).

---

> The seven below were surfaced 2026-08-26–27 by ADHDev Phase 2 integration (Stages 0–3 live). Unlike the wire-hardening items above, these are host ergonomics and operational observability gaps found under a long-running mixed-platform fleet. Source references describe the implementation as inspected on 2026-08-27. **Owner review 2026-08-27: all seven accepted and implemented as extensions** (statuses below); like P1–P9, the SPEC amendments themselves remain pending ratification.

## P10 — surface repeated HELLO non-response as an actionable peer signal (§5.1–5.3, §14)

A session starts a retried HELLO request and a five-second deadline (`src/session.ts:94-95`; `HELLO_TIMEOUT_MS = 5_000` at `src/constants.ts:32`). Expiry calls the same reasonless `close()` used by transport closure and protocol failure (`src/session.ts:240-250`). `manageReconnect` then schedules another dial indefinitely, with exponential backoff capped at 30 seconds; it resets the backoff only after `ready`, and `onError` covers dial/attach exceptions rather than pre-ready session closure (`src/host.ts:164-178`, `src/host.ts:196-219`). Consequently a reachable but non-speaking endpoint can remain a silent reconnect loop with no supported threshold at which the host can warn or stop dialing.

Proposal: add a reasoned pre-ready-close signal and a reconnect-level option such as `onPeerUnresponsive({ peerId, consecutiveHelloTimeouts })`, fired after configurable N consecutive HELLO timeouts. The callback should let the host alert and either continue or suppress further dialing; a successful `ready` resets the count. This is additive and need not change the default reconnect policy.

**ADHDev 실증 근거.** A dashboard endpoint was misclassified as a seqscribe peer, producing attach → five-second close → redial flapping for days without an error or warning. A library-level HELLO-unresponsive signal would have identified the classification error immediately.

**Status: implemented 2026-08-27** — every session close now carries a `SessionCloseReason` (`hello_timeout` distinct from `transport`/`protocol`/`stall`/`detach`/`node_closed`; queryable via `PeerHandleExt.closeReason()` — see P16), and `manageReconnect` gains `unresponsiveAfter` (default 3) + `onPeerUnresponsive({peerId, consecutiveHelloTimeouts})`, fired at the threshold and on each further consecutive timeout; returning `"stop"` suppresses further dialing, a `ready` session resets the count, and the default reconnect policy is unchanged when the callback is absent. Regression: `test/host-ergonomics.test.ts` P10 block (threshold + stop suppression, ready-resets-count).

## P11 — make `append` failures one asynchronous contract, or document the split (§14)

The public signature promises `Promise<EntryId>` (`src/types.ts:313-317`), but several preflight failures escape synchronously before a Promise exists: closed node, unknown topic, and JSON encoding validation (`src/log.ts:158-185`), plus raw append misuse on register topics (`src/node.ts:194-203`). Failures after enqueue — including a sealed writer, HLC stamping, complete entry validation, and the MAX_ENTRY_BYTES check — reject asynchronously (`src/log.ts:694-747`). A caller therefore needs both `try/catch` around the call and rejection handling on the returned Promise.

Proposal: prefer making all public append paths return a rejected Promise (for example, move preflight into an async boundary). If synchronous validation is intentionally retained, specify the two error phases explicitly in the API documentation and show the one safe calling pattern. Apply the same rule consistently to register mutation helpers that promise `Promise<EntryId>`.

**ADHDev 실증 근거.** The integration had to wrap appends with both synchronous `try/catch` and `.catch`/`await` rejection handling. Missing either half turns an ordinary validation failure into an uncaught exception or unhandled rejection.

**Status: implemented 2026-08-27** — every data-dependent preflight failure (closed node, unknown topic, JSON encoding) now rejects the returned Promise; SPEC v3.4 already states the rule this restores ("Promise-returning APIs reject", §14 error carriage), so no amendment text is needed beyond the §11.1 note below. Register mutation helpers inherit the same path through `core.append`. The single surviving synchronous throw is §11.1's raw append on a register topic — the SPEC pins that case as "throws", and it is a static API misuse knowable at development time, not a runtime condition; the split is documented in host-guide §1.5 with the one safe calling pattern (`await`/`.catch` only). Regression: host-ergonomics P11 block; `test/hygiene.test.ts` closed-surface expectation updated to the async contract.

## P12 — provide an opt-in JSON sanitizer for explicit `undefined` (§4 encoding, §14)

`assertJsonValue` deliberately rejects explicit undefined object properties (`src/encoding.ts:30-58`), and append runs it synchronously before enqueue (`src/log.ts:169-172`). That strictness is correct for canonical hashing, but `{ k: undefined }` is a routine JavaScript payload shape and every host that accepts loosely shaped application objects must independently normalize it.

Proposal: export an opt-in `sanitizeJson(value)` helper, or an explicit append option, that recursively removes undefined object properties before validation while retaining the strict default. Array behavior must be specified without changing positions — preferably reject undefined array elements (or convert them to `null` only under an explicitly named mode). The result must still pass `assertJsonValue` and JCS unchanged thereafter.

**ADHDev 실증 근거.** ADHDev ledger payloads contained explicit undefined properties, so the integration implemented its own recursive stripping pass before append. A canonical library helper would remove duplicated, easy-to-diverge normalization code.

**Status: implemented 2026-08-27** — `sanitizeJson(value)` exported from the package root: recursively drops undefined object properties into a fresh non-mutated tree, throws `ERR_ENTRY_ENCODING` on undefined array elements (positions are load-bearing under §4 — no silent shift or null coercion; the proposal's named-mode conversion was not taken) and on everything else `assertJsonValue` rejects; an own `__proto__` key survives as data (built via `Object.fromEntries`, never index-assignment). Output passes `assertJsonValue` and JCS unchanged. The strict append default stands. Regression: host-ergonomics P12 block (recursive strip, array rejection, `__proto__`, end-to-end append).

## P13 — expose append-oriented entry-size estimation before allocation (§2, §16)

The limit is `MAX_ENTRY_BYTES = 65_536` (`src/constants.ts:5`) and enforcement measures the JCS serialization of the complete `LogEntry` (`src/codec.ts:106-115`). `assertEntrySize` is already exported (`src/index.ts:36-44`), but it accepts a fully constructed `LogEntry`; an append caller does not yet know library-owned fields such as writer, seq, HLC, and chain. The actual failure therefore arrives only after enqueue when the library constructs and validates the entry (`src/log.ts:716-737`).

Proposal: export an append-oriented helper such as `estimateEntryBytes({ topic, kind, payload, key?, causal?, ref? }, context?)`, returning an exact value when node context supplies the next-entry metadata, or a documented conservative upper bound otherwise. A companion `canAppend` result may include the configured limit and estimated bytes. `assertEntrySize` remains the authoritative final check.

**ADHDev 실증 근거.** ADHDev projections implemented their own byte cap and digest-only downgrade path so oversized materialized payloads could be reduced before append. A supported estimator would make that fallback predictable and reusable.

**Status: implemented 2026-08-27** — `estimateEntryBytes(shape, ctx?)` exported (`shape` = the caller-known half: topic/kind/payload/key?/causal?/ref?; `ctx?` = writer/seq/hlc): exact when ctx supplies the next-entry metadata (chain is fixed-width 64 hex), a monotone conservative upper bound otherwise (max-length writer, 16-digit numeric fields). Compare against `Constants.MAX_ENTRY_BYTES`; the companion `canAppend` was not added — the estimator plus `resolveConstants()` covers it without new surface. `assertEntrySize` at commit remains authoritative. Regression: host-ergonomics P13 block (exactness, bound monotonicity, over-limit pre-flagging).

## P14 — define a supported runtime topic activation/reopen protocol (§6 schema negotiation, §14)

The local registry can add a previously unknown topic at runtime: `defineTopic` delegates to `TopicRegistry.define`, which only forbids changing the policy of an existing topic (`src/node.ts:188-192`, `src/topics.ts:82-98`). The operational limitation is peer activation: grants must name already defined topics at attach (`src/node.ts:211-215`), and the host guide consequently requires topic definitions before attach (`docs/host-guide.md:28`). There is no node-level API or documented choreography for a topic discovered after boot to become negotiated across existing peers; hosts must coordinate definitions and detach/reattach (or reopen nodes) themselves.

Proposal: either add a runtime topic-definition/activation API that coordinates schema-hash negotiation and session grant refresh, or document an official reopen/reattach pattern with ordering, failure, and mixed-schema behavior. Keep policy replacement a coordinated fleet schema change; this proposal concerns adding a new topic, not mutating an existing definition.

**ADHDev 실증 근거.** ADHDev creates mesh containers after process boot. Local `defineTopic` alone could register their topics, but could not make those topics available on already established daemon sessions, forcing lifecycle choreography outside the library.

**Status: resolved 2026-08-27 (via P15 + documented choreography)** — runtime activation is now: `defineTopic` on both endpoints, then `updateGrants` on each side's handle (or `ReconnectHandle.updateGrants`, which also covers future redials). The P15 re-advertisement carries schema hashes, recomputes refusals, and triggers an immediate HAVE round — no detach/reattach, no node reopen. Ordering is free (whichever side updates last completes the mutual pair; until then the topic simply isn't mutual), mixed-schema behavior is per-topic refusal (ERR_SCHEMA_MISMATCH) with the session staying ready. Choreography documented in host-guide §1.5. Mutating an *existing* topic's policy remains a coordinated fleet schema change, exactly as before.

## P15 — re-advertise grants when topics become available on an established session (§5.4 HELLO, §14)

Attach grants are fixed session input (`src/types.ts:320-327`, `src/sync.ts:164-190`). HELLO serializes that grant map with each topic's current schema hash (`src/session.ts:254-260`), then stores the peer's grants once; after `ready`, duplicate HELLO is ignored (`src/session.ts:309-334`). `defineTopic` neither updates sessions nor emits a control message. Thus a topic defined after handshake cannot become mutually full/servable until a new session is attached, even if both endpoints now have matching definitions.

Proposal: add a versioned grant-update/re-advertisement mechanism for established sessions. A safe design must validate newly named local topics, carry schema hashes, recompute refused/mutual relations, and trigger an immediate HAVE round for newly mutual-full topics. If renegotiation is intentionally out of scope, expose and document an explicit `reattachForGrants` operation rather than leaving mutation of the original grants object as an apparent but ineffective path.

**ADHDev 실증 근거.** ADHDev Stage 2 defines container topics lazily, which conflicts with attach-time grant snapshots. The integration recorded this as a gap that must be closed before Stage 4 dynamic topic rollout.

**Status: implemented 2026-08-27** — versioned grant re-advertisement on established sessions. `PeerHandleExt.updateGrants(grants)` replaces the session's grant map (full replacement, revalidated under the §14 attach ACL guard including defined-before-grant), bumps a `grantsGen` carried in HELLO (extension fields `grantsGen`/`ackGen`, absent ≡ 0 / not-an-ack), and re-advertises via the §5.3 request machinery — retried every CONTROL_RETRY_MS until the peer answers with `ackGen === grantsGen`, so a dropped frame cannot silently strand the update. The receiver applies gen-newer maps (gen dedup makes handshake retries no-ops), recomputes refusals from the current grant pair (ERR_SCHEMA_MISMATCH per newly refused topic; the session stays ready), and each applied update triggers an immediate HAVE round on both sides, so pre-existing backlogs on the new topic converge without waiting for anti-entropy. Mutating the original attach-time grants object is now provably ineffective (the session copies it). `manageReconnect` accepts a `grants` thunk and `ReconnectHandle.updateGrants` so redials do not regress to attach-time grants. Interop: pre-P15 peers ignore the extension fields and post-ready HELLOs — the update then never acks (1 frame per CONTROL_RETRY_MS until close); both endpoints must run ≥ this build for runtime activation, which is the vendored-fleet reality. Regression: host-ergonomics P14/P15 block (live activation < ANTI_ENTROPY_MS, backlog via triggered HAVE round, mismatch refusal without close, ACL revalidation).

## P16 — make peer-session lifecycle events complete and reasoned (§14)

There is a partial hook today: `PeerHandle` exposes `state()` and `onStateChange(attached | ready | closed)` (`src/types.ts:217-222`). It is not a complete lifecycle feed. The session is already `attached` before the handle is returned, and listener registration does not replay current state (`src/session.ts:52-53`, `src/session.ts:102-109`, `src/sync.ts:211-216`), so a normal caller cannot observe the initial attached transition. `close()` emits only `closed`, without cause (`src/session.ts:240-250`), making HELLO timeout, remote channel closure, protocol rejection, stall, and explicit detach indistinguishable. `manageReconnect` consumes ready/closed internally but exposes no per-attempt lifecycle callback (`src/host.ts:196-219`).

Proposal: formalize a reasoned lifecycle event surface, for example `{ peerId, event: "attached" | "ready" | "closed" | "hello_timeout", attempt, reason? }`, with defined delivery/replay semantics. At minimum, return-time subscribers must be able to record attach, and closed events must identify HELLO timeout versus transport/protocol/host detach. The reconnect helper should forward the same events rather than requiring another wrapper.

**ADHDev 실증 근거.** ADHDev added attach/ready/close logging in its glue and used those logs to find two live integration defects. The diagnostic value was demonstrated in production, but every host currently has to recreate an incomplete version of the same instrumentation.

**Status: implemented 2026-08-27** — `attach` now returns `PeerHandleExt` (a strict superset of the §14 `PeerHandle`): `onLifecycle(cb)` delivers `{peerId, event: "attached"|"ready"|"closed", reason?}` with **replay-on-subscribe** (the full event history is delivered synchronously at registration, so a return-time subscriber records the initial attached transition), and `closed` always carries a `SessionCloseReason` (`hello_timeout` / `transport` / `protocol` / `stall` / `detach` / `node_closed`), also queryable via `closeReason()`. The proposal's separate `"hello_timeout"` event value became `closed` + `reason` — one terminal event with the cause attached. `manageReconnect` forwards the same feed via `onEvent` with the attempt number, so no wrapper is needed. `onStateChange` is unchanged for compatibility. Regression: host-ergonomics P16 block (replay for return-time and post-close subscribers, reason discrimination across close paths).
