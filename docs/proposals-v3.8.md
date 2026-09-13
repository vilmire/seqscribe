# SPEC amendments — P30–P36 (P30–P33, P35–P36 ratified as v3.7; P34 open)

> Status: **P30–P38 all RATIFIED as SPEC v3.7** (2026-09-13) — applied in [SPEC.md](../SPEC.md) §5.2/§5.7/§5.7a/§7.6/§11.1/§11.6/§14/§14.1/§14.2 and stamped in [CHANGELOG.md](../CHANGELOG.md). P34 was initially deferred here for want of a shape; it landed once the missing input was identified (board completeness is host-supplied because `BeaconTransport` carries no truncation signal). This file is retained as the amendment rationale record; the per-item "Status" markers record what was actually built, including where it diverged from the proposal.
>
> Ratification followed the v3.5/v3.6 discipline: the SPEC text was written from the **implementation**, not from the proposals above. Two places where the two disagreed, with the SPEC following the code: P31's `readIntervalStats()` copies each counter object (the proposal did not mention it — the defect was found while implementing, and the SPEC now states the MUST), and P33 landed as the in-source caller warning plus normative §11.1/§14 text, with the proposal's remedy (2) explicitly not taken.
>
> Discovery path for this cycle: **reading the embedder's integration layer instead of waiting for it to report.** Every previous cycle arrived as a report — a failing run (v3.4), an incident or a blocked phase (v3.5/v3.6), a README that could not be written (v3.7). This one came from reading the ~11,400 lines of host glue in the production integration's `seqscribe/` directory and asking a different question: *where is the host paying a standing cost the library could absorb?* That cost is visible in the glue's own comments, which document each workaround and why it was necessary. None of the five below was ever filed as a request — the host worked around them and moved on, which is exactly why they persisted.
>
> **All five are host-resolvable** (each already has a shipped workaround). None blocks anyone. They are recorded because the workarounds are non-obvious, several were arrived at only after a defect, and every embedder hitting the same shape will pay the same cost.

---

## P30 — `BeaconHandle` has no host-callable push, and hint derivation is private (§5.7, §14.2)

`BeaconHandle` exposes exactly one member:

```ts
interface BeaconHandle { stop(): void; }
```

The only push trigger in the library is `notifyApplied()`'s `BEACON_DEBOUNCE_MS` debounce after an append (`src/beacon.ts`). **A host cannot ask the beacon to publish now.** That matters on reconnect: a node that has just re-attached wants its vector on the board immediately, not on the next append's debounce — and a node with no pending writes never publishes at all.

This gap is the reason **P28 existed**. The host drove `stop()` then `start()` on every reconnect to force a fresh publish, which is what surfaced the one-way latch. P28 fixed the latch, but the need that produced the pattern — "publish now" — still has no API, so the workaround it exposed is still the only route.

The second half is worse, because it silently degrades a feature two previous cycles were spent fixing. `BeaconHub.buildHints()` is **private** (`src/beacon.ts:150`), so a host that publishes its own report cannot produce the P27 `hints` map. The integration's beacon wrapper documents the consequence:

> `pushNow()` bypasses the library's private `buildHints()`. Preserve the last library-produced, already-projected hint set so reconnect re-seeding does not erase it from the server board; the next applied write refreshes it.

So the host caches whatever hints the library last produced and replays them on each manual push — stale by construction, and the only alternative is publishing a report that **deletes** the node's hints from the board. P26, P27 and P28 hardened the beacon across two stamped revisions; the host still cannot reach that work through a supported call.

**Amendment**: add to §14.2 —

```ts
interface BeaconHandle {
  stop(): void;
  pushNow(): Promise<void>;   // publish immediately, bypassing the debounce
}
```

`pushNow()` MUST build its report through the same path as the debounced push — identical `vectors()` snapshot and identical `hints` derivation, honoring each topic's `hintKeys` policy — so a host-initiated publish is byte-indistinguishable from a library-initiated one. It MUST be advisory on failure (reject swallowed and counted, matching `push().catch()`), and MUST be a no-op after `stop()`/`close()`.

Implementation is small: the debounced path's body already exists; `pushNow` is that body minus the timer, and `buildHints()` stays private because nothing outside needs to call it directly once the report is built internally. No wire change — `BeaconReport` is unchanged, and no hashed input moves.

**Status: implemented 2026-09-13** — `BeaconHandle.pushNow(): Promise<void>`. `BeaconHub.push()` was refactored to *return* its promise (it was `void`-returning fire-and-forget); `pushNow` is that same method, so report construction — `vectors()` snapshot, `buildHints()` derivation, `hintKeys` policy — is one code path shared with the debounced push, which is the property the proposal actually needed. `buildHints()` stays private: nothing outside needs to call it once the report is built internally. A stale handle (retired by `stop()` or a later re-arm) resolves without publishing, reusing the same `armGen` discipline `stop()` already had; after `close()` it is likewise a no-op. Never rejects — `push()` already swallows, matching §5.7's best-effort contract. Regression: `test/beacon.test.ts` "host-initiated push (P30)" — publish-without-advancing-the-clock while a debounce is still pending, **hints and vectors byte-equal to the debounced push's** (the half that makes the hint-preservation hack unnecessary), and no-op after stop / re-arm / close.

**Field evidence.** The integration ships a 904-line beacon wrapper whose `doPut`/`doGet`/`pushNow` re-implement the transport round trip specifically to get a host-initiated publish. Its comments name both halves: "Push a report NOW, outside the library's debounce" and "P27 hint derivation is private to the library, so preserve its last projected hint set rather than clearing it on the server during reconnect re-seeding." Not all 904 lines are library-replaceable (projection, split-retry and the content boundary are genuinely host concerns), but the push path is, and the hint-preservation hack exists **only** because of the private method.

---

## P31 — `stats()` is destructive, so its values depend on how many callers exist (§14.1)

`stats()` drains the P24 interval counters: `drainIntervalStats()` does `this.interval.clear()` (`src/sync.ts:295`), and the documented interval is "[previous `stats()` call, this one]". The call therefore **mutates observable state**, and the number of independent callers becomes part of the semantics.

With N callers on independent cadences, each reads only the slice accumulated since whichever caller last ran. Nobody observes a whole interval; the per-caller values shrink as *unrelated* call frequency rises. The failure mode is the one v3.5 kept finding and naming: **no error, no zero, just numbers wrong by an unknowable factor**, on a surface whose entire purpose is telling a host whether replication is healthy. A host that adds a second metrics consumer silently corrupts the first, and nothing anywhere reports it.

P24 chose read-and-reset deliberately ("reset on every `stats()` read exactly as proposed") on the reasoning that a rate over a host-chosen cadence is ill-defined. That reasoning holds for *defining* the interval and does not license making the **whole of `stats()`** destructive: `topics[].logRows`, `pending`, `consumers[].lagRows`, `peers`, `fgen` age are all point-in-time gauges that a host may legitimately poll from several places, and they ride on the same now-destructive call.

**Amendment**, in decreasing order of preference:

1. **Split the surfaces.** `stats()` becomes a pure read in which the interval block reports the window since the last *drain* without clearing it; a new `drainSyncInterval()` does the read-and-reset for the single owner that wants disjoint windows. Pure-read `stats()` is then safe for any number of callers at any cadence, which is what every other getter on the node already promises.
2. **Per-reader cursors.** `stats(o?: { reader?: string })` keyed accumulation, so each named reader sees its own disjoint intervals. More machinery; removes the need for hosts to appoint an owner.
3. **Minimum viable**: keep the behavior, state it normatively in §14.1 as "destructive — exactly one caller", and emit `ERR_MISUSE`… which is not detectable, so in practice this is documentation only. Listed for completeness; (1) is the real fix.

(1) changes no field shape and is additive apart from `stats()` ceasing to clear — which can only *widen* what a correct single-reader host observes.

**Status: implemented 2026-09-13 — remedy (1), the surface split.** `stats()` is now a **pure read**: `SyncEngine.readIntervalStats()` returns the counters without clearing, and the new `SeqscribeNodeExt.drainSyncInterval()` returns-and-resets for a single owning reader that wants strictly disjoint windows. Leaving the drain uncalled simply makes the interval counters cumulative-since-start, which no reader can corrupt for another. One defect found while implementing, beyond what the proposal described: the original `new Map(this.interval)` shared the live counter *objects*, so a returned "snapshot" kept mutating under its holder — the same class of defect as the destructive read, just quieter. `readIntervalStats()` copies each counter. **This is a behavior change to a shipped API**: a host that relied on `stats()` draining now sees cumulative values, which is why it is recorded here rather than slipped in — the fix direction is strictly safer (a single-reader host sees *more* than before, never less), but it is not byte-compatible. Regression: `test/sync-stall.test.ts` — the rewritten P24 block (re-read returns identical values rather than zeros; a returned snapshot is immune to later traffic; `drainSyncInterval()` resets and the next read is all-zero) plus a new **P31 case pinning the defect itself**: three independent `stats()` readers each see the whole interval.

**Field evidence.** The integration added a 270-line collector whose stated purpose is to make `stats()` single-reader by construction: it ticks on its own cadence, is the only caller of `node.stats()` in the daemon, and publishes a pure-getter `snapshot()` everything else reads. Its header records the defect from production: three independent callers (a status reporter, a metadata RPC, a readiness probe) each stealing the others' accumulation. There is a dedicated regression test asserting the single-reader property, and a starred comment warning future contributors that calling `node.stats()` directly re-introduces the bug. That is a host building a library invariant the library does not enforce.

---

## P32 — `Anomaly` names a kind and nothing else, so no anomaly is actionable without a second lookup (§14.1)

```ts
interface Anomaly { kind: /* 14 kinds */; entry?: LogEntry; }
```

`entry?` is the only payload, and it is absent for most kinds. A host receiving `sync_stalled` learns **that** a WANT round stopped progressing but not toward which peer or on which stream; `view_faulted` names no view; `consumer_abandoned` names no consumer; `delta_mismatch` names no view. In each case the library knew the answer at emit time and dropped it.

The consequence is that the anomaly feed cannot be alerted on directly — every handler must correlate against `stats()` to find out what the signal referred to, and under P31 that correlation is itself sampled at a different instant than the anomaly.

P22's status marker records this explicitly: "the anomaly carries the kind, the histogram lives in stats (the SPEC `Anomaly` shape has no detail field — extending it was not taken)." That was a scoping decision inside an incident fix, not a judgment that the detail is unwanted, and the three cycles since have left the feed unactionable.

**Amendment**: extend `Anomaly` with optional, kind-appropriate identifiers:

```ts
interface Anomaly {
  kind: /* unchanged */;
  entry?: LogEntry;
  topic?: Topic;        // every kind that has one
  peerId?: string;      // sync_stalled, sync_hot
  writer?: WriterId;    // sync_stalled, writer_forked, entry_quarantined
  view?: string;        // view_faulted, delta_mismatch
  consumer?: string;    // consumer_abandoned
}
```

All optional, so no existing handler breaks and an implementation that omits them stays conformant. Content stays out by construction: these are identifiers already exposed through `stats()`, never payload. **`entry` remains the only field carrying user content**, and the §14 guidance that hosts should not log it is unchanged — if anything this makes compliance easier, since the identifiers a host actually wants to log are now available without reaching into `entry`.

**Status: implemented 2026-09-13** — all five optional fields added, and **populated at every emit site that has the identifier in scope** (the fields are worthless if only declared): `sync_stalled` now carries topic + peerId + writer, `sync_hot` topic + peerId, `view_faulted`/`delta_mismatch` topic + view, `consumer_abandoned` topic + consumer, `bad_cert` topic, `bad_directive` topic + writer, `canonical_unavailable` topic + writer, `writer_forked` topic + writer at all four emit sites, `owned_violation`/`takeover_invalid` topic + writer alongside the `entry` they already carried. Purely additive and all-optional, so no existing handler breaks. No entry content is added anywhere — every new field is an identifier already exposed through `stats()`, and `entry` remains the only field carrying payload. Regression: `test/sync-stall.test.ts` asserts the `sync_stalled` subject on a real stall (and that it carries no `entry`); `test/views.test.ts` asserts the faulted view is named.

**Field evidence.** The integration's anomaly handler documents the shortfall where it logs: "The `Anomaly` payload is `{ kind, entry? }` — the library carries no peer or topic detail on the sync signals, so the log line is the kind plus the node-level context we already hold." Its `sync_stalled` line ends by telling the reader to go look somewhere else (`check get_status_metadata seqscribe.stalledStreams / applyRejects`) — a log message whose content is an instruction to perform the lookup the library could have saved.

---

## P33 — `append`'s one surviving synchronous throw makes correct in-flight accounting a trap (§11.1, §14)

v3.5 P11 moved every data-dependent append failure onto the returned Promise, leaving exactly one synchronous throw: a raw `append` on a register topic (§11.1, normatively "throws"). The reasoning was sound — that is static API misuse, knowable while writing the code, not a runtime condition.

The unintended consequence lands on any host that bounds concurrent appends, which is ordinary practice for a fire-and-forget shadow write off a hot path:

```ts
inflight++;                                  // reserve
node.log(t).append(kind, payload)            // ← can throw synchronously
  .finally(() => inflight--);                // never attached
```

A synchronous throw skips both settle handlers, so the reserved slot is **never returned**. Repeated misuse walks the counter monotonically to the cap and parks it there, and the leg then sheds every record for the life of the process *while the topic is perfectly healthy* — a silent, permanent, fail-closed drop. The correct order (call first, reserve only once a promise is in hand) is discoverable, but only after you know the asymmetry exists; nothing in the signature indicates it.

This is the same **shape** as v3.5's recurring finding: a guard stuck permanently on one side, with no distinguishing symptom on the failing path.

**Amendment**, either of:

1. **Document the hazard normatively.** §11.1 and §14's error-carriage paragraph gain one sentence: raw append on a register topic throws synchronously, so a caller bounding concurrency MUST acquire its slot *after* `append` returns a promise, never before. Zero code change, zero compatibility cost — and the §14 rule ("Promise-returning APIs reject") currently reads as absolute to anyone who does not find the §11.1 exception.
2. **Remove the asymmetry** by rejecting this case too, leaving `append` with no synchronous throw at all. Cleaner for callers, but it contradicts a normative "throws" and reclassifies a development-time error as a runtime one; it would need a deliberate §11.1 amendment rather than being slipped in.

(1) is recommended. Note that **neither option is an in-flight/backpressure API** — the library has no append-side queue-depth signal at all (`SEND_QUEUE_CAP` bounds the *send* lane, not local appends), so every host doing load-shedding hand-rolls the counter. Whether the library should own that is worth asking, but it is a larger question than this item and is not proposed here.

**Status: implemented 2026-09-13 — remedy (1), documentation.** The hazard is now stated at the throw site in `src/node.ts`: a caller bounding concurrency must take its slot *after* `append` returns a promise, never before, because a slot reserved first is never released and the counter parks at its cap — a silent, permanent, fail-closed drop on a healthy topic. Remedy (2) (removing the asymmetry) was **not** taken: it contradicts §11.1's normative "throws" and would reclassify a development-time error as a runtime one, which needs a deliberate §11.1 amendment rather than being folded into a host-surface cycle. The §14 amendment text still needs writing at ratification — this lands the in-source warning, which is what a reader of the code hits first. No behavior change, no test (nothing executable changed).

**Field evidence.** The integration extracted a shared `inflight-gate.ts` after **two** legs hand-rolled the same accounting and grew the **same two defects**: a slot leaked on the surviving synchronous throw, and a negative count after a reconfigure zeroed the counter with appends still outstanding. Its header documents the first defect exactly as analyzed above, including that the result is a permanent fail-closed drop on a healthy topic. Two independent implementations of the same small counter (now 123 lines once both defects were understood and the generation discipline added) converging on the same two bugs is the signal that the hazard is in the library's shape, not in one author's care.

---

## P34 — `staleness()` is computed and discarded; the beacon's two certified features have no reader (§5.7a, §14.1)

Not a defect — a **dead end in the surface**. `staleness(topic, key?)` returns:

```ts
interface Staleness { behind: Record<WriterId, number>; asOf: string; keyStale?: {...}; }
```

`behind` answers "which writers am I behind on, by how much", per topic. The two things a beacon exists to make answerable — *wake-up lag* ("who is ahead of me") and *sole-copy awareness* ("what might exist only here") — are both derivable from `behind` plus the board, but the library provides no derivation and no aggregate. Every host must write that arithmetic itself, for a feature whose entire purpose is operator-facing.

Worse, the honest answer requires knowing something only the host has: whether its board GET was **truncated**. If the peer list is a subset of the fleet, "no peer has this entry" is unprovable — the peer that has it may be one the server dropped. Sole-copy must then report a third value (`unknown`), not `false`. Nothing in the library says this, and the natural implementation returns a confident wrong answer.

This also explains why **P27's `keyStale` could ship inert for two revisions**: `staleness()` has no way to signal "this feature has no data source", because an absent `keyStale` is also the correct return when no peer is ahead. A surface that cannot distinguish *no data* from *no problem* will hide its own breakage, and did.

**Status: implemented 2026-09-13.** The shape resolved once the blocking question was named precisely: *can the library know whether its board is complete?* It cannot — `BeaconTransport.get()` returns `BeaconReport[]` with no truncation signal — so completeness is an **input**, not a derivation. `setKnownVectors(v, { truncated })` supplies it; `staleness()` then returns `aheadPeers` (needs no completeness: one peer ahead proves lag) and `soleCopyRisk: true | false | "unknown"` (needs it, because the claim is about *every* peer). Absent truncation, non-zero truncation, and no-board-yet all yield `"unknown"` — fail-safe by construction, so a host that never supplies completeness simply never gets a sole-copy claim instead of getting a wrong one. Three-valued rather than nullable-boolean on purpose: a nullable boolean makes the dangerous reading (falsy ⇒ "not at risk") the easy one. Regression: `test/beacon.test.ts` "staleness derivations (P34)" — `aheadPeers` without completeness, `"unknown"` across all three ignorance cases, and both `true`/`false` once the board is stated whole.

**Amendment** (lowest-confidence item here — the shape deserves a second embedder before ratification):

- `staleness()` gains a documented derivation for the two beacon-certified questions, or an aggregate (`aheadPeers`, `soleCopyRisk: true | false | "unknown"`) that encodes the truncation rule so a host cannot get it subtly wrong.
- §5.7a states normatively that a truncated board makes sole-copy **unprovable**, and that `keyStale` is advisory-only and MUST NOT gate correctness (already true in v3.6's text; worth stating at the reader rather than only at the producer).
- Whatever lands must let a caller distinguish "no data source" from "nothing stale".

**Field evidence.** The integration built the full beacon stack — board, transport, split-retry — and then, in its own words, "stopped: the board round-trips live in production and NOTHING reads it. `staleness()` is computed by the library and thrown away, so the two features certified as reachable (wake-up lag, sole-copy awareness) are present in the data and absent from every surface a human looks at." A separate `beacon-diagnostics.ts` is that missing read, and it enforces in code two disciplines the library states nowhere: truncation ⇒ sole-copy is `'unknown'` never `false`, and `keyStale` is advisory-only (renamed `keyStaleAdvisory` locally so no caller can mistake it for a gate). Both are correct. Both are conclusions the library should not have made a host derive.

---

---

## P35 — the archive's storage form was specified as a filesystem path two shipped adapters cannot write (§7.6)

§7.6 named the cold archive `archive/<topic>.jsonl.gz` from v3.2 through v3.6. The library has never written it: archived rows move to a sibling `sq_archive` table, and `src/archive.ts` carried a comment calling the file form "an adapter concern" — i.e. the implementation knew it diverged and proceeded anyway, which is the state v3.5's ratification discipline exists to prevent.

The divergence is not laziness; **the SPEC text is the part that is wrong.** §14 requires the library to run on storage adapters that have no filesystem at all — a browser on sqlite-wasm/OPFS and a Cloudflare Durable Object. A conforming implementation therefore had to violate the letter of §7.6 in order to satisfy §14, and any second implementor reading §7.6 literally would either write a file nobody reads or conclude the two sections contradict each other.

**Amendment**: §7.6 no longer names a storage form. What stays normative is the **behavior** — archived entries leave the hot log, remain locally retrievable, remain reachable through the §14.1 writer-form `scanEntries` (which spans the archive), and are drainable via `export`. Where they physically live is a storage-adapter concern, stated as such.

**Status: implemented 2026-09-13** (documentation only — the code was already correct). §7.6 rewritten; `sq_archive` named as the reference implementation's choice rather than as the contract. No behavior change, no test change, no hashed input moves.

---

## P36 — `RegisterSnapshotState` silently omits pending requests (§11.6)

`pendingRequests()` reads live register state, but `RegisterSnapshotState` has no `requests` field, so a node that bootstraps from a **snapshot** rather than replaying the log does not see `owned` requests made below the cut. The owner's approval UI never lists them, and an owner who approves only what it can see never approves them.

This was previously tracked as a known gap with the parenthetical "a spec-level decision" — but the decision was recorded nowhere in the SPEC, so the omission read as an oversight to anyone comparing §11.4 against §11.6.

**Amendment**: state the decision and its consequence in §11.6 rather than changing behavior. Requests are ordinary log entries with a `REQUEST_TTL_MS` (30 d) lifetime; the requester's remedy is to re-request, which is one append, and the TTL bounds how long the gap can matter. Carrying them would replicate *unapproved* host-policy state into the compaction artifact — state whose only consumer is a live owner's UI — to close a window the requester can close itself.

**Status: implemented 2026-09-13** (documentation only). §11.6 gains the explicit non-field note. Behavior unchanged — this converts an undocumented silence into a stated decision, which is what makes a second implementation match the first.

---

---

## P37 — the package split's precondition was true by accident, with nothing checking it

DESIGN §8 splits this repo into `seqscribe` / `seqscribe-ws` / `seqscribe-beacon` / `@seqscribe/*`. The split itself is correctly deferred to release — moving folders while the package is unpublished would break the embedder's `file:` vendor path for no benefit. But the *property the split depends on* — that the would-be-separate modules do not reach into core internals — held only by accident of good layering, with nothing enforcing it.

Inspection found the situation better and narrower than the tracked note implied: `ws.ts` (94 lines) and `adapters.ts` (211 lines) already import **types only**, so they are split-ready today. And `seqscribe-beacon` in DESIGN §8 means the reference **server**; the beacon *client* (`BeaconHub`) is core by design (DESIGN §5.7 — `node.ts` owns one) and is not moving. So the remaining risk is not "do the split" but "don't silently lose the ability to".

**Amendment**: none — this is a repo-hygiene gate, not a contract change.

**Status: implemented 2026-09-13** — `tools/check-boundaries.mjs`, wired into `npm run check` (and therefore CI) as `check:boundaries`. Each listed module declares the only local modules it may import; a violation fails with the DESIGN §8 rationale rather than a bare diff. `beacon.ts` is listed with its full current set *allowed* rather than restricted, so a new core dependency there is a deliberate edit to the gate instead of invisible drift. Verified in both directions: adding a `./session.js` import to `ws.ts` exits 1 with the explanation; removing it exits 0.

---

## P38 — `ERR_PROTOCOL` was deferred three times for a mechanism that already existed

P6 (v3.5) proposed a distinct protocol-violation `ErrCode`. v3.5 declined it, v3.6 re-deferred it, and v3.7's first pass re-deferred it again — each time on the same reasoning, recorded in §5.2: the code should arrive *"alongside a protocol-version bump that gives peers a negotiated way to know the code is available"*, not as a silent widening of the union.

**That mechanism was already in the wire.** `HELLO` has carried `protoMin`/`protoMax` since v3.1 and `Session` already computes `Math.min(PROTO_MAX, m.protoMax)` and rejects incompatible ranges with `ERR_PROTO_VERSION`. It had simply never been *used* for anything: both bounds were pinned at 1, and the negotiated result was computed and thrown away. Three revisions deferred an item waiting for a facility the implementation had the whole time — which is the same shape as P30 (a feature hardened across two cycles that no host could reach) and P34 (a reader the SPEC certified and nobody wrote).

**Amendment**: `PROTO_MAX` 1 → 2, `ERR_PROTOCOL` added to `ErrCode`, and §5.2 states the gating rule — a receiver that negotiated **proto ≥ 2** MUST send `ERR_PROTOCOL` at the four protocol-violation closes (the §5.2 credit-window bound and the three §5.4 reassembly overflows); one that negotiated **proto 1** MUST send `ERR_ENTRY_ENCODING` there, which is what a proto-1 peer's union contains. Pre-HELLO sessions count as proto 1.

**Status: implemented 2026-09-13** — the negotiated version is now retained (`Session.protoNow`) and all four sites route through one `Session.violationCode()` helper, so they cannot drift apart. The remedy is unchanged and **no peer's behavior depends on which code it receives** — it is being closed on either way, which is exactly why this is safe as a pure diagnostic refinement. Regression: `test/wire-hardening.test.ts` "ERR_PROTOCOL is version-gated (P38)" — proto-2 gets the new code, **proto-1 still gets `ERR_ENTRY_ENCODING` (the assertion that makes it shippable)**, and a session that never completed HELLO is treated as proto 1. The harness's `ready()` now takes a `protoMax` defaulting to 1, so every pre-P38 test keeps asserting what a proto-1 peer actually receives.

---

## Summary

| # | Gap | Host cost today | Fix size |
|---|---|---|---|
| # | Gap | Status |
|---|---|---|
| **P30** | No `BeaconHandle.pushNow()`; `buildHints()` private | **Implemented** — `pushNow()` shares the debounced push's report path, hints included |
| **P31** | `stats()` destructive ⇒ caller count changes values | **Implemented** (remedy 1) — `stats()` pure, `drainSyncInterval()` explicit. *Behavior change to a shipped API* |
| **P32** | `Anomaly` carries no topic/peer/view/consumer | **Implemented** — five optional fields, populated at every emit site with the identifier in scope |
| **P33** | One synchronous throw in an otherwise-async `append` | **Implemented** (remedy 1, documentation). §14 amendment text still to write at ratification |
| **P34** | `staleness()` has no reader for the features it feeds | **Implemented** — `aheadPeers` + three-valued `soleCopyRisk`; completeness is host-supplied |
| **P35** | §7.6 named a filesystem archive path two shipped adapters cannot write | **Implemented** (doc only) — §7.6 now specifies behavior, not storage form |
| **P36** | `RegisterSnapshotState` omits pending requests, undocumented | **Implemented** (doc only) — the decision and its remedy are now stated |
| **P37** | Package-split precondition true by accident, unenforced | **Implemented** — `check:boundaries` gate in CI; split itself still at release |
| **P38** | `ERR_PROTOCOL` deferred 3× for a mechanism HELLO already had | **Implemented** — `PROTO_MAX` 2, gated so proto-1 peers are unaffected |

`npm run check` green across all of it: 275 tests (up from 271), strict `types.d.ts` pass, fixture consumability gate.

**One compatibility note for ratification.** P31 is the only item that changes existing behavior rather than adding surface: a host that relied on `stats()` draining now reads cumulative counters. The direction is strictly safer (a correct single-reader host sees *more* than before, never less) and the old semantics remain available via `drainSyncInterval()`, but it is not byte-compatible and should be called out in the CHANGELOG entry rather than folded in silently.

**Note on `types.d.ts`.** It is generated from SPEC.md's normative `ts` blocks, so it still shows the pre-P30 `BeaconHandle` and the pre-P32 `Anomaly` — correct, since SPEC is frozen until a stamp. The real shipped surface is `dist/index.d.ts`, generated from source, which carries all of it. Ratification closes the gap.

Three of the five (P30, P31, P34) are in surfaces that v3.5/v3.6 had already amended — evidence that the amendment cycles were fixing the defect in front of them rather than the surface around it. P32 and P33 are both explicitly scoped-out decisions from P22 and P11 respectively, recorded here because the cost of the omission is now measurable.
