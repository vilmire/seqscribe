# SPEC amendments — v3.8 candidates (P30–P34)

> Status: **PROPOSED, none implemented.** Nothing here is in SPEC.md or CHANGELOG.md yet. Per the standing convention, SPEC is frozen between stamps and amendments accumulate here first.
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

**Amendment** (lowest-confidence item here — the shape deserves a second embedder before ratification):

- `staleness()` gains a documented derivation for the two beacon-certified questions, or an aggregate (`aheadPeers`, `soleCopyRisk: true | false | "unknown"`) that encodes the truncation rule so a host cannot get it subtly wrong.
- §5.7a states normatively that a truncated board makes sole-copy **unprovable**, and that `keyStale` is advisory-only and MUST NOT gate correctness (already true in v3.6's text; worth stating at the reader rather than only at the producer).
- Whatever lands must let a caller distinguish "no data source" from "nothing stale".

**Field evidence.** The integration built the full beacon stack — board, transport, split-retry — and then, in its own words, "stopped: the board round-trips live in production and NOTHING reads it. `staleness()` is computed by the library and thrown away, so the two features certified as reachable (wake-up lag, sole-copy awareness) are present in the data and absent from every surface a human looks at." A separate `beacon-diagnostics.ts` is that missing read, and it enforces in code two disciplines the library states nowhere: truncation ⇒ sole-copy is `'unknown'` never `false`, and `keyStale` is advisory-only (renamed `keyStaleAdvisory` locally so no caller can mistake it for a gate). Both are correct. Both are conclusions the library should not have made a host derive.

---

## Summary

| # | Gap | Host cost today | Fix size |
|---|---|---|---|
| **P30** | No `BeaconHandle.pushNow()`; `buildHints()` private | 904-line beacon wrapper; hints cached and replayed stale | **Small** — expose the existing push path |
| **P31** | `stats()` destructive ⇒ caller count changes values | 270-line single-reader collector + dedicated regression test | **Small–medium** — split pure read from drain |
| **P32** | `Anomaly` carries no topic/peer/view/consumer | Anomaly feed unalertable; log lines that say "go look elsewhere" | **Small** — optional fields |
| **P33** | One synchronous throw in an otherwise-async `append` | Two legs, same two defects, extracted into a shared gate | **Trivial** (document) |
| **P34** | `staleness()` has no reader for the features it feeds | Host-side diagnostics deriving rules the library never states | **Needs design** |

Recommended order: **P30, P32, P33** are small, self-contained, and each removes a standing host workaround. **P31** is the highest-value correctness item (it is the only one currently producing *wrong numbers* rather than missing ones) but needs an API decision about `stats()` compatibility. **P34** should wait for a second embedder to confirm the shape.

Three of the five (P30, P31, P34) are in surfaces that v3.5/v3.6 had already amended — evidence that the amendment cycles were fixing the defect in front of them rather than the surface around it. P32 and P33 are both explicitly scoped-out decisions from P22 and P11 respectively, recorded here because the cost of the omission is now measurable.
