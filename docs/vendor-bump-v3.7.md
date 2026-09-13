# Vendor bump to SPEC v3.7 — notes for the embedder

> Status: **non-normative handoff note** (2026-09-13). Written for whoever performs the
> next `oss/vendor/seqscribe` bump in the ADHDev repo. The normative contract is
> [SPEC.md](../SPEC.md); this is the migration-shaped reading of it.
>
> **Nothing here is urgent and nothing is broken today.** The vendored copy is SPEC v3.6
> and works. This note exists because one item (P31) is **not byte-compatible**, and
> because four of the items exist specifically to delete host-side workarounds — so the
> bump is worth doing deliberately rather than incidentally.

## 0. The one thing that must not be missed

**`stats()` no longer drains the P24 interval counters.** Everything else in v3.7 is
additive; this one changes the behavior of a call the daemon already makes.

- Before (v3.5–v3.6): every `stats()` call returned the counters accumulated since the
  previous call and **reset them**.
- After (v3.7): `stats()` is a **pure read**. Counters accumulate until someone calls the
  new `drainSyncInterval()`.

If the bump lands with no other change, the daemon keeps working but
`throughput-collector.ts` stops producing *interval* throughput and starts producing
**cumulative-since-start** throughput. Nothing throws; the numbers just change meaning —
which is the same failure mode the collector was built to prevent, so it deserves an
explicit decision rather than a silent reinterpretation.

**The fix is one line** in `throughput-collector.ts`: the collector is already the
process's single reader, so change its read from `node.stats()` to
`node.drainSyncInterval()` for the interval block (keeping `node.stats()` for the
point-in-time gauges it also projects — `logRows`, `pending`, `quarantined`, `archived`,
`consumers[].lagRows`, `peers`, `finalityGeneration`). `drainSyncInterval()` returns
exactly `{ topics, syncHotspots }` in the same shape `stats()` reports them, so the
publish path downstream does not change.

### What this means for the single-reader discipline

`throughput-collector.ts` exists to make `stats()` single-reader by construction, and
`tests/seqscribe-convergence.test.mjs` G6 pins that property. **That discipline is no
longer required for correctness** — `stats()` is safe from any number of callers at any
cadence now. It is still *good hygiene* for the collector to own the drain, and the G6
test remains valid as written (it asserts who calls what, which is still true).

Two consequences worth noting, both improvements:

1. **`mesh-read-readiness.ts:372`'s fallback is now strictly safe.** That file currently
   reads `node.stats()` directly when no collector is attached, with a carefully argued
   comment about why the tradeoff is acceptable ("no interval to protect in that
   configuration"). Under v3.7 there is no tradeoff at all — a direct read cannot
   fragment anything. The reasoning was correct; the hazard it reasoned about is gone.
2. **New `stats()` consumers no longer need to route through the collector.** If a future
   read site wants point-in-time gauges, calling `stats()` directly is now fine. Only the
   *interval* counters have a single-owner requirement, and only because disjoint windows
   are a choice rather than a safety property.

## 1. Workarounds you can now delete

Four host-side accommodations exist because of library gaps that v3.7 closes. Two are
genuine deletions (§1.1, §1.4), two are justification changes where the code stays but the
comment explaining it becomes wrong (§1.2, §1.3). None *has* to change for the bump to be
safe — they all keep working — but they are the point of the bump.

### 1.1 `beacon.ts` — the `pushNow()` hint-preservation hack

`BeaconHandle` now has **`pushNow(): Promise<void>`**, which publishes immediately,
bypassing the debounce, and builds its report through the *same* path as the debounced
push — identical `vectors()` snapshot and identical hint derivation honoring each topic's
`hintKeys`.

**You already have a `pushNow()`** on your own wrapper handle (`beacon.ts:460`), and the
reason it exists — reconnect needs an edge-triggered re-seed, not a paced timer — is
correct and unchanged. What is new is that the *library* can now do it, which removes the
one thing your version could not do.

Concretely, this retires `lastLocalHints` (`beacon.ts:594`, `:612`, `:844`). That cache
exists because your `pushNow` builds the report itself and therefore could not reach the
library's private `buildHints()`, so publishing would **erase** the node's hints from the
board — the cache replays the library's last-produced set to avoid that, stale by
construction. `handle.pushNow()` derives them fresh each time, so the cache and the
erasure hazard both go.

Whether to keep your wrapper around it is your call: the projection, split-retry, GET
bounding and content-boundary work in that file are genuinely host concerns and the
library does not replace them. The narrow claim is that the *report-building* half can now
delegate.

**Your arm-once-per-node decision needs no change.** The header comment at `beacon.ts:546`
argues it correctly — a WS reconnect does not close the node or replace its transport
bridge, so a stop/start boundary there would add lifecycle state without buying a stronger
guarantee. P28 made re-arm *healthy*; it never made it *required*. v3.7 only adds the
library-side `pushNow()` your design already wanted.

Semantics to rely on: advisory (never rejects — the round is best-effort per §5.7), and a
no-op after that handle's `stop()`, after a re-arm onto a later handle, and after node
close. The generation discipline matches `stop()`'s.

### 1.2 `throughput-collector.ts` — the single-reader enforcement

See §0. The file can stay (it still owns the drain and publishes a pure-getter snapshot),
but its *justification* changes from "prevents a silent correctness bug" to "keeps
interval windows disjoint for one owner". Worth updating that header comment, since it
currently describes a hazard that no longer exists in the library.

### 1.3 Anomaly logging — the "go look elsewhere" log line

`Anomaly` now carries optional identifiers, populated at every emit site that has them in
scope:

| field | kinds that carry it |
|---|---|
| `topic` | every kind that has one |
| `peerId` | `sync_stalled`, `sync_hot` |
| `writer` | `sync_stalled`, `writer_forked`, `entry_quarantined`, `canonical_unavailable` |
| `view` | `view_faulted`, `delta_mismatch` |
| `consumer` | `consumer_abandoned` |

All optional, so the existing handler keeps compiling unchanged. The daemon's current
`sync_stalled` log line ends by telling the reader to go check
`get_status_metadata seqscribe.stalledStreams / applyRejects` — that correlation is no
longer needed: the anomaly names its `(topic, peerId, writer)` directly. Same for
`view_faulted` (names the view) and `consumer_abandoned` (names the consumer).

**None of these carry entry content.** Every new field is an identifier already exposed
through `stats()`. `entry` remains the only field carrying user/agent payload, so the
existing rule of not logging `entry` is unchanged — this just makes compliance easier,
since the identifiers you actually want to log are now available without reaching into it.

### 1.4 `beacon-diagnostics.ts` — the derivations are in the library now

`staleness()` now returns two derived answers:

- **`aheadPeers: WriterId[]`** — writers some known peer holds more of than we do
  (wake-up lag). Needs no board completeness.
- **`soleCopyRisk: true | false | "unknown"`** — whether this node holds entries no known
  peer does.

**The truncation rule you enforce in code is now the library's rule**, and it needs your
input to work: `setKnownVectors(v, { truncated })`. The library cannot derive completeness
— `BeaconTransport.get()` returns `BeaconReport[]` with no truncation signal — so:

- `truncated` omitted → `soleCopyRisk: "unknown"`
- `truncated > 0` → `"unknown"`
- no board observed yet → `"unknown"`
- `truncated: 0` → an actual `true`/`false`

That is the same discipline `beacon-diagnostics.ts` enforces (`truncated > 0 ⇒ 'unknown',
never false`), arrived at independently — which is the main reason it was safe to adopt
into the library. Pass the count your transport already computes and the arithmetic can
come out of the host.

Note `soleCopyRisk` is three-valued rather than a nullable boolean deliberately, for the
reason your own file gives: a nullable boolean makes the dangerous reading (falsy ⇒ "not
at risk") the easy one. `SoleCopyUnknownReason` on your side is richer than the library's
single `"unknown"` — if that distinction (`'truncated'` vs `'no-board'`) is load-bearing
for the UI, keep deriving it host-side; the library's value is the gate, not the
explanation.

## 2. Additive surface you may want

- **`ViewHandle.table`** — the materialized table name (`sqv_<name>_<hash8>`). Previously
  unreachable through public API, which is why the library's own tests used a private
  `_views` cast. Not currently relevant to ADHDev (it uses `onEntry`/`scanEntries`, not
  materialized views) — noted in case a future read model wants one.
- **`drainSyncInterval()`** — see §0.
- **`ERR_PROTOCOL`** — a new `ErrCode`. **Checked: you need to do nothing.** No host code
  switches on an ERR code from a close frame (the only `ERR_ENTRY_ENCODING` reference in
  the integration is about `sanitizeJson`, unrelated). For completeness, it is
  sent *only* to a peer that negotiated protocol ≥ 2, at four protocol-violation closes,
  immediately before the session closes. A peer still on v3.6 negotiates proto 1 and keeps
  receiving `ERR_ENTRY_ENCODING` exactly as before. If any host code switches on the ERR
  code from a close frame, widen it to accept `ERR_PROTOCOL`; the close *reason* is
  unchanged (`protocol`).

## 3. Mixed-version fleet during rollout

The one genuine wire change (P38, `ERR_PROTOCOL`) is version-gated, so a fleet running a
mix of v3.6 and v3.7 nodes is fine in both directions:

| sender | receiver | code emitted at a protocol violation |
|---|---|---|
| v3.7 | v3.7 | `ERR_PROTOCOL` (both negotiated proto 2) |
| v3.7 | v3.6 | `ERR_ENTRY_ENCODING` (negotiated proto 1) |
| v3.6 | either | `ERR_ENTRY_ENCODING` (v3.6 has no proto 2) |

Everything else in v3.7 is either additive surface or a local behavior change with no wire
component. **No hash input moves** — chains, seeds, `certHash`, `snapshotId` and
`topicSchemaHash` are all unchanged, so v3.7 and v3.6 nodes replicate against each other
normally and the published test vectors remain valid.

## 4. Suggested order

1. Bump the vendored copy, build, and run `npm run test:seqscribe-gate`. Expect it green —
   nothing here is breaking at the type or API level.
2. Do the §0 `drainSyncInterval()` change. Check whatever asserts interval behavior, since
   "interval" vs "cumulative" is exactly what changes if this is skipped.
3. Delete the §1.1 `lastLocalHints` cache and have your `pushNow` delegate report
   construction to `handle.pushNow()`. Keep the arm-once lifecycle — that decision stands
   on its own reasoning and v3.7 does not touch it.
4. Simplify the anomaly log lines (§1.3) and, if wanted, hand `truncated` to
   `setKnownVectors` and drop the host-side sole-copy arithmetic (§1.4).
5. Update the `throughput-collector.ts` and `mesh-read-readiness.ts` header comments,
   which currently document a library hazard that no longer exists.

Steps 3–5 are optional cleanups; 1–2 are the bump itself.

## 5. Where this came from

P30–P38 were not reported by ADHDev. They were found by **reading** the integration glue
in `oss/packages/daemon-core/src/seqscribe/` (~11,400 lines) and asking where the host was
paying a standing cost the library could absorb. The glue's own comments documented each
workaround and why it was necessary, which is what made them findable — four of the five
host-surface items were identified from those comments alone.

The pattern worth naming, because it argues for doing this again: every one of these had
been worked around and moved past, so none would ever have surfaced as a bug report. Two
were features the library had already hardened across multiple revisions that no host
could actually reach (`pushNow`, the `staleness()` readers). Full rationale per item is in
[docs/proposals-v3.8.md](proposals-v3.8.md); the ratified contract is in
[SPEC.md](../SPEC.md) and [CHANGELOG.md](../CHANGELOG.md) under v3.7.
