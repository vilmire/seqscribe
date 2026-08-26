# Proposed SPEC amendments — v3.4 candidates

> Status: **RATIFIED as SPEC v3.4** (2026-08-26 — stamped under the owner's standing directive to complete all pre-integration work; the implementation had already validated all four amendments against the P7 gate). Applied to [SPEC.md](../SPEC.md) §5.2/§6.3/§8/§14/§16/§19/§20 and recorded in [CHANGELOG.md](../CHANGELOG.md). This file is retained as the evidence record. Originally surfaced by running the §19 P7 catch-up gate at the full profile — the first defects found the way v3.2 predicted: as failing runs, not document review. Reproduce via `npx vitest run test/p7.test.ts`.

---

## P1 — Knowledge-based push replaces the hlc-window relay (§6.3)

**Defect.** The v3.1 changelog claimed "control retries + eager relay fan-out (fixes the anti-entropy-vs-P7 arithmetic)". Measured: it does not. The relay criterion is hlc recency (`hlc.l ≥ now − RELAY_WINDOW_MS`), so entries authored during a partition longer than RELAY_WINDOW_MS (30 s) never gossip after the heal — and catch-up entries arrive via WANT with old stamps, so recovering nodes don't re-gossip them either. Post-partition repair degenerates to **one hop per anti-entropy round** (300 s): the 100-node profile converged *never* within 400 s (and still failed the 120 s gate with ANTI_ENTROPY_MS forced down to 30 s, because propagation is hop-limited, not cadence-limited).

**Amendment.** Replace §6.3's relay paragraph with knowledge-based push:

- Each session retains the peer's last advertised stream heads (from HAVE rounds — authoritative; updated optimistically by own accepted pushes and by received ENTRIES `toSeq`).
- On applying an external entry, mark the stream **dirty** toward every READY mutual-full peer except the sender; on own appends, toward the K most-recently-active peers (unchanged), honoring the topic's `flushThrottleMs`.
- A per-peer, capacity-aware pump drains dirty streams as batched ENTRIES from `known+1` to local contig, within SEND_QUEUE_CAP; a tail-dropped batch keeps the stream dirty and resumes when the queue drains.

`RELAY_WINDOW_MS`/`RELAY_FANOUT` become unused (retire or repurpose at ratification). Duplicate suppression is knowledge-based instead of time-based, so backlogs gossip at network RTT: the 100-node gate converges at **heal+40 s**.

**Why it stays bounded** (the flood the hlc window guarded against): pushes are targeted by known deficit — a peer whose vector already covers the stream is never pushed; bulk-bootstrap traffic (WANT responses) updates `known` and therefore suppresses re-push.

---

## P2 — Data-lane retransmission (§5.2)

**Defect.** The Channel guarantees neither delivery nor ordering (§5.1), and receivers process data frames in contiguous-mid order — so a single lost data frame **head-of-line blocks every later frame on that session**, and the only specced recovery is the CHANNEL_STALL_MS (30 s) close + host reconnect. Under P7's 1% loss, a catch-up session exchanges hundreds of frames; nearly every session eats one-to-three 35 s stall cycles, which alone busts the 120 s gate (observed as frozen WANT queues and stuck push pumps in the diagnostics).

**Amendment.** Append to §5.2:

```
Senders retain unACKed frames and retransmit any frame unACKed for
CONTROL_RETRY_MS (same mid — mids are assigned once, at first transmission).
Receivers already dedupe by mid. Stall-close remains the backstop for a peer
that has genuinely stopped ACKing.
```

Loss recovery drops from ~35 s (stall cycle) to the retry cadence (5 s). Memory cost: ≤ INFLIGHT_CREDITS retained frames per session.

---

## P3 — Silent partitions heal at anti-entropy cadence (documentation)

**Finding, not a defect.** With P1+P2, the P7 gate passes when the partition is **host-visible** (channels close; the host redials; HAVE exchange at reconnect seeds the catch-up wave) — which matches §5/§14: transport and reconnection are the host's, and the intended transports (WebRTC DataChannel, WebSocket) surface disconnection. A **silent** partition (packets dropped, no close signal, no data in flight to trigger the stall) leaves surviving sessions idle until their next anti-entropy round — recovery in ≤ ANTI_ENTROPY_MS (300 s) per §6's design ("the repair path for lost pushes").

**Amendment (one sentence + a P7 clarification).** In §19 P7, state that the partition is host-visible; in §6.3 or §20, state the silent-partition bound explicitly:

```
A partition the host cannot observe (no channel close, no in-flight data to
stall) is repaired by the next anti-entropy round — hosts wanting faster
healing after silent partitions should surface transport liveness and cycle
the channel.
```

---

## P4 — `sq_log.rowid` must be monotonic (§8 DDL)

**Defect.** §7.6's consumer semantics ("resume resets its cursor to the first post-cut rowid") and §9's `onEntry` rowid-ordered delivery assume rowids never move backwards. The §8 DDL declares `rowid INTEGER PRIMARY KEY`, and SQLite reuses rowids after deletion: the first cold-archive pass that empties a topic's hot log makes the next append restart at rowid 1 — **below every existing cursor and view watermark**, so new entries become permanently invisible to consumers and views. Found the moment archiving met the bootstrap test.

**Amendment.** §8 DDL:

```
rowid INTEGER PRIMARY KEY  →  rowid INTEGER PRIMARY KEY AUTOINCREMENT
```

(AUTOINCREMENT persists the high-water mark in sqlite_sequence; rowids are then strictly increasing for the table's lifetime.) Storage adapters not backed by SQLite must provide the same monotonicity guarantee.

---

## Ratification checklist

1. Apply P1/P2 normative text to §6.3/§5.2; P3 sentences to §19/§20; P4 to the §8 DDL; bump SPEC to v3.4 + CHANGELOG entry (self-critical format — the v3.1 "fixes the arithmetic" claim should be named as the mistake).
2. On P1 ratification, decide the fate of RELAY_WINDOW_MS / RELAY_FANOUT in §14/§16.
3. The P7 gate test (`test/p7.test.ts`) becomes the regression anchor for all three.
