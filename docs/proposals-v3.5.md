# Proposed SPEC amendments — v3.5 candidates (pending owner stamp)

> Status: **proposals only — not normative** (2026-08-26). Surfaced while preparing the ADHDev integration surface. The implementation ships all three as extensions (nothing in v3.4 forbids them); ratification would pin them for second implementations.

## P1 — attach MUST reject a "full" grant on a subscribe-only topic (§14)

A `full` grant on a `replication: "subscribe-only"` topic negotiates mutual full-sync against a topic with no durable log: HAVE advertises the persistent stream heads (§14 ring head rule), the peer WANTs ranges that no responder can serve, and the deficit loop spins until anti-entropy noise settles. The topic table shape ADHDev uses (mixed full-sync and subscribe-only topics in one grants map) makes this an easy host mistake. Amendment: add to §14 `attach` — "a grant of `full` for a topic whose policy is `subscribe-only` throws (grant `serve` instead)". Implemented.

## P2 — `stats()` observability surface (§14, additive)

The host-guide's baseline metrics (per-topic fgen age, log/pending/quarantine/archive sizes, consumer cursor lag, per-peer queue depth and dirty-stream counts) need a supported API rather than SQL spelunking. Implemented as `SeqscribeNodeExt.stats(): NodeStats` — exact shape in `src/node.ts`. Amendment: add to §14 as OPTIONAL (implementations MAY omit; shape normative when present).

## P3 — cross-process ownership enforcement note (§8)

§8's "single-process ownership MUST" names the requirement but no mechanism, and SQLite's own WAL locking does not deliver it (two seqscribe processes corrupt each other's caches silently). The better-sqlite3 adapter now enforces it by holding `BEGIN EXCLUSIVE` on a sibling `<db>.lock` database — an OS-level file lock that dies with the process, so a crashed owner never wedges the DB. Amendment: one sentence in §8 — "storage adapters MUST enforce single ownership with a mechanism that survives owner crash (e.g. an OS file lock); an in-process flag alone does not satisfy this section."
