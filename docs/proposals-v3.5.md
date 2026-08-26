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
