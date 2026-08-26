# seqscribe — subscribe to sequences: P2P log sync for SQLite

> Pronounced **"seek-scribe"** — **seq**uence + sub**scribe** + *scribe* (the writer-owned principle: every stream has exactly one author).
> Naming history: seqline · syncline · seqsync · sqlync were evaluated; **sqlync was abandoned (2026-08-26) after discovering a collision with an existing same-category product (SQLync, sqlync.com, GitHub org since 2023)** → seqscribe finalized after a clean 5-axis availability audit (npm · GitHub user/org · repos · domain · product search). Lesson: name vetting must cover **user/org, domain, and same-name products** — not just npm and repos.

> Status: **v1 architecture record** (2026-08-25 exploration → 2026-08-26 fifteen decisions stamped, §10). Detailed spec = [SPEC.md](SPEC.md). **The normative contract is SPEC alone; this document is background and decision history (non-normative)** — where they conflict, SPEC wins (settled in the v2 revision incorporating three external reviews, 2026-08-26).

**One-line definition**: an **embeddable, transport-agnostic** sync library that replicates writer-owned append-only logs peer-to-peer with cursor-based resumption, materializing them into derived SQLite views. FSL-1.1-Apache-2.0 (relicensed from MIT, 2026-08-26).

**Positioning**: "y-webrtc for SQLite". Market slot = **SQLite semantics × true P2P (no server anchor) × BYO transport** — no production-grade incumbent as of the 2026-08 survey (§3).

---

## 1. Motivation — three internal ADHDev consumers

| Consumer | Use | When |
|---|---|---|
| Fleet Assistant journals | converging per-machine append-only journals across the fleet | assistant phase 2 |
| Repo Mesh event log / ledger | durable event log + cursor delivery replaces the pending/outbox/redrive compensation machinery; multi-machine ledger survivability | mesh refactor |
| Dashboard replica (Tier 2) | browser holds only subscribed slices of ledger/fleet views; offline reading | later |

Three hand-rolled precedents already exist: daemon-p2p chat-tail subscriptions (seq/cursors), the TopicSubscriptionRegistry, and status flushes — all ad-hoc Tier-2 implementations. The mesh ledger has likewise organically converged on JSONL-append + derived SQLite (mesh-runtime.db). **seqscribe is not an invention; it is the extraction and generalization of these patterns.**

## 1.5 Design envelope (2026-08-26 — scale review)

Upper bounds this spec is accountable for (team/org fleets — the "3 personal machines" assumption is forbidden):

| Axis | Bound |
|---|---|
| Nodes per account | ~100 |
| Active topics | thousands (per-session transcripts etc.) |
| Writes per topic | tens/s |
| Entry size | ≤64KB (larger via blob references) |
| Log history | years, millions of entries |
| Concurrent subscribers | tens per view |

Beyond the envelope (thousands of nodes) is an explicit non-goal. Envelope claims must be verifiable by the simulation harness.

**Spec changes the envelope forced (vs the first draft)**: ① snapshots/checkpoints/compaction are first-class and mandatory (full-replay bootstrap forbidden) ② only log append is on the synchronous critical path; view materialization is async batched; ring topics live outside the durable DB ③ view deltas computed once per change and broadcast to all subscribers ④ HLC drift bound ε (over-ε receipts: no clock merge + flag; ordering keeps the stamp) + specified checkpoint spacing ⑤ anti-entropy rounds (periodic HAVE re-exchange — the repair path for lost pushes) ⑥ writer retirement protocol ⑦ eager push limited to K peers + transitive propagation.

## 2. Design principles

1. **Writer-owned append-only log** — one author per stream; only the author appends. Conflicts are structurally impossible (no merge logic).
2. **Cursors replace delivery** — "guaranteed storage + cursor resumption" instead of "guaranteed delivery". A delivery failure is just a cursor that hasn't advanced yet.
3. **Idempotent application** — (writerId, seq) is the event id; duplicates are harmless.
4. **Derived artifacts are rebuildable** — SQLite views/indexes can always be rebuilt from the log. The log is the only truth.
5. **Transport and storage are injected** — the library accepts only an already-connected bidirectional channel and a storage interface. Peer discovery, auth, NAT traversal are the host's job (ADHDev brings its existing WebRTC/TURN stack).
6. **No consensus** — AP only (availability + partition tolerance). Eventual convergence is the only promise.

## 3. Competitive map & why-not (2026-08-25 survey)

| Alternative | Why not |
|---|---|
| Corrosion (Fly.io, Apache-2.0) | sidecar + own QUIC/gossip = assumes datacenter private networks; unusable behind consumer NAT (requires Tailscale). **First-rate design reference** for delta sync, subscriptions, compaction |
| cr-sqlite (MIT) | arbitrary multi-writer row merging — outside our scope (§7 non-goals). Maintenance uncertain. Future option only |
| rqlite / dqlite / distributed SQL | Raft quorum = writes halt in a majority-offline fleet (our default state). Category mismatch |
| libSQL embedded replicas (MIT) | requires a primary anchor = hub model, not P2P. A separate option if a hub is ever needed |
| RxDB WebRTC replication | document NoSQL (Mango), not SQL; SQLite/OPFS storage is paid. **Its replication protocol (checkpoint pull/push) is the #1 design reference** |
| ElectricSQL / PowerSync / Triplit / Zero | server-anchored (Postgres etc.) sync-engine lane |
| Gun.js / OrbitDB / Earthstar / DXOS | graph/IPFS/document lineage, not SQL |
| Ditto | commercial proof of demand for the slot; fully proprietary |
| sqlite-wasm-webrtc | hobby demo (statement relay) — no durability, cursors, or partition recovery |

Methodology lesson (naming & surveying): category-name searches do not substitute for capability searches. The public README's "why this exists" must be grounded in the full 5-axis map (distributed SQLite / CRDT libraries / local-first sync engines / P2P DBs / commercial).

## 4. Data model

> ⚠️ The draft sketch in this section was superseded by decisions 8 (HLC total order) and 9 (causal attachment) — **the precise definition is SPEC §1–§2**. Gist: LogEntry = {topic, writer, seq, **hlc**, kind, key?, causal?, payload (JSON only, ≤64KB)}. Order within a writer is seq; the cross-writer total order is (hlc, writer, seq).

- **Snapshots/compaction**: see SPEC §10 (v2: topic-level total-order cuts). The (writerId, seq) namespace is permanently reserved (no reuse).
- **Derived views**: reducer + SQLite materialization helper; views are always `rebuild()`-able. FTS indexes are a kind of view.
- Physical log format belongs to the storage adapter (Node: SQLite tables; JSONL as an import/export compatibility path for the existing mesh ledger).

## 5. Protocol (overview)

Both tiers are in the initial implementation scope. No deferrals or reservations:

> ⚠️ Overview sketch only — **per-message field definitions are governed by SPEC §5** (19 message types incl. HELLO, SNAPSHOT family, ACK; HAVE carries chains; SUB is `{view, params}` per decision 2 — no queries).

```
// Tier 1 — full replicas (between daemons)
HAVE / WANT / ENTRIES (+ SNAPSHOT_OFFER/GET/SNAPSHOT — bootstrap)
// Tier 2 — subscribers (browsers, daemon-to-daemon tail subscriptions, light consumers)
SUB {view, params, fromCursor?} / SNAP / DELTA / UNSUB
// Common: HELLO (ACL & version negotiation), ACK (credits), ERR
```

- Transport interface: `Channel { send, onMessage, onClose }` — the host connects, authenticates, reconnects, and hands over only the channel.
- Cursors are **owned and persisted by consumers** (resume from cursor after restarts, reconnects, upgrades).
- Tier-2 trigger semantics: while connected = DELTA push; on reconnect = pull from fromCursor. No separate mechanism.

## 5.5 Topic policies and the subscriber matrix ("everything through one library")

**Principle: full-sync vs subscribe is not a node property but a (node, topic) relation.** A coordinator daemon is a full replica for the ledger and a tail subscriber for worker transcripts. Tier 2 is a general role — not browser-only.

Three policy axes per topic:

```
retention:   none (volatile) | ring (tail N) | full (durable + compaction)
replication: subscribe-only | full-sync
access:      content (P2P only) | metadata (server peers allowed)   ← enforces the server
             content boundary as a topic classification
```

ADHDev topic layout (the complete target design — implementation presumes this entire table):

| Topic | Retention | Replication | Access | Replaces |
|---|---|---|---|---|
| mesh.ledger / mesh.events | full | full-sync | metadata envelope | JSONL ledger + pending/outbox/redrive |
| assistant.journal | full | full-sync | content | (new) |
| session.transcript | ring | subscribe-only | content | chat-tail subscription machinery |
| fleet.status | ring | subscribe-only | metadata | status flush / TopicSubscriptionRegistry |
| config.settings | full | full-sync | content | per-machine files (gains fleet-wide sync + audit) |

Subscriber matrix: browsers (memory/OPFS), daemon→daemon (remote session tails), server/DO (metadata class only), assistant/MCP (in-process views), push pipeline (events → notifications).

**Exclusions (boundaries)**: large blobs (screenshots) go as content-addressed references; transfer stays on the existing chunk channel. Remote input events are latency-sensitive with no durability value → raw channel. No forced adoption.

Precedents: Zero/Replicache's "sync engine as the app's data layer"; Corrosion's role inside Fly. End state: a new feature is "declare topic policy + reducer + subscribe" — a declarative data plane.

## 5.6 Daemon-to-daemon subscription/replication rules

**"Subscribing" splits into two mechanisms chosen by topic policy:**

**(A) full-sync topics = replicate + consume locally.** Authors append locally (offline-durable) → HAVE on connect → peers converge via WANT/ENTRIES → **consumers always consume their own local replica with a local cursor**. Because the consumption cursor is local: crash-resume without loss, author offline writes survive, (writerId, seq) idempotency makes duplicates harmless. Replication is transitive (receiving from any replica preserves provenance). → Replaces pendingCoordinatorEvents/outbox/redrive with a property of the data model.

**(B) subscribe-only topics = connection-scoped subscriptions (daemons are Tier-2 clients too).** SUB → SNAP (tail within ring retention) → DELTA push (author-side per-topic throttle). Channel loss = auto-unsubscribe; on reconnect re-SUB with the held cursor. Cursor beyond ring retention → SNAP+reset. SUB targets must hold the data (author or full-sync replica); subscribe-only data is not relayed by default.

**Common**: no polling (all push). The HELLO handshake presents the host's per-peer allowed-topic grants — the enforcement point of the content/metadata access classes (server peers see metadata-class only). Backpressure = per-subscription in-flight bounds + chunks.

Example — coordinator↔worker: mesh.events (full-sync) — the worker appends locally, the coordinator consumes the replicated copy with a local cursor. session.transcript (subscribe-only) — the coordinator SUBs the worker for the tail only.

## 5.7 Beacon — a minimal third point; prediction on the node (owner direction, 2026-08-25)

**Principle: the third point is dumb; prediction lives on nodes.** The beacon exists solely for the non-overlapping-online-windows scenario (3↓ → 1·2 work → 1·2↓ → 3↑): an always-on point that can say "there are changes you don't know about". All comparison, judgment, and warnings run in the node-side library.

**Beacon protocol (all metadata — no content, no intelligence):**
```
PUT-VECTOR  { nodeId, vectors: {topic: {writer: seq}}, hints?: {topic: [key/keyHash + seq]} }
GET-VECTORS { } → every node's last vector + hints (+ report times)
```

**Node-side prediction (library features, fed by beacon/peer vectors):**
1. Wake-up lag summary — "N days stale; per-topic change counts".
2. Pre-write warning — editing a register key while your vector lacks its latest known change → "you would overwrite". Upgrades post-hoc conflict surfacing to prevention.
3. Sole-copy awareness — a node (or host UI) knows it holds entries nobody else has while going offline.

**Two grades of third point (pluggable — host's choice):**
- **Cloud beacon** (ADHDev Workers/DO): content-free vector board. Fully consistent with the content boundary; ~zero cost. The default advantage for cloud users.
- **Self-hosted always-on peer**: run a normal seqscribe node on an always-reachable machine (Tailscale etc.) — a strict superset of the beacon (vectors + full-sync relay). No separate implementation; a normal node is the role.
- None: sync unaffected; prediction gracefully degrades to peer-overlap moments (HELLO exchange).

Library deliverables: `staleness(topic, key?)` + beacon client + reference beacon (`seqscribe-beacon`, kept to tens of lines).

## 5.8 Conflict policy — per-topic/key declaration + custom resolvers

Register topics declare a conflict policy: a default + key-pattern overrides:

```ts
node.defineTopic("config.settings", {
  kind: "register",
  conflict: {
    default: "lww",                        // latest wins (total order)
    overrides: {
      "security.*": "fww",                 // earliest wins among concurrent writes
      "providers.pinned": "resolver",      // delegate to a custom resolver
    },
  },
});

node.onConflict("config.settings", async (c) => {
  // c: { key, entries, concurrent: true, provisionalWinner }
  return c.resolve(mergedValue);           // may be async — custom convergence
});
```

**Convergence safeguards:**
- Resolver outcomes are applied **only by appending a new resolve event** (no direct view manipulation). The resolve event rides the ordinary total order, so all nodes converge identically; concurrent resolves settle by LWW (recursion terminates).
- Until resolution, the default policy's **provisional winner** applies — the system never stalls.
- "resolver" on a key with no registered resolver = keep the provisional winner + surface the conflict event only (de-facto manual resolution queue).

**The fourth policy — `owned` (owner proposal, 2026-08-26):** one owner (nodeId) per key/topic — only the owner writes; conflicts become **impossible by definition**. This lifts the writer-owned-log principle to the data level and codifies ADHDev's implicit discipline (machine config = that machine; transcripts = hosting daemon; task state = coordinator).
- Initial owner = first author. **Transfer (chown) is an event only the current owner can append** — transfer itself is single-writer, hence conflict-free (the recursion closes).
- Non-owner writes: policy choice — `reject` (local refusal) or `request` (write-request queued → applied/approved when the owner returns — an async approval model).
- **Cost (explicit)**: owner offline = that key is unwritable (per-key CP). Recommended only for keys with natural affinity; never the default.
- Orphaned ownership: normal retirement includes bulk transfer (decision 13). Abnormal death goes through host-signed takeover — the library defines only the event shape; issuing authority is the host's.

## 6. API sketch

> ⚠️ Superseded — **the complete API is SPEC §13** (async append, register helpers that stamp causal, full view ABI, verifyTakeover injection, etc.). Kept here only as historical shape.

## 7. Non-goals (v1, codified)

- ✗ Arbitrary multi-writer row merging (general CRDTs) — cr-sqlite's territory. Reduce multi-writer edits to "append an edit event".
- ✗ Consensus / exclusivity arbitration — impossible for CRDTs/logs. Claims belong to the host's serialization points (coordinator/DO).
- ✗ Transport implementations — reference only (in-memory, WebSocket). WebRTC etc. are brought by the consumer.
- ✗ Peer discovery, auth, key management.
- ✗ Server/hub — though joining a hub as "one always-on peer" falls out of the protocol naturally (not a feature).

## 8. Packages, repo, license

- Separate repo (`vilmire/seqscribe`), **FSL-1.1-Apache-2.0** (originally stamped MIT; relicensed 2026-08-26 before any implementation release — permissive for embedding, restricted only against competing hosted offerings, auto-Apache-2.0 after two years. The MIT-licensed npm placeholder 0.0.1 predates the implementation and carries none of it). Candidate third submodule for the ADHDev monorepo.
- Packages: `seqscribe` (core, pure TS) / `seqscribe-ws` (reference transport) / `seqscribe-beacon` (reference beacon, §5.7) / storage adapters (`@seqscribe/*` or `seqscribe-*`).
- npm name `seqscribe` reserved (placeholder 0.0.1 published 2026-08-26). GitHub org `seqscribe` secured.
- Positioning: "extracted from ADHDev, maintained for our needs" — keeps the maintenance debt proportional to scope.

## 9. Implementation policy — complete design first (owner decision, 2026-08-25)

**No staged feature evolution (v0→vN).** The whole system — core (logs, cursors, idempotency, views), Tier-1 full sync, Tier-2 subscriptions, the three topic-policy axes, storage/transport adapters, snapshot bootstrap — is fixed as one complete spec first; the library ships as a finished implementation of that entire spec.

- The only ordering that exists is **ADHDev integration** (operationally impossible to cut every flow over at once): assistant journals → mesh events/ledger (incl. compensation-machinery removal) → dashboard replica → transcripts/status (replacing the three hand-rolled subscription systems) → config. That is consumer-side migration order, not library version stages.
- Public release only after ADHDev production validation. README = competitive map + why-nots + "we run it in production".

## 9.5 ADHDev integration requirements → split into the host repo

The eight bundles of host-side integration work live in the ADHDev repo (`docs/design/2026-08-25-seqscribe-adhdev-integration.md`) — not this repo's concern.

## 9.6 SPEC v3 revision digest (2026-08-26 — second round of three external reviews)

Two structural additions: **finality certificates** (a host-issued, replicated, monotone certificate with a total-order watermark replaces independent per-node cut advancement, which diverges; deterministic quarantine on certificate acceptance) and **fork canonicalization** (retirement directives with finalChain reconverge pre-fork divergence — first-applied retention alone leaves permanent splits). Also: ACK-to-contiguous-mid with stall-by-oldest-unACKed-age, control retries + eager relay fan-out (fixes the anti-entropy-vs-P7 arithmetic), byte-level snapshot chunking, full register snapshot state, causally-defined fww (pure total-order fww collapses to immutable-once-set), causal stamping over the author's own uncommitted tail, and full in-document type definitions (TopicPolicy restoration incl. peerClass enforcement). Details: SPEC v3 header.

## 9.7 SPEC v2 revision digest (2026-08-26 — three external reviews)

Of the §10 decisions, the following were **revised in detail** by SPEC v2 (direction kept; details corrected — SPEC governs):
Decision 3: snapshots per-writer → **topic-level total-order cuts** (reducer state is a cross-writer product). New: **finality window** — rejecting pre-cut late arrivals resolves the compaction-vs-unbounded-lateness conflict. Decision 5: count ACK → **message ids + cumulative ACK**. Decision 6: HAVE reports the **verified contiguous prefix (contig)**, not max observed; out-of-order buffering. Decision 8: checkpoints kept in full inside the finality window (cut advancement = pruning). Also: byte-canonical chainHash (JCS), async append, local-only flags, completed view/subscription ABI, seal_reason split + retirement tombstones, library-stamped causal. Details: SPEC v2 header revision list.

## 10. Stamped decisions (2026-08-26 — details since revised via §9.7 / SPEC v2)

> Includes scale-review revisions A–D and the "ownerless by default" principle (owned is opt-in).

1. **Physical log format = a single SQLite file** (`seqscribe.db`, WAL). Log PK = (topic, writerId, seq). **Only append is on the synchronous critical path** (batched group commit); view materialization is async. Ring topics live outside the durable DB. JSONL is an import/export adapter (mesh-ledger migration + `seqscribe export --jsonl`), not a primary format.
2. **Subscription unit = named view + params** (`{view, params}`). Arbitrary SQL subscriptions rejected (security, cost, schema evolution). All consumers are known applications.
3. **Bootstrap = snapshot + active-window replay (full replay forbidden).** Snapshots/checkpoints/compaction are first-class. Per-kind compaction: register = latest map per key (natural compaction); append-log = active beyond the archive boundary.
4. **E2E encryption = confirmed non-goal.** Under the current trust model (peers = the user's machines; beacon = content-free) there is nothing to protect against. Payloads are JSON values; ciphertext-as-string (base64 etc.) is the extension point — host-level encryption is possible under today's spec. A real key-distribution layer belongs to a future spec with untrusted content relays.
5. **Backpressure = credit window.** Chunks ≤64KB, in-flight 4 per peer/subscription, ACK restores credits. Author-side per-topic flush throttles are policy values. **View deltas computed once per change → shared broadcast** (per-subscriber state is just a cursor). Slow subscribers: subscribe-only degrades to SNAP after queue overflow; full-sync self-paces via pull.
6. **Four error/recovery cases.** ① future cursor = error → consumer restarts from SNAP ② cursor beyond ring = SNAP+reset ③ writerId fork (machine restore/clone): detected via chains → stream freeze + event + host reissues writerId ④ cursor beyond archive = snapshot path (decision 3). DB corruption: views rebuild; logs re-replicate from peers (only your own writer stream is a true loss = reduces to the sole-copy problem).
7. **No npm aliases.** Single identity: seqscribe. GitHub org secured.
8. **Total order = HLC.** Deterministic (hlc, writerId, seq) order + suffix recomputation for late arrivals (cost bounded by checkpoint spacing). Over-ε receipts: **keep stamps for storage/ordering; do not merge into the local clock; flag** (clamping/re-stamping breaks determinism — precise rules in SPEC §3). Forcing order-independent reducers rejected (shifts burden to consumers). Semantic-conflict arbitration is the host's (§7) — merging only promises fact preservation.
9. **Register conventions = §5.8.** Key-level set/add/remove events (whole-file LWW forbidden). Causal attachment is the lightweight per-key last-seen (writer, seq). Conflicts surfaced via `*.conflict` + log-preserved restoration. **Ownerless is the default** — unowned keys flow through lww/fww/resolver.
10. **Beacon = minimal, piggybacked.** Push = 5 s debounce after append + heartbeat piggyback (silent when idle). Key hints plaintext by default with `hintKeys:"hash"` option. Auth is the host's — ADHDev piggybacks vectors on the existing status_report/DO channel; the reference beacon is an `npx seqscribe beacon` subcommand.
11. **Conflict-policy details.** fww = earliest wins among concurrent pairs only (not immutable-once-set — causal successors apply normally; use resolver/owned for immutability). Resolver runs on any detecting node (designated-node model rejected for offline stalls) — resolve-event races settle by LWW. resolve schema = `{kind:"resolve", key, supersedes:[(writer,seq)…], value}`.
12. **Anti-entropy + K-peer fan-out.** Periodic HAVE re-exchange every 5 min (repair path). Eager push to K=3–5 preferred peers + transitive propagation.
13. **Writer retirement.** Seal → absorb into snapshot → remove from vectors. 30-day grace (unseal on return). Includes bulk ownership transfer (host arbitration if no target).
14. **Simulation harness = our own deterministic simulator.** The adapter-injection structure makes it natural (in-memory transport + virtual clock; no external framework). Seeded fuzzing: N nodes, partitions, clock skew, message loss/duplication/reordering. Verified properties: convergence, identical views, no loss, envelope bounds. Registered as a CI gate.
15. **owned details.** Write-request retention 30 days then expiry (with requester notification). Approval API = owner-node callback (`onOwnedRequest`). Takeover = host-signed `{kind:"chown-takeover", key, newOwner, hostSig}` — verification injected by the host. `ownerOf(topic, key)` query API.
