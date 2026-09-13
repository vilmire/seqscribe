# SPEC amendments — P29 (ratified as v3.7)

> Status: **RATIFIED as SPEC v3.7** (2026-09-13). P29 is applied in [SPEC.md](../SPEC.md) §14 and stamped in [CHANGELOG.md](../CHANGELOG.md). This file is retained as the amendment rationale record. The editorial de-branding pass recorded at the end shipped in the same cycle.
>
> Discovery path for this cycle: **documentation**. v3.3 came from building the vectors and harness, v3.4 from failing P7 runs, v3.5/v3.6 from the ADHDev production integration — P29 came from writing a README quickstart and discovering the example could not be written using public API at all. That is a fourth distinct path, and a cheap one: the first honest "how do I use this" walkthrough is itself a surface audit.

## P29 — `ViewHandle` MUST expose its materialized table name (§14, additive)

`ViewHandle.query(sql)` takes **raw SQL** against the view's materialized table, but the table is named `sqv_<name>_<hash8>` (`src/views.ts` — the `name` slug with a truncated sha256 of the name appended) and `ViewHandle` did not expose it. The handle therefore had a method whose only parameter required information the handle itself withheld: **no correct `SELECT` against a view was writable using public API.**

The gap was load-bearing in practice, not theoretical — every caller in-tree already worked around it by reaching through a private cast:

```ts
// test/adapters-cloud.test.ts, test/provisional.test.ts, e2e-browser/main.src.js
const table = (node as unknown as { _views: { get(n: string): { table: string } } })
  ._views.get("counts").table;
h.query(`SELECT * FROM "${table}" ORDER BY kind`);
```

When the library's own tests and its browser e2e all breach the package surface to use a documented method, the surface is wrong — not the callers. `ViewRegistry.get(name)` already returned `table` internally; only the public handle omitted it.

**Amendment**: add `table: string` to the §14 `ViewHandle` interface —

```ts
interface ViewHandle { name: string; version: string; table: string;
  rebuild(): Promise<void>;
  query<T = Row>(sql: string, params?: unknown[]): T[]; }
```

Stability is what makes this safe as a plain field rather than a getter: `table` is derived purely from the view **name** at registration and is never reassigned. §9 rebuilds re-mint `epoch` and drop/recreate the table's *contents*, but the identifier is stable for the view's lifetime — so a caller may cache it alongside the handle. (`epoch` remains the thing that changes across a rebuild, and remains the field subscribers key on.)

Deliberately **not** taken: exposing `rowKeyCol`, `ftsTable`, or the rest of `ViewRegistry.get()`'s shape. `table` is the minimum that makes the existing `query()` contract usable; the others are internal layout that callers writing their own SQL do not need and should not pin. The FTS mirror stays reachable as `<table>_fts` by the §9 rule without being a separate field.

**Status: implemented 2026-09-13** — one line in the `ViewRegistry.view()` return object (`src/views.ts`) and one field in `src/types.ts`. Purely additive: no existing call signature changes, no behavior changes, no hashed input moves (the table naming rule is unchanged — it is only now *visible*), so [docs/test-vectors.md](test-vectors.md) remains valid unchanged. `npm run check` green (271 tests, strict `types.d.ts` pass, fixture consumability gate). The private `_views` casts in the test suite and `e2e-browser/` are now unnecessary and should be migrated to `handle.table` opportunistically — they are left in place for now because they are also exercising other internals in the same cast.

**Documentation evidence.** Writing the README quickstart required showing a view being queried, since "materializes into derived SQLite views" is the library's primary selling point. The example was drafted three times — against `counts.table` (did not exist), against `coreOf(node).viewTable()` (does not exist; `coreOf` returns `LogCore`), and against a bare `SELECT * FROM counts` (wrong — the real table carries the hash suffix, so it would have failed at runtime in every reader's hands). Only then did reading `src/views.ts:105` and `:160` show that the name was computed, used internally, returned by `ViewRegistry.get()`, and dropped from the public handle. A README that had simply copied the test suite's private cast would have shipped the workaround as the documented usage.

---

## Editorial — de-branding the documentation set (no normative change)

Separate from P29 and **not an amendment**: the documentation assumed familiarity with ADHDev, the project seqscribe was extracted from, in places where a reader has no way to acquire it. Mechanisms were explained by naming ADHDev components (its coordinator, its cloud, its daemon/mcp-server split) rather than by the role those components play, which made generic rules read as site-specific arrangements.

The pass replaces *branding used as explanation* with the role being illustrated, across README, SPEC §7.3, `host-guide.md`, `implementation.md`, `CONTRIBUTING.md`, `DESIGN.md` §1/§9, and the per-item evidence in `proposals-v3.3/4/5.md`.

**One SPEC edit is included** (§7.3, authority constraints): `"adhdev:coordinator"` → `"fleet:coordinator"` as the example identity, and the metadata-class-authority rule is now stated as the general consequence (a peer admitted only to metadata topics holds no content entries, so it cannot certify them) instead of via ADHDev's specific cloud/coordinator split. **The rule itself is unchanged** — only the illustration is. No wire format, constant, state machine, or hashed input moves, and no conformance test changes; this is recorded here rather than left silent because SPEC is frozen between stamps and every edit to it should be traceable, including the editorial ones.

**Deliberately preserved:**
- **`adhdev:m1` in `vectors/vectors.json` and `docs/test-vectors.md` §7.** It is a **hashed input** — `seed("mesh.ledger", "adhdev:m1")` — so renaming it would invalidate the published known-answer corpus for no benefit. The vectors are a frozen artifact; an arbitrary writer-id string inside them carries no meaning a reader must decode.
- **Every concrete symptom, measurement, and failure mode in the proposals files.** What makes an amendment record credible is specificity (an event-loop drift of 6–60 s, a 1.6 GB RSS death, a `lagRows === 0` gate passing over an empty index). Only the *attribution* is genericized — "the production integration" rather than the product name — so the evidence stays probative without requiring the reader to know whose fleet it was.
- **Attribution itself, where it is the point.** README, `DESIGN.md` §9 and `CONTRIBUTING.md` still name ADHDev: that the library is extracted from and dogfooded by a real production system is a fact about its maturity and its maintenance scope, not an assumed-knowledge problem. The distinction drawn throughout is between *naming the origin* (kept) and *explaining a mechanism by reference to it* (removed).
