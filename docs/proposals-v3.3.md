# Proposed SPEC amendments — v3.3 candidates

> Status: **RATIFIED as SPEC v3.3** (2026-08-26, at implementation start — the owner's "구현작업 시작" directive accepted the blocking proposals; P1 was a hard prerequisite for milestone ⓪). All four items are applied in [SPEC.md](../SPEC.md) and stamped in [CHANGELOG.md](../CHANGELOG.md); the test-vectors §7 values are now authoritative. This file is retained as the amendment rationale record. Originally surfaced by building the known-answer vectors ([test-vectors.md](test-vectors.md) §8) and designing the simulation harness ([harness.md](harness.md)).

---

## P1 — Injectable timers (blocking: P5/P7 are unimplementable without it)

**Defect.** §19 mandates seed determinism (P5) with "injected clock **and RNG**", and P7 measures catch-up in *virtual* time. But the library schedules future work from six constants (CONTROL_RETRY_MS, ANTI_ENTROPY_MS, GROUP_COMMIT_MS, CHANNEL_STALL_MS, HELLO_TIMEOUT_MS, BEACON_DEBOUNCE_MS), and `createSeqscribe` injects only `clock()` and `rng()` — timer *firing* would come from real `setTimeout`, which is wall-clock, non-deterministic, and invisible to the virtual scheduler. `clock()` makes time *readable* deterministically but not *schedulable* deterministically.

**Amendment.** In §14, extend `createSeqscribe` opts:

```ts
timers?: {
  setTimeout(cb: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
};  // default: host globals. The harness injects virtual-time timers; with
    // clock/rng/timers all injected, the library performs no nondeterministic
    // operation (P5's actual precondition).
```

And in §19 first paragraph, amend "injected clock **and RNG**" → "injected clock, RNG, **and timers**".

**Impact.** Additive, optional, no wire/storage change. types.d.ts gains one optional field.

---

## P2 — topicSchemaHash: exact normalized object (interop gap)

**Defect.** §14 fixes the hashed field *set* — `{kind, conflict (incl. §11.7 matching semantics), registerSemanticsVersion: 1, finalityAuthority}` — but not the object's exact shape: what an append topic's `conflict` serializes as, what an absent `finalityAuthority` serializes as, and how defaults materialize structurally. Two conforming implementations can disagree on every hash, and a hash mismatch is load-bearing (ERR_SCHEMA_MISMATCH refuses sync).

**Amendment.** Replace the §14 comment's first sentence with:

```
topicSchemaHash = sha256(JCS(N)) where N is exactly:
  { kind,
    conflict: kind=="register"
        ? { default: conflict?.default ?? "lww", overrides: conflict?.overrides ?? {} }
        : null,
    registerSemanticsVersion: 1,          // carries §11.7 matching semantics; bump on change
    finalityAuthority: finalityAuthority ?? null }
```

**Impact.** Pins byte-level interop; matches the vectors in [test-vectors.md](test-vectors.md) §7 (which become authoritative on ratification). No behavior change for any single-implementation fleet.

---

## P3 — Unicode normalization: one clarifying sentence (documentation-only)

**Defect (latent).** JCS does not normalize unicode; §4 never says so. Visually identical NFC/NFD strings in payloads and register `Key`s produce different bytes, different chains, and *different register keys*. Correct behavior is bytes-as-authored — but an implementation that "helpfully" NFC-normalizes before hashing would diverge from one that doesn't, and nothing in the SPEC forbids it.

**Amendment.** Append to §4.2:

```
Strings hash and compare as authored — implementations MUST NOT apply unicode
normalization (NFC/NFD) at any point; two keys differing only in normalization
form are distinct keys.
```

**Impact.** None for correct implementations; forecloses a divergence class.

---

## P4 — `dec()` definition (editorial)

**Defect (cosmetic).** §4.3 uses `dec(seq)` without defining it. Only one reading is sensible for non-negative safe integers, but the chain format deserves zero ambiguity.

**Amendment.** Add to §4.3 before the seed definition:

```
dec(n) = the base-10 ASCII representation of the non-negative safe integer n,
no sign, no leading zeros ("0" for 0).
```

**Impact.** None; matches the vectors.

---

## Ratification checklist (when stamping any subset)

1. Apply the text; bump SPEC header to v3.3 with a revision line naming what changed and why.
2. Add the CHANGELOG.md entry (newest first, same self-critical format).
3. If P2 is ratified: mark [test-vectors.md](test-vectors.md) §7 authoritative (remove the ⚠ banner) and re-run `node tools/gen-vectors.mjs` (values should not change — the generator already implements P2).
4. If P1 is ratified: unblock milestone ⓪/① in [implementation.md](implementation.md).
