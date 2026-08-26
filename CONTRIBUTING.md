# Contributing

seqscribe is maintained for [ADHDev](https://adhf.dev)'s needs first and is
pre-release; external contributions are welcome but the bar below is fixed.

## The contract comes first

[SPEC.md](SPEC.md) is the sole normative contract and it is **frozen**: no PR
edits it directly. Changes to protocol, storage, API, or acceptance behavior go
through a proposal in `docs/proposals-*.md` (see the existing files for the
required shape: rationale, amendment text, evidence, implementation status) and
land in the SPEC only via a stamped revision. Extensions that nothing in the
current SPEC forbids may ship ahead of ratification — every proposal that does
carries a "Status: implemented" line naming its regression tests.

## What a PR must include

- `npm run check` green — that is gen:types, the §14 `types.d.ts` typecheck,
  `tsc`, the full vitest suite (vectors, fixed-seed corpus, the §19 P7 gate),
  the build, and the fixture-package typecheck. CI runs exactly this plus
  `FRESH_SEEDS=10`.
- Tests with teeth for any behavior change. House style: directed tests cite
  the SPEC section they pin; distributed behavior goes through the
  deterministic harness (`harness/` — injected clock/timers/rng, virtual bus),
  never wall-clock sleeps.
- Comments only where the code can't say it: SPEC citations and *why*, not
  what. Match the density and idiom of the file you're in.
- A seed that breaks a property joins `harness/seeds.json` **permanently**
  (§19). Minimize it first: `node tools/minimize-seed.mjs <seed>`.

## Security

Suspected vulnerabilities go through [SECURITY.md](SECURITY.md), not issues.

## Licensing

Dual FSL-1.1-Apache-2.0 OR AGPL-3.0-only ([LICENSE](LICENSE)). By
contributing you agree your contribution is licensed under the same terms.
