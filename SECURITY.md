# Security policy

seqscribe is pre-release (`0.1.0-dev`); there are no supported released
versions yet. Until a versioned release exists, `main` is the only line that
receives fixes.

## Reporting a vulnerability

Report privately via [GitHub security advisories](https://github.com/vilmire/seqscribe/security/advisories/new).
Please do not open public issues for suspected vulnerabilities.

## Scope and trust model

The library's trust model is defined in SPEC §4.1 and docs/host-guide.md:
identity issuance, transport authentication, and authority signing are host
obligations — the library verifies chains, certificates, and directives but
deliberately does not authenticate peers. Reports most valuable to us:

- A peer holding a valid channel (any grant tier) that can corrupt another
  node's converged state, crash the host process, or exhaust its memory or
  storage beyond the documented bounds.
- Chain/certificate/directive verification bypasses (content-level forgery).
- Determinism escapes: un-injected nondeterminism reachable under the
  simulation harness (breaks SPEC §19 P5 and the failing-seed reproducibility
  story).

Out of scope: attacks requiring a compromised host, a forged host-issued
identity, or a malicious authority — those are the host's contract (see the
host-guide's adjudication runbooks).
