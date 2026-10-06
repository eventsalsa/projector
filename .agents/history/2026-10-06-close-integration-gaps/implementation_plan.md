# Implementation plan: close pre-existing integration test gaps

## Context

While adding detached-projection coverage, an audit of the integration suite
surfaced paths that were never exercised, independent of that feature. They were
recorded as `P1`-`P10` in the detached-projections review matrix and deferred to
their own branch. This branch closes the subset that is deterministic and does
not depend on the detached feature.

## Scope

Close `P1`, `P2`, `P4`, `P5`, `P6`, `P7`, `P8`, and `P9`.

Out of scope: `P3` (transactional-path gap resolution between the probe and the
batch transaction, not deterministically controllable) and `P10` (detached
checkpoint connection drop, which belongs with the detached work).

## Approach

1. Extend the harness so fatal daemon exits can be asserted:
   `testProjectorHarness` records the `Start` result on a `done` channel and
   exposes `awaitExit`.
2. Add starters that accept arbitrary projections and a configurable instances
   table, for the graceful-drain and schema-qualified tests.
3. Add `integration_test/gaps_test.go` for `P1`, `P2`, `P4`, `P5`, `P6`, `P7`,
   `P8` and `integration_test/gaps_schema_test.go` for `P9`.
4. For `P1`, install a sequence-backed trigger that raises SQLSTATE 40001 once on
   the checkpoint row, so the failure is a genuine server error rather than a
   fabricated one, then assert recovery.

## Verification

```bash
rtk make check
go test -p 1 -race -tags=integration -timeout=300s ./integration_test/...
```

Expected: lint clean, unit tests green, and 33 integration tests green (25
pre-existing plus 8 new).
