# Close pre-existing integration test gaps

## Summary

Closes the pre-existing test gaps found while auditing the projector's
integration suite during the detached-projections work. These are paths the
library did not exercise before; they are independent of that feature and land
on their own branch.

No production behavior changed. The only non-test change is promoting
`golang.org/x/sync/errgroup` from an indirect to a direct dependency, because a
test now imports it.

## Gaps closed

| ID | Test | Covers |
| --- | --- | --- |
| P1 | `TestGap_RealSerializationFailureIsRetried` | A genuine server-side SQLSTATE 40001 raised by a checkpoint trigger fails the batch and the daemon recovers on retry |
| P2 | `TestGap_CheckpointNeverRegresses` | `SaveCheckpoint`'s `GREATEST` guard rejects a lower position |
| P4 | `TestGap_HeartbeatRegistrationLossIsFatal` | Losing the instance registration makes `Start` return `ErrInstanceRegistrationMissing` |
| P5 | `TestGap_MaxConsecutiveFailuresIsFatal` | Exceeding `MaxConsecutiveFailures` makes `Start` return `ErrConsecutiveFailures` |
| P6 | `TestGap_GracefulShutdownDrainsInFlightBatch` | An in-flight transactional batch commits within `ShutdownTimeout` on `Stop` |
| P7 | `TestGap_ErrgroupPropagatesFatalToSibling` | Two daemons sharing a pool in an `errgroup`: a fatal in one returns from `Wait` |
| P8 | `TestGap_SequentialStaleSkipsAreAudited` | Two sequential stale gaps produce two distinct audit rows |
| P9 | `TestGap_SchemaQualifiedProjectorTables` | A daemon configured with `infra.*` projector tables registers and checkpoints there, leaving the default tables untouched |

## Not closed

- **P3** (`revalidateStaleGapSkip` gap-resolved branch on the transactional
  path): the branch fires only when a gap commits between the frontier probe and
  the batch transaction, which is not deterministically controllable from a
  test. The equivalent behavior on the detached path is covered by `D7` on the
  detached-projections branch.
- **P10** (backend connection drop during the detached checkpoint
  transaction): detached-specific, so it belongs with the detached work rather
  than this branch.

## Harness changes

- `testProjectorHarness` now records the daemon's exit result and exposes
  `awaitExit`, so fatal shutdown paths can be asserted without the cleanup
  helper turning the expected error into a test failure.
- `startTestProjectorWithProjections` and `startTestProjectorWithTables` start a
  daemon from arbitrary projections and wait on a configurable instances table,
  which the schema-qualified test needs.
- A slow transactional projection drives the graceful-drain test.

## Verification

```bash
rtk make check
go test -p 1 -race -tags=integration -timeout=300s ./integration_test/...
```

Lint is clean, 132 unit tests pass, and 33 integration tests pass (the 25
pre-existing ones plus these 8).
