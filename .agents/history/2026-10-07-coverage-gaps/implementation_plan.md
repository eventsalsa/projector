# Implementation plan: close remaining integration coverage gaps

## Scope

Close the four coverage items left open by the earlier review: `P3`, `P10`,
`D11`, `S5`.

## Findings before writing code

- `P3` was already covered. `TestProcessBatch` in `daemon_test.go` includes the
  "stale gap revalidation processes a late commit instead of skipping it" case,
  which drives the probe into a blocked gap and then revalidates to a contiguous
  window. No new test.
- `D11` has no blocker: `shutdown` force-cancels the processing context and gives
  up after `ShutdownTimeout` even if a handler ignores cancellation.
- `P10` can be made realistic by terminating the backend running the checkpoint
  transaction, rather than fabricating an error.

## Approach

1. Extend the fault pool with a drop mode that runs
   `pg_terminate_backend(pg_backend_pid())` on the checkpoint statement, and add
   `TestDetachedProjection_CheckpointConnectionDropRecovers`.
2. Add `TestShutdownTimeout_ForceCancelsHandlerThatIgnoresContext` using a
   handler that blocks and ignores `ctx`, and waits for the leaked goroutine to
   finish before teardown.
3. Add `TestMixedShapes_AllFourRunInOneDaemon` with one projection of each shape,
   scoped by stream type.
4. Fix the shutdown race that D11 exposed: wait for the leader loop's completion
   before releasing the leader connection.

## Verification

```bash
rtk make check
go test -p 1 -race -tags=integration -timeout=300s ./integration_test/...
```

Expected: lint clean, unit tests green, integration tests green under the race
detector.
