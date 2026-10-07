# Close the remaining integration coverage gaps

## Summary

Closes the last open test gaps from the coverage review, and fixes a shutdown
race the new test exposed.

## Gaps

| ID | Status |
| --- | --- |
| P3 - transactional-path gap resolution | Already covered. `TestProcessBatch` / "stale gap revalidation processes a late commit instead of skipping it" (`daemon_test.go`) drives the probe into a blocked gap, then revalidates to a contiguous window and asserts the refreshed plan is applied with `staleSkipped` false. No new test needed. |
| P10 - backend drop during the detached checkpoint transaction | Added `TestDetachedProjection_CheckpointConnectionDropRecovers`. |
| D11 - shutdown timeout with a handler that ignores cancellation | Added `TestShutdownTimeout_ForceCancelsHandlerThatIgnoresContext`. |
| S5 - a daemon mixing all four handler shapes | Added `TestMixedShapes_AllFourRunInOneDaemon`. |

## P10

The fault pool gained a drop mode that runs `pg_terminate_backend(pg_backend_pid())`
on the checkpoint statement, so the client observes a genuine connection drop
rather than a fabricated statement error. The test asserts the daemon stays up
and the checkpoint eventually reaches the latest position once the pool hands out
a fresh connection.

## D11

A handler that blocks and deliberately ignores `ctx`. With `ShutdownTimeout` at
300ms the test asserts `Start` returns instead of hanging, and that the aborted
batch does not advance the checkpoint. The handler is released at the end and the
test waits for it to finish so nothing touches the pool during teardown.

## S5

One daemon with a transactional per-event projection, a transactional batch
projection, a detached per-event projection, and a detached batch projection,
each scoped to its own stream type. Asserts all four checkpoints reach the
latest position and each shape processed exactly its own event.

## Fix: leader connection race on forced shutdown

D11 exposed a real race under `-race`: on the shutdown give-up path the daemon
called `releaseLeaderConnection` while `runLeaderLoop` could still be committing
a rebalance on the same pooled connection. The pool close then blocked waiting
for the connection.

`shutdown` now waits for the leader loop's own completion channel, bounded by
`ShutdownTimeout`, before releasing the leader connection. A stuck projection no
longer prevents the leader connection from being released, and the release can no
longer overlap an in-flight rebalance.

## Verification

```bash
rtk make check
go test -p 1 -race -tags=integration -timeout=300s ./integration_test/...
```

Lint clean, 165 unit tests, and 69 integration tests under the race detector.
