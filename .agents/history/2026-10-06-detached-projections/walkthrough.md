# Detached projections and a registry builder

## Summary

This change fixes two problems with projections whose read model is not in the
daemon's PostgreSQL database:

1. Projection handlers ran inside the daemon's batch transaction, so a remote
   handler doing HTTP held a pooled connection and both the assignment and
   checkpoint row locks for the whole handler loop. One slow endpoint could
   starve the heartbeat and block the leader's rebalance transaction.
2. The handler contract was per-event only, so destinations with bulk APIs
   (Typesense import, Elasticsearch bulk, multi-row upsert) could not be used
   efficiently.

It also introduces a registry builder so the four handler shapes can coexist
without a lowest-common-denominator interface, and normalizes handler shapes
behind one internal interface so future concerns (tracing, metrics, retry) are
written once.

## What changed

### Four handler shapes selected by a registry

`Projection` keeps its name and signature, so existing projection types are
untouched. Three siblings were added: `DetachedProjection`,
`BatchProjection`, and `DetachedBatchProjection`. Go does not allow two methods
with the same name on a type, so each type implements exactly one shape.

`projector.New` now takes a `*Registry` instead of `[]Projection`. Build one
with `NewRegistry` and `Add`/`AddProjection`/`AddDetached`/`AddBatch`/
`AddDetachedBatch`, or use `FromProjections(p1, p2)` for daemons that only use
the classic shape.

Registration filters moved to options: `OnStreamTypes`, `OnEventTypes`.
`FilterStreamTypes` and `FilterEventTypes` remain for the classic shape and are
deprecated.

### Detached execution path

For a detached applier the daemon:

1. Probes the frontier in a short read-only transaction, as before.
2. If the plan is a stale-gap advance, revalidates it in a short read-only
   transaction and freezes the applied rows and checkpoint target.
3. Applies the frozen rows with **no transaction held** (`nil` transaction).
4. Writes the checkpoint, and any stale-gap audit record, in a short
   transaction. Only this transaction is retried on a serialization failure.
5. If the gap the frozen plan was skipping past closed during the apply, the
   checkpoint write is discarded and the batch is re-probed instead of skipping
   the newly available event.

Transactional projections keep the original behavior: one transaction, the two
`FOR UPDATE` row locks, in-transaction revalidation, and exactly-once effect on
a same-database read model.

### Other

- `BatchStats` gained a `Detached` field; `EventsHandled` is now the count of
  events actually applied after registration filtering.
- The handler-shape plurality lives at the registry boundary only: four
  adapters map the public shapes onto one internal `applier`, and filters and
  future middleware decorate that single interface.

## Delivery contract

Transactional projections remain exactly-once per committed batch.

Detached projections are **at-least-once**. Events can be applied again after a
crash between the external apply and the checkpoint write, when the checkpoint
transaction is retried on a serialization failure, or when a projection is
reassigned during an apply. Detached handlers must be idempotent: deterministic
document ids plus a monotonic version guard from `StreamVersion` or
`GlobalPosition`. A destination error fails the whole batch and the checkpoint is
never advanced past work that was not applied.

## Compatibility

- Projection type declarations and `Handle` methods do not change.
- The only required downstream edit is the `projector.New(...)` call site per
  daemon. `FromProjections` keeps the classic case a one-liner.
- `FilterStreamTypes`/`FilterEventTypes` are deprecated but still work.

## Tests

Unit tests cover the registry (shape inference, filters, validation),
the shape adapters, and the detached executor (checkpoint committed, apply
error skips the checkpoint, checkpoint moved under the apply, gap resolved
during the apply requests a retry, and a checkpoint serialization failure
retries without repeating the apply).

Integration tests added (all run under `-race` and `-p 1`, matching CI):

| Group | IDs | Covers |
| --- | --- | --- |
| Detached happy path and isolation | D1, D2, D17 | Apply outside any tx, checkpoint afterwards, a blocked handler on a single-connection pool, no session left `idle in transaction`, and the assignment row lockable during the apply |
| Detached failures | D3, D4, D5, D8, D9, D10, D16 | Destination error, partial batch failure, checkpoint write failure and replay, checkpoint serialization retry without repeating the apply, ownership lost between apply and checkpoint, shutdown mid-apply, apply past `BatchTimeout` |
| Detached gaps and progress | D7, D12, D13, D14, D15 | Gap resolved during the apply retries instead of skipping, filter with zero matches, blocked on a gap then resolves, detached batch bulk window, per-event partial failure |
| Detached checkpoint safety | D6 | Checkpoint advanced by another worker during the apply does not regress |
| Handler shapes | S1, S2, S3, S4, S7, S8, S9 | Batch window in one call, batch rollback, window boundaries, batch never receives a gapped position, `FromProjections`, deprecated filter wrapper |
| Registration filters | F2, F3, F4, F5, F6 | Event-type filter, combined stream and event filter, per-projection filter isolation, observer accounting, filter with detached |
| Cross-cutting | T1, T2, T3 | `BatchTimeout` cancellation, checkpoint row contention, permanent-hole resume after restart without a duplicate skip row |

Unit tests cover the registry, the shape adapters, filters, and the detached
executor, including checkpoint-moved, gap-resolved, and serialization-retry-only
cases.

`rtk make check` (lint, unit with `-race`, integration via testcontainers) and
`go test -p 1 -race -tags=integration ./integration_test/...` both pass: 63
integration tests, including the 25 pre-existing ones.

### Pre-existing gaps closed (consolidated onto this branch)

The pre-existing coverage gaps found during the audit are included here rather
than in a separate PR:

| ID | Test | Covers |
| --- | --- | --- |
| P1 | `TestGap_RealSerializationFailureIsRetried` | A genuine server-side SQLSTATE 40001 on a checkpoint write fails the batch and the daemon recovers on retry |
| P2 | `TestGap_CheckpointNeverRegresses` | `SaveCheckpoint`'s `GREATEST` guard rejects a lower position |
| P4 | `TestGap_HeartbeatRegistrationLossIsFatal` | Losing the instance registration makes `Start` return `ErrInstanceRegistrationMissing` |
| P5 | `TestGap_MaxConsecutiveFailuresIsFatal` | Exceeding `MaxConsecutiveFailures` makes `Start` return `ErrConsecutiveFailures` |
| P6 | `TestGap_GracefulShutdownDrainsInFlightBatch` | An in-flight transactional batch commits within `ShutdownTimeout` on `Stop` |
| P7 | `TestGap_ErrgroupPropagatesFatalToSibling` | Two daemons sharing a pool in an `errgroup`: a fatal in one returns from `Wait` |
| P8 | `TestGap_SequentialStaleSkipsAreAudited` | Two sequential stale gaps produce two distinct audit rows |
| P9 | `TestGap_SchemaQualifiedProjectorTables` | A daemon configured with `infra.*` projector tables registers and checkpoints there, leaving the default tables untouched |

The integration harness now records the daemon's exit result and exposes
`awaitExit`, so fatal shutdown paths can be asserted, and gains starters that
accept arbitrary projections and a configurable instances table. `P3` (the
transactional-path gap-resolution branch) is not deterministically controllable
from a test; `P10` (a connection drop during the detached checkpoint) remains
open. `golang.org/x/sync` is promoted to a direct dependency because a test
imports `errgroup`.

## Coverage status

Implemented: D1-D10, D12-D17, S1-S4, S7-S9, F2-F6, T1-T3, P1, P2, P4-P9.

Still open in the review matrix: D11 (shutdown timeout with a handler that
ignores cancellation) and S5 (a daemon mixing all four shapes; D2 covers
detached plus transactional per-event). S6 (registry validation) is unit-tested
rather than integration-tested. P3 and P10 are noted above.

One intended deviation: the plan's D15 expected a per-event checkpoint after
each successful apply. The implementation checkpoints per batch, so a partial
per-event failure leaves the checkpoint at the batch start and replays the whole
window, which is what the test asserts. Per-event progress would need an
executor change and is a design question, not a test gap.

## Verification

```bash
rtk make check
```

No migrations or schema changes are required.
