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

Integration tests added:

| ID | Test | Covers |
| --- | --- | --- |
| D1 | `TestDetachedProjection_ProcessesAndAdvancesCheckpoint` | Detached apply outside any tx, checkpoint afterwards |
| D2 | `TestDetachedProjection_DoesNotHoldConnectionDuringRemoteIO` | A blocked detached handler on a single-connection pool does not stop a transactional projection |
| D3 | `TestDetachedProjection_ApplyErrorDoesNotAdvanceCheckpoint` | Destination error keeps the checkpoint and recovers |
| S1 | `TestBatchTransactionalProjection_HandlesWindowInOneCall` | One batch handler call per window |
| S2 | `TestBatchTransactionalProjection_ErrorRollsBackWholeBatch` | Batch error rolls back the read model and checkpoint |
| F2 | `TestRegistrationEventTypeFilter_AdvancesCheckpointPastSkippedEvents` | `OnEventTypes` end to end |
| T1 | `TestBatchTimeout_CancelsBlockedHandlerWithoutAdvancingCheckpoint` | Batch timeout cancels a blocked handler |

`rtk make check` (lint, unit with `-race`, integration via testcontainers)
passes, including the 25 pre-existing integration tests.

## Coverage status

Implemented here: D1, D2, D3, S1, S2, F2, T1.

Still to add from the review matrix: D4-D17 (partial batch failure,
crash/checkpoint-failure replay, checkpoint-moved, gap-resolved, ownership
mid-apply, shutdown, filters with detached, detached batch, apply timeout,
rebalance isolation), S3-S9 (batch target truncation, batch with gap, mixed
shapes, registry validation, `FromProjections` equivalence, deprecated wrapper,
window boundary), F1/F3-F6 (stream filter, combined filters, per-projection
filter isolation, observer accounting, filter with detached), T2 (checkpoint row
contention) and T3 (permanent-hole resume).

Pre-existing gaps not addressed by this change are tracked separately in the
review matrix (P1-P10).

## Verification

```bash
rtk make check
```

No migrations or schema changes are required.
