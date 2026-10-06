# Detached projections and a handler-shaped registry

## Context

Today every projection handler runs inside the daemon's batch transaction
(`daemon.go:1020` opens it, `handleRelevantEvents` at `daemon.go:1159` calls
`Projection.Handle` per event, `daemon.go:1065` commits). Two `FOR UPDATE` row
locks are held for the whole handler loop: the assignment row
(`ensureProjectionOwnership`, `daemon.go:1622`) and the checkpoint row
(`GetCheckpointForUpdate`, `postgres/checkpoint.go:37`).

That is correct and valuable when the read model lives in the same PostgreSQL
database and every write goes through `tx`. It is harmful when the read model is
remote:

- A remote handler performs network I/O while holding a pooled connection and
  both row locks. One slow endpoint can starve the heartbeat
  (`daemon.go:384` uses the same pool) and stall the leader's single rebalance
  transaction, which blocks `SetAssignments` for every projection
  (`daemon.go:566`).
- `BatchTimeout` (30s) rolls the transaction back, but the network side effects
  already happened, so the "read model and checkpoint are atomic" guarantee is
  false for remote stores.
- The stale-gap path retries the whole attempt up to `staleGapRetryLimit` (3),
  replaying external writes (`daemon.go:979`).
- `MaxConsecutiveFailures` (5) raises `ErrConsecutiveFailures`, a fatal daemon
  shutdown. A flaky search API can take the whole projector down.

We will add a **detached** processing path for projections whose read model is
outside the daemon's PostgreSQL: the handler is applied with no transaction
held, and the checkpoint is persisted afterwards in a short transaction. This
changes the delivery contract for those projections from exactly-once to
at-least-once, which requires idempotent handlers.

We will also replace the flat `[]Projection` daemon input with a strongly typed
builder registry so the four handler shapes can coexist without a lowest-common
-denominator interface. Batching is a natural part of that shape set and is
included here as interface surface, but no batching behavior is implemented
beyond passing the batch to batch-shaped handlers.

## Goals

- Remote read models never hold a pooled connection or a row lock during
  network I/O.
- Existing same-DB projections keep exactly-once semantics and their two row
  locks. Their handler bodies do not change.
- Four handler shapes are supported, each with a `Handle` method:
  `(ctx, tx, event)`, `(ctx, event)`, `(ctx, tx, events)`, `(ctx, events)`.
- The daemon input becomes a builder registry with strong typing and
  registration-level filters.
- Projection implementations stay untouched; only the `projector.New(...)` call
  site changes downstream.

## Non-goals

- No outbox/relay.
- No lease table. Ownership remains assignment-based; detached duplicates across
  a rebalance are accepted under idempotency.
- No change to the frontier algorithm. The safe-harbor skip behavior is
  preserved; the detached path only moves where the authoritative decision is
  taken (see "Detached execution algorithm").
- No migration changes. No new tables.

## Public API

### Four handler interfaces (`projection.go`)

`Projection` keeps its name and signature so every existing downstream
projection type still satisfies it.

```go
type Projection interface {
	Name() string
	Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error
}

type DetachedProjection interface {
	Name() string
	Handle(ctx context.Context, event store.PersistedEvent) error
}

type BatchProjection interface {
	Name() string
	Handle(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) error
}

type DetachedBatchProjection interface {
	Name() string
	Handle(ctx context.Context, events []store.PersistedEvent) error
}
```

Go forbids two methods with the same name on one type, so a concrete type
implements exactly one shape. Detection is total and unambiguous.

### Registry (`registry.go`)

```go
func NewRegistry() *Registry

// FromProjections builds a registry from transactional per-event projections
// so single-shape daemons stay a one-liner.
func FromProjections(projections ...Projection) *Registry

func (r *Registry) Add(p any, opts ...RegistrationOption) error // auto-detect
func (r *Registry) AddProjection(p Projection, opts ...RegistrationOption) error
func (r *Registry) AddDetached(p DetachedProjection, opts ...RegistrationOption) error
func (r *Registry) AddBatch(p BatchProjection, opts ...RegistrationOption) error
func (r *Registry) AddDetachedBatch(p DetachedBatchProjection, opts ...RegistrationOption) error

type RegistrationOption func(*registration)

func OnStreamTypes(types ...string) RegistrationOption
func OnEventTypes(types ...string) RegistrationOption
```

- `Add` type-switches over the four interfaces (disjoint, so order is
  irrelevant), returns `ErrUnknownProjectionShape` when a value implements none,
  and `ErrNilProjection` for nil.
- All `Add*` methods validate a non-empty, unique `Name()` at registration time
  and return an error on violation. `New` revalidates defensively.
- Filters combine with AND semantics: a registration with both options passes an
  event only when it matches both allowlists.
- `Registry` stores `[]registration` in registration order.

### Daemon constructor (`daemon.go`)

```go
func New(db PgxPool, eventStore projectorStore, registry *Registry, opts ...Option) *Daemon
```

One constructor, because Go has no overloading and the registry is the single
input for all shapes. `FromProjections` keeps the common tx-event daemon
readable. This is the only downstream call-site change; projection bodies are
untouched.

### Filters

`FilterStreamTypes` and `FilterEventTypes` stay for the `Projection` (tx-event)
shape, implemented over the same internal filter decorator, and are marked
deprecated in their doc comments in favor of `OnStreamTypes`/`OnEventTypes`.
They only need to wrap one shape, so no wrapper multiplication.

## Internal architecture

Three seams keep shape and concern multiplicity out of the core.

### Normalized applier (`applier.go`)

```go
type applier interface {
	apply(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) (int, error)
	transactional() bool
}
```

Four boundary adapters are the only shape-aware code in the daemon:

- `txEventApplier`: loops `Handle(ctx, tx, event)`, `transactional() == true`.
- `detachedEventApplier`: loops `Handle(ctx, event)`, `transactional() == false`.
- `txBatchApplier`: single `Handle(ctx, tx, events)`, `transactional() == true`.
- `detachedBatchApplier`: single `Handle(ctx, events)`, `transactional() == false`.

Concern decorators wrap the normalized applier and are written once:

- `filteredApplier`: applies the stream/event allowlists to the slice, forwards
  the subset, returns the inner applied count, forwards `transactional()`.
- Future tracing/metrics/retry decorators follow the same pattern.

`registration` holds `{name, filters, applier}`; the registry builds the applier
chain once (`adapterFor` then `wrapFilters`), so the daemon and executors never
branch on shape.

### Batch plan (`frontier.go` / `daemon.go`)

Rename `frontierProbe` to `batchPlan` (and `buildFrontierProbe` to
`buildBatchPlan`) and always truncate `plan.rows` to `position <= target`. This
removes the `upperBound` argument from the handler dispatch and makes "what was
read" and "what is applied" identical, which the detached path depends on.

Keep the planning functions but split their transaction coupling:

- `d.probePlan(ctx, projectionName, gapTracker, override...) (batchPlan, error)`:
  begins the short read-only transaction and runs today's `probeFrontier`.
- `d.revalidatePlan(ctx, tx pgx.Tx, projectionName string, pl *batchPlan) error`:
  today's `revalidateStaleGapSkip`, callable both inside the tx executor and in a
  short read-only transaction by the detached executor.

### Executors (`daemon.go`)

`processBatchWithGapState` becomes: `probePlan` → observer bookkeeping →
dispatch on `entry.applier.transactional()` → `processTxBatch` or
`processDetachedBatch` → observer bookkeeping.

- `processTxBatch`: today's `processProbedBatch` +
  `processProbedBatchAttempt` + `prepareProbeForBatch`, with
  `handleRelevantEvents` replaced by `entry.applier.apply(ctx, tx, pl.rows)`.
  Behavior, locks, and serializable retry are unchanged.
- `processDetachedBatch`: new (below).

## Detached execution algorithm

Per batch, for an applier with `transactional() == false`:

1. `pl := d.probePlan(...)` in a short read-only transaction. If
   `pl.target <= pl.checkpoint`, return without work (unchanged early exit).
2. If `pl.staleSkipped`, run `d.revalidatePlan` in a short read-only transaction
   and **freeze** the resulting rows and target. This is the authoritative
   decision; it must not be re-derived forward after the apply.
3. `applied, err := entry.applier.apply(ctx, nil, pl.rows)`. No transaction, no
   connection, no row lock. The detached adapters ignore the nil `tx`.
4. Checkpoint in a short transaction, retrying **only this transaction** on
   serialization failure (reuse `isSerializationFailure`):
   1. `ensureProjectionOwnership(ctx, tx, name)`; if not assigned, roll back and
      return `errProjectionOwnershipLost`.
   2. `current := postgres.GetCheckpointForUpdate(ctx, tx, ...)`; if
      `current != pl.checkpoint`, roll back and return a `blockedByGap` result
      without writing. This mirrors `prepareProbeForBatch` and prevents a
      checkpoint jump past another worker's progress. The row lock is held only
      for this short transaction.
   3. If `pl.staleSkipped`, re-check the gap in this transaction
      (`d.store.ReadEvents` + `buildBatchPlan`). If the gap has resolved,
      roll back and return a `retryRequested` result: rule 2 from the design
      discussion. This keeps the loss window at attached-path parity.
   4. `recordStaleGapSkip` when `pl.staleSkipped`.
   5. `postgres.SaveCheckpoint(ctx, tx, ..., pl.target)`.
   6. Commit.
5. Return `processedBatch{progressed: true, checkpoint: pl.target, ...}`.

Notes:

- Ownership is only held as a row lock inside step 4, never across the apply, so
  the leader's rebalance is not blocked by remote I/O.
- On `retryRequested`, the projection loop re-probes immediately once without
  incrementing `consecutiveFailures`; if it recurs, fall back to normal backoff.
  Bound it to one immediate retry per batch to avoid livelock.
- `BatchTimeout` bounds the apply. The checkpoint transaction uses a short
  internal timeout so it is not squeezed by an apply that consumed the whole
  batch budget.
- Duplicates are expected and covered by idempotency. Detached handler
  documentation must state: use deterministic document ids and a monotonic
  version guard from `StreamVersion`/`GlobalPosition`.

## File changes

| File | Change |
| --- | --- |
| `projection.go` | Add `DetachedProjection`, `BatchProjection`, `DetachedBatchProjection`. Keep `Projection` and the two filter wrappers (deprecated). |
| `registry.go` (new) | `Registry`, `NewRegistry`, `FromProjections`, `Add*`, `RegistrationOption`, `OnStreamTypes`, `OnEventTypes`, validation errors. |
| `applier.go` (new) | `applier` interface, four adapters, `filteredApplier`, `adapterFor`, `wrapFilters`. |
| `frontier.go` | `batchPlan` type (renamed from `frontierProbe`) and `buildBatchPlan`; always truncate rows to target. Keep pure frontier math. |
| `daemon.go` | `New` takes `*Registry`; store `[]registration` + name map; `validate`; `projectionNames`/`projectionByName`; `syncAssignments` start/stop types; rename `probeFrontier`→`probePlan`, `revalidateStaleGapSkip`→`revalidatePlan`; `processTxBatch`; new `processDetachedBatch`; dispatch in `processBatchWithGapState`; `processedBatch.retryRequested`; loop handling of the retry signal. |
| `config.go` | No new option required. Document `BatchTimeout` covering the detached apply. |
| `observer.go` | Add `Detached bool` to `BatchStats`. `EventsHandled` becomes the post-filter applied count. |
| `projection_test.go` | Rewrite filter tests for registry options and the deprecated wrappers. |
| `daemon_test.go` | Replace `New(..., []Projection{...}, ...)` and `Daemon{projections: ...}` with registry construction; add detached executor tests. |
| `integration_test/*` | Build registries; add the two new integration tests below. |
| `README.md` | Update quick start, projection contract, decorators, processing model, checkpoint semantics, pool sizing, and the at-least-once contract for detached projections. |

## Decisions

Recommended, and open to your adjustment at review:

1. **Single `New` taking `*Registry`.** Go has no overloading. `FromProjections`
   keeps tx-event-only daemons a one-liner. This changes one call site per
   daemon downstream.
2. **Detached failure policy unchanged for now.** `MaxConsecutiveFailures`
   still triggers a fatal shutdown. A softer detached policy (or a separate
   tolerance) is worth a follow-up, but I would keep this change scoped.
3. **`BatchTimeout` bounds the apply; checkpoint transaction gets a short
   internal timeout.** No new public option.
4. **Filters as registry options**, with the two wrapper functions kept as
   deprecated tx-event sugar.
5. **Batch handlers return `error` only**; the executor counts `len(events)`.
   No partial-count contract.
6. **`EventsHandled` semantics change** to the post-filter applied count. This
   is a telemetry-only change; document it.

## Verification

Must pass `rtk make check` (`golangci-lint` + tests) before any commit.

### Unit tests

- Registry: `Add` auto-detects each of the four shapes; `Add*` typed methods;
  empty/duplicate/nil names rejected; `Add` rejects an unknown shape;
  `FromProjections` produces equivalent behavior.
- Appliers: each adapter forwards correctly; per-event adapters stop at the
  first error and return the applied count; detached adapters ignore `tx`;
  `filteredApplier` filters the slice, forwards AND semantics, and forwards
  `transactional()`.
- `detachedExecutor` with the existing `stubPgxPool`/`stubPgxTx`/
  `stubProjectorStore` doubles in `daemon_test.go`: apply is called with a nil
  tx; a checkpoint that moved during the apply is discarded without a write;
  a gap that resolves during the apply yields `retryRequested` and the next
  attempt applies the contiguous set; a serialization failure retries only the
  checkpoint transaction (assert the apply is called exactly once).
- `txExecutor` regression: the existing suite must pass unchanged.

### Integration test matrix

Three blocks: **Preserve** (existing tests that must survive this change),
**New** (must-have for this change, including `T1`–`T3` pulled in from the
pre-existing gaps), and **Pre-existing gaps** (paths that stay untested here).

#### Preserve — existing integration tests

Do not delete, rename, or weaken these while refactoring `daemon.go` and the
harness. They are the regression guard for the attached path.

| ID | Test | Protects |
| --- | --- | --- |
| E1 | `TestProjector_SingleProjectorMultipleProjections` | Assignment, filtered projections advancing to the unscoped frontier |
| E2 | `TestRebalance_ScaleUp_ProjectorsReassignWithoutGapsOrDuplication` | Scale-up handoff without gaps or duplication |
| E3 | `TestRebalance_ScaleDown_ProjectorStops_SurvivorOwnsAllProjections` | Scale-down ownership reclamation |
| E4 | `TestProjectorStartupCleanup_RemovesVeryStaleProjectorRows` | Startup pruning of very stale instances |
| E5 | `TestProjectorStartupCleanup_PreservesRowsNewerThanCleanupThreshold` | Conservative cleanup boundary |
| E6 | `TestDispatcher_WakeupDispatcher_IdleProjectionsWakePromptly` | Wakeup interrupts poll backoff |
| E7 | `TestTransactionalIntegrity_MidBatchFailureRollsBackAndRetriesFromCheckpoint` | Handler error rolls back read model and checkpoint, retries same window |
| E8 | `TestGapHandling_LowerPositionCommitsLateBeforeThreshold_NoSkip` | Gap resolves before threshold, no skip |
| E9 | `TestGapHandling_StaleGapAfterThreshold_AdvancesBySafeHarbor` | Stale skip, audit row fields, explicit harbor lag |
| E10 | `TestGapHandling_StaleGapAfterThreshold_AdvancesWithSparseVisibleWindowUnderDefaultLag` | Safe harbor when the window is narrower than the lag |
| E11 | `TestCheckpointCorrectness_RestartResumesFromPersistedCheckpoint` | Durable checkpoint resume |
| E12 | `TestLeaderFailover_NewLeaderElectedAndRebalancingContinues` | Advisory leader failover |
| E13 | `TestLeaseLeaderFailover_NewLeaderElectedAndRebalancingContinues` | Lease leader failover |
| E14 | `TestLeaseLeader_UncleanCrash_SurvivorTakesOver` | Lease-expiry takeover |
| E15 | `TestLeaseLeader_CascadingDelete_SchemaConstraint` | Lease FK cascade |
| E16 | `TestLeaseLeader_ReleaseLease_Success` | Voluntary lease release |
| E17 | `TestProjector_SplitBrain_OwnershipLost` | Ownership-loss write prevention between batches |
| E18 | `TestLeaderFailover_AdvisoryLock_UncleanCrash` | Advisory lock loss on backend termination |
| E19 | `TestLeaseLeader_HeartbeatRenewalHiccupAndSelfDemotion` | Lease renewal failure and self-demotion |
| E20 | `TestDispatcher_NotifyDispatcher_Reconnection` | Notify reconnect and reconciliation |
| E21 | `TestComprehensiveScaleUpAndDown` | 25 projections, 1→7→1, balance and no loss |
| E22 | `TestIntegration_Observer_FullLifecycle` | Heartbeat, rebalance, batch telemetry |
| E23 | `TestIntegration_Observer_BacklogCatchupLag` | Multi-batch catch-up telemetry |
| E24 | `TestIntegration_Observer_GapDetectionAndStaleSkip` | Gap telemetry |
| E25 | `TestIntegration_Observer_BatchFailureAndRecovery` | Failure telemetry and recovery |

#### New — must-have for this change

Detached path:

| ID | Scenario | Key assertions |
| --- | --- | --- |
| D1 | Detached happy path | Handler applied outside any tx; destination receives all events in order; checkpoint reaches frontier |
| D2 | Connection isolation (core motivation) | `MaxConns=1`, detached handler blocked; heartbeat and a transactional projection make progress; no deadlock or acquire timeout |
| D3 | Destination error | No checkpoint advance; event retried; destination has no partial state |
| D4 | Partial batch failure | Handler applies k of N then errors; checkpoint never passes the applied prefix; retry re-delivers idempotently |
| D5 | Crash between apply and checkpoint | Fail or kill the checkpoint tx; restart re-applies all events, no skips, checkpoint eventually latest |
| D6 | Checkpoint moved during apply | Concurrent session advances checkpoint; daemon write never regresses; no double-apply treated as new work |
| D7 | Gap resolves during apply | Held position commits mid-apply; rule-2 re-check triggers re-probe; previously-gapped event processed in order; zero (or exactly one, past threshold) skip rows |
| D8 | Serialization failure retries checkpoint only | Inject 40001 on the checkpoint tx; handler invocation count per event is exactly 1; destination not re-written |
| D9 | Ownership lost between apply and checkpoint | Reassign during apply; old owner writes no checkpoint; new owner re-applies; eventual correctness |
| D10 | Shutdown with detached apply in flight | Drain within `ShutdownTimeout`; no checkpoint for the incomplete batch; restart re-applies |
| D11 | Shutdown timeout with stuck apply | `Start` returns; checkpoint unchanged; no pooled connection leaked |
| D12 | Detached + filter, zero matches | Checkpoint advances; destination receives nothing |
| D13 | Detached + unresolved gap | Checkpoint cannot advance past the unscoped gap until it resolves or goes stale |
| D14 | Detached + batch bulk apply | One bulk call outside tx; order preserved; checkpoint after success |
| D15 | Detached per-event progress | Success at k, error at k+1 leaves checkpoint at k; restart resumes at k+1 |
| D16 | Detached apply exceeds `BatchTimeout` | Context cancellation aborts the apply; no checkpoint; no connection held |
| D17 | Detached apply does not block rebalance | While a detached handler is blocked, the leader's rebalance transaction commits promptly; no assignment-row lock is held across the apply |

Shapes, registry, batch:

| ID | Scenario | Key assertions |
| --- | --- | --- |
| S1 | Four-shape invocation matrix | Per-event shapes call N times, batch shapes call once; transactional shapes get a live tx, detached shapes do not; checkpoint advances |
| S2 | Batch transactional atomicity | Batch handler error rolls back read model and checkpoint; success sets checkpoint to target |
| S3 | Batch target truncation | Batch receives exactly the rows at or below target, never beyond the safe frontier |
| S4 | Batch with a gap | Batch never receives the missing position; checkpoint respects the frontier |
| S5 | Mixed shapes in one daemon | A blocked detached projection does not stall a transactional projection or the heartbeat |
| S6 | Registry validation | Nil, empty name, duplicate name, unknown shape are rejected before processing starts |
| S7 | `FromProjections` equivalence | Single-shape tx-event daemon behaves identically to today |
| S8 | Deprecated filter wrapper through registry | `FilterStreamTypes(p)` still works when added via `AddProjection` |
| S9 | Window boundary | A full window of exactly `BatchSize` triggers catch-up pacing; no off-by-one in rows or checkpoint |

Filters:

| ID | Scenario | Key assertions |
| --- | --- | --- |
| F1 | Registration `OnStreamTypes` | Equivalent to E1: matching events handled, checkpoint reaches unscoped frontier |
| F2 | Registration `OnEventTypes` | End-to-end coverage that does not exist today |
| F3 | Combined stream + event filter | AND semantics; only the intersection is handled |
| F4 | Two projections, different filters, same events | Each receives its subset; both checkpoints reach latest |
| F5 | Filter + observer accounting | `EventsRead` is the window; `EventsHandled` is the post-filter applied count; skips are not errors |
| F6 | Filter + detached | Only matching events reach the destination; checkpoint advances |

Cross-cutting paths pulled into scope (previously untested):

| ID | Scenario | Key assertions | Failure technique |
| --- | --- | --- | --- |
| T1 | `WithBatchTimeout` cancellation | A handler blocks past the batch timeout; the batch context is canceled, the transaction rolls back, the checkpoint does not advance, and processing resumes on the next batch | Real DB, blocking handler (H1) |
| T2 | Checkpoint row contention | Two sessions process the same projection; the second's `GetCheckpointForUpdate` blocks on the first, then sees the moved checkpoint and returns `blockedByGap` without writing; no double processing | Real DB with two concurrent transactions (H4) |
| T3 | Permanent-hole resume after restart | After a stale-gap skip, restart the daemon; it resumes past the hole, does not re-block on it, and writes no duplicate `projection_gap_skips` row | Real DB (existing `beginControlledAppend` + restart) |

#### Pre-existing test gaps (still out of scope)

These are paths the library does not test today, found while auditing the
existing suite. They are unrelated to the detached/registry work and this change
will not fix them. They are listed so nobody assumes they are covered by the
matrix above.

| ID | Gap |
| --- | --- |
| P1 | No integration test produces a real SQLSTATE 40001 on the normal tx path |
| P2 | Checkpoint monotonicity / `GREATEST` regression guard is untested |
| P3 | `revalidateStaleGapSkip` gap-resolved branch on the tx path (overlaps D7) |
| P4 | Heartbeat loss → fatal `Start` return (unit only today) |
| P5 | `MaxConsecutiveFailures` → `ErrConsecutiveFailures` fatal (unit only today) |
| P6 | Graceful drain of an in-flight transactional batch |
| P7 | `errgroup` fatal propagation and shared-pool multi-daemon setup |
| P8 | Sequential/multiple gaps and skip idempotency across restarts |
| P9 | Schema-qualified table names and the query-exec-mode matrix (PgBouncer) |
| P10 | Backend connection drop during the detached checkpoint transaction (only the advisory-lock drop is covered, by E18) |

### Harness extensions required

- **H1** `testProjection` gating hook: channels to block/release a handler, plus
  a timestamped call log, for D2, D10, D11, D15, D16, S5.
- **H2** Detached test projections implementing `DetachedProjection` and
  `DetachedBatchProjection`, backed by an in-process or `httptest` destination
  with idempotent upsert by event id and version, and injectable per-item
  failure, for D1, D3, D4, D14, F6.
- **H3** Fault-injecting `PgxPool` wrapper: `Daemon.db` is an interface
  (`PgxPool`), so a test can pass an object that forwards every call to a real
  pool but deliberately returns an error for one specific call on demand, for
  example the write inside the short checkpoint transaction. Realism rules for
  this double: it must return the same error shapes a real Postgres produces
  (`*pgconn.PgError` with the relevant SQLSTATE, `pgx.ErrTxClosed`, context
  errors), not a generic `errors.New`; and it must leave the database in the
  state the real failure would leave it in, which means failing the statement
  before the real write happens and letting the real transaction roll back. A
  double that forwards a successful write and only pretends the commit failed
  would corrupt the very state the test asserts. Needed for D5 and D8.
- **H4** Helpers to advance the checkpoint, hold the checkpoint row from a second
  session, and reassign ownership mid-apply, for D6, D9, D13, T2.
- **H5** A pool-sized-to-one harness for D2 and D11.
- **H6** A harness stop variant that surfaces fatal errors, for P4 and P5 when
  those are picked up.

Failure injection is hybrid, and the split is deliberate:

- **Test-double pool (H3)** where the point is the daemon's reaction at a precise
  moment: D5 and D8. Placement can be exact and the test is deterministic.
- **Real database failures** where the point is the database's own behavior:
  T2 (row-lock contention between two sessions) and T3 (restart across a
  permanent hole). These validate the pgx/Postgres boundary, which a double
  cannot validate.
- One small test should generate a real serialization failure (SQLSTATE 40001)
  and assert `isSerializationFailure` accepts it, so the error shape the double
  fabricates is anchored to a real one.

### Subagent verification

An independent audit subagent inventoried the suite and produced its own matrix.
It counted the same 25 existing tests (E1–E25) and proposed the same
detached/shape/filter surface. It contributed these additions, now folded in:
batch target truncation (S3), detached-slow-past-timeout (D16), detached
per-event progress (D15), window boundary (S9), and the harness extensions H1–H6.
On review, three of its findings were moved into scope as `T1`–`T3` (batch
timeout, checkpoint contention, permanent-hole resume); the rest are recorded as
still-out-of-scope gaps `P1`–`P9`.

### Failure taxonomy

Maps each production failure mode to the test that exercises it, so no failure
mode is only assumed.

| Failure mode | Path | Covered by |
| --- | --- | --- |
| Handler returns an error | attached per-event and batch | E7, S2 |
| Destination error before apply | detached | D3 |
| Partial batch apply | detached batch | D4 |
| Checkpoint write fails after a successful apply | detached | D5 (double) |
| Process crash between apply and checkpoint | detached | D5 (restart) |
| Serialization conflict (40001) at commit | tx stale-skip retry, detached checkpoint tx | T2 (real), D8 (double), plus a real-40001 anchor test |
| Checkpoint moved by another worker | attached and detached | T2 (real), D6 |
| Gap resolves inside the decision window | detached | D7 (attached branch stays P3) |
| Ownership lost mid-batch | attached and detached | E17 (between batches), D9 (mid-apply) |
| Rebalance blocked by a held assignment-row lock | attached; must be gone for detached | D17 |
| Shutdown mid-batch | attached and detached | D10, D11 (attached drain stays P6) |
| Batch exceeds `BatchTimeout` | attached and detached | T1, D16 |
| Registry misconfiguration | startup | S6 |
| Backend connection drop during a DB call | attached and detached checkpoint | E18 for advisory locks; detached checkpoint drop stays P10 |

Manual check: confirm `pg_stat_activity` shows no `idle in transaction`
connection from the daemon while a detached handler is blocked.

## Branch and commits

- Branch `feat/detached-projections` (never work on `main`).
- Conventional commits with bodies. Suggested sequence: registry + interfaces +
  appliers; plan/executor split; detached executor + retry signal; observer and
  docs; tests.
