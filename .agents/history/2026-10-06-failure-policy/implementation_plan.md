# Projection failure policy

## Context

PR #19 is squash-merged into `main` (commit `f17cd1a`). It introduced the four
handler shapes, the `Registry`, the detached execution path, and per-registration
options.

Today a projection that fails a batch gets no classification at all:

- `runProjection` counts consecutive batch failures and, at
  `MaxConsecutiveFailures` (default 5), calls
  `reportFatal(ErrConsecutiveFailures)`. `Start` then returns that error, which
  is daemon-fatal: under `errgroup` it cancels sibling daemons too
  (`daemon.go:48`, `daemon.go:703`-`770`).
- `applyOptions` normalizes `MaxConsecutiveFailures <= 0` to the default
  (`daemon.go:1767`), so `WithMaxConsecutiveFailures(0)` cannot disable it today.

This is the right default for a same-database projection, where repeated failure
usually means a bug. It is wrong for a detached projection whose read model is a
network dependency: a brief outage of a search index should not kill the daemon,
its siblings, or its leader duties.

Issue #21 tracks a circuit breaker as a later enhancement; that is out of scope.

## Goals

- Let handlers classify failures: transient versus permanent.
- Give unclassified failures a per-projection policy with shape-derived defaults.
- Remote or transient failures back off and degrade visibly; they never kill the
  daemon.
- Permanent failures stop only the affected projection by default, with an
  opt-in poison handler for a user-supplied dead-letter path.
- Keep existing behavior available for same-database projections.

## Naming

`projector.Permanent(err)` does not read like a factory. Recommended: a small
dedicated package.

```go
// import "github.com/eventsalsa/projector/failure"
func Retryable(err error) error
func Permanent(err error) error
func IsRetryable(err error) bool
func IsPermanent(err error) bool
```

Usage reads as a constructor at the failure site:

```go
if err := client.Upsert(ctx, doc); err != nil {
    if isTransient(err) {
        return failure.Retryable(err)
    }
    return failure.Permanent(err)
}
```

Rationale: it keeps the root package focused, gives the classification helpers a
home, and avoids a bare adjective in the main API. Alternative if a package is
unwanted: root-level `projector.MarkRetryable(err)` / `projector.MarkPermanent(err)`,
which read as actions. I recommend the package.

Use typed wrappers with `Unwrap`, not sentinel values, so the cause is preserved.
The daemon detects them with `errors.As` through the existing `%w` wrapping in the
appliers.

## Public API

Root package:

```go
type FailurePolicy int

const (
    // FailurePolicyFailFast keeps today's behavior: after MaxConsecutiveFailures
    // unclassified failures the daemon stops with ErrConsecutiveFailures.
    FailurePolicyFailFast FailurePolicy = iota
    // FailurePolicyRetry backs off and retries the same batch forever. It is
    // never fatal and reports degradation through the observer.
    FailurePolicyRetry
)

// PoisonHandler is called when a projection returns a Permanent error and a
// poison handler is configured. It receives the whole failing batch and the
// cause. Returning nil tells the daemon the batch was recorded and may be
// skipped; returning an error stops the projection.
type PoisonHandler func(ctx context.Context, projectionName string, events []store.PersistedEvent, cause error) error

func WithFailurePolicy(policy FailurePolicy) RegistrationOption
func WithPoisonHandler(handler PoisonHandler) RegistrationOption
```

Defaults by shape, unless overridden: transactional appliers get
`FailurePolicyFailFast`, detached appliers get `FailurePolicyRetry`. The sentinel
always wins over the policy.

Observer additions:

```go
type DegradedStats struct {
    ProjectionName      string
    ConsecutiveFailures int
    LastError           error
    Since               time.Time
}

type PoisonBatchStats struct {
    ProjectionName  string
    TargetPosition  int64
    EventCount      int
    Cause           error
}

OnProjectionDegraded(ctx context.Context, stats DegradedStats)
OnPoisonBatchSkipped(ctx context.Context, stats PoisonBatchStats)
```

Add both to `Observer`, `NoopObserver`, and `multiObserver`.

## Classification and the loop

Decide in `runProjection`'s error branch, in this order:

1. `errors.Is(err, errProjectionOwnershipLost)` — unchanged: stop the projection.
2. `failure.IsRetryable(err)`, or `errors.Is(err, context.DeadlineExceeded)`, or
   `errors.Is(err, context.Canceled)` — retry: grow the poll interval with
   `nextPollInterval` up to `MaxPollInterval`, emit `OnProjectionDegraded`, never
   fatal, keep the projection running. This also covers a detached apply that
   exceeds `BatchTimeout`.
3. `failure.IsPermanent(err)` — poison path, see below.
4. Otherwise — the projection's policy: `FailFast` keeps today's counter and
   `ErrConsecutiveFailures`; `Retry` behaves like case 2.

Daemon-level errors are not routed through handler classification. Serialization
failures keep their existing retry. Database and pool errors during the probe or
checkpoint are not handler errors and should not be maskable by a sentinel.

### Poison path

The executor knows the batch that failed; `runProjection` currently only has the
error. Carry the batch with it: introduce an internal error type, for example

```go
type batchError struct {
    cause  error
    plan   *batchPlan
    target int64
}
```

with `Error()` and `Unwrap()`, returned by the executors in place of a bare error.
`runProjection` recovers it with `errors.As` and passes `plan.rows` and the cause
to the poison handler.

- No handler configured: stop the projection. Do not call `reportFatal`; the
  daemon and its siblings stay alive. Log and emit an observer signal.
- Handler returns `nil`: record the skip (`OnPoisonBatchSkipped` plus a log),
  then advance the checkpoint to the batch target in a short transaction
  (ownership check + `SaveCheckpoint`), and continue the loop.
- Handler returns an error: stop the projection, log the handler error.

Contract to document, since this is data not applied to the read model:

- Granularity is the whole batch. The handler receives every event in the failing
  batch and the daemon skips all of them, so good events in that batch go to the
  dead-letter path as well. This is deliberate and must be stated in the README
  and in the `PoisonHandler` doc comment.
- Transactional: the transaction rolled back, so nothing was applied before the
  skip.
- Detached: a partial apply may already have happened before the failure. The
  handler should record the whole batch; the at-least-once contract covers the
  duplicate side.
- The skip is observable and logged. There is no audit table; the user's handler
  is the record. (Observer plus log only, as agreed.)

## Plugin plumbing

`RegistrationOption` is currently `func(*filterSpec)` (`registry.go:167`), so it
can only express filters. Generalize it to a registration config that holds the
filter spec, the failure policy, and the poison handler:

```go
type registrationConfig struct {
    filterSpec
    policy FailurePolicy
    poison PoisonHandler
    policySet bool
}

type RegistrationOption func(*registrationConfig)
```

`OnStreamTypes` and `OnEventTypes` are unchanged from the caller's view. The
parameter type is unexported today, so no external code can define its own
options and nothing downstream breaks.

`registration` gains the resolved `policy` and `poison`. Resolve the default from
`applier.transactional()` at registration time when the caller did not set one.

## Files

- `failure/failure.go` (new) and `failure/failure_test.go` (new): wrappers,
  predicates, and their unit tests.
- `registry.go`: generalize `RegistrationOption`; add `WithFailurePolicy` and
  `WithPoisonHandler`; resolve and store the policy and poison handler.
- `daemon.go`: `batchError`; wrap executor failures with it; rework the
  `runProjection` error branch; add `skipPoisonBatch`; grow the backoff on
  retryable failures; keep `ErrConsecutiveFailures` for `FailFast` only.
- `observer.go`: `DegradedStats`, `PoisonBatchStats`, the two new interface
  methods, and the `NoopObserver`/`multiObserver` implementations.
- `README.md`: a failure-policy section covering the sentinel, the policy
  defaults, the poison handler with a dead-letter example, and the whole-batch
  skip contract.
- `.agents/history/<date>-failure-policy/`: plan and walkthrough.

## Behavior summary to document

| Situation | Behavior |
| --- | --- |
| `failure.Retryable(err)` | Capped backoff, retried forever, never fatal, `OnProjectionDegraded` |
| Context deadline or cancel | Same as retryable |
| `failure.Permanent(err)`, no poison handler | Stop that projection, daemon and siblings alive |
| `failure.Permanent(err)` with a poison handler returning nil | Skip the batch, advance the checkpoint past the target, `OnPoisonBatchSkipped`, continue |
| `failure.Permanent(err)` with a poison handler returning an error | Stop that projection |
| Unclassified, transactional (default `FailFast`) | `ErrConsecutiveFailures` after the threshold; unchanged |
| Unclassified, detached (default `Retry`) | Same as retryable |
| Ownership lost | Stop the projection; unchanged |

## Verification

- `rtk make check`, plus `go test -p 1 -race -tags=integration ./integration_test/...`.
- Unit tests for the `failure` package; classification priority (retryable beats
  policy, permanent beats policy, wrapped errors survive `%w`); default policy per
  shape; poison handler nil versus error; checkpoint advance on skip; backoff
  growth.
- Integration tests: a detached projection against a permanently failing
  destination stays up, reports `OnProjectionDegraded`, and never returns
  `ErrConsecutiveFailures`; a `Permanent` projection stops while the daemon keeps
  serving another projection; a poison handler records the batch and the
  checkpoint advances past it; a transactional `FailFast` projection still returns
  `ErrConsecutiveFailures` (existing `TestGap_MaxConsecutiveFailuresIsFatal`).
- Existing tests that pass `WithMaxConsecutiveFailures(0)` still pass, since
  detached now defaults to `Retry`. Update the README table entry to say the
  threshold applies only to `FailFast` unclassified failures.

## Decisions to confirm

1. Naming: the `failure` subpackage with `Retryable`/`Permanent` (recommended), or
   root-level `MarkRetryable`/`MarkPermanent`.
2. `FailFast` semantics: keep the daemon-fatal `ErrConsecutiveFailures` for
   backward compatibility (recommended, since `Permanent` already covers "stop
   this projection"), or change `FailFast` to stop only the projection and report
   through the observer.
3. Database or pool errors with an unclassified status: leave them on the
   projection's policy (recommended), or always classify infrastructure errors as
   retryable.
