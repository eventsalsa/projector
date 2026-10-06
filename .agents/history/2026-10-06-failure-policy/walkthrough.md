# Error classification and per-projection failure policy

## Summary

Repeated batch failures used to be unclassified and ultimately daemon-fatal:
after `MaxConsecutiveFailures` (default 5) the daemon returned
`ErrConsecutiveFailures`, which under `errgroup` also cancels sibling daemons.
That is the right default for a same-database projection, but it is wrong for a
detached projection whose read model is a network dependency.

This change lets handlers classify failures and gives each projection a failure
policy, so a flaky remote read model backs off and reports degradation instead of
taking down the process, its siblings, and its leadership.

## What changed

### Error classification (`failure` package)

A new `github.com/eventsalsa/projector/failure` package wraps an error with a
classification that survives `%w` wrapping:

```go
return failure.Retryable(err) // transient: network, rate limit, 5xx
return failure.Permanent(err) // retrying will not help: schema mismatch, bad document
```

`failure.IsRetryable` and `failure.IsPermanent` inspect the chain with
`errors.As`; the outermost classification wins. Causes are preserved, so
`errors.Is(err, myErr)` still works.

### Per-projection failure policy

`FailurePolicy` has two values and is set per registration with
`WithFailurePolicy`:

- `FailurePolicyFailFast` — the historical behavior, fatal after
  `MaxConsecutiveFailures`.
- `FailurePolicyRetry` — back off up to `MaxPollInterval` and retry forever,
  never fatal, reporting degradation.

Defaults by shape: transactional appliers get `FailFast`, detached appliers get
`Retry`. A classified error always wins over the policy.

`RegistrationOption` was generalized from `func(*filterSpec)` to
`func(*registrationConfig)` so options can carry the policy and the poison
handler as well as filters. The parameter type is unexported, so nothing
downstream can have depended on its shape.

### Permanent failures and the poison path

A `Permanent` error stops the affected projection by default, without touching
the daemon or its siblings. Before this change, `syncAssignments` would have
restarted the projection on the next tick, so a `failedProjections` marker on the
daemon keeps it stopped for the lifetime of the process.

With `WithPoisonHandler`, a `Permanent` failure hands the whole failing batch to
a user callback instead:

- callback returns `nil` — the batch is skipped, the checkpoint advances past it
  in a short transaction, `OnPoisonBatchSkipped` fires, and the projection
  continues.
- callback returns an error — the projection stops.

The contract is whole-batch and there is no audit table; the handler is the
durable record. See the README for the details.

### Observer

`Observer` gained `OnProjectionDegraded` and `OnPoisonBatchSkipped`, with
`DegradedStats` and `PoisonBatchStats`, implemented on `NoopObserver` and
`MultiObserver`.

## Behavior

| Situation | Behavior |
| --- | --- |
| `failure.Retryable(err)`, context deadline or cancellation | Capped backoff, retried forever, never fatal, `OnProjectionDegraded` |
| `failure.Permanent(err)`, no poison handler | Stop that projection; daemon and siblings alive |
| `failure.Permanent(err)`, poison handler returns nil | Skip the batch, advance the checkpoint, `OnPoisonBatchSkipped`, continue |
| `failure.Permanent(err)`, poison handler returns error | Stop that projection |
| Unclassified, transactional (default `FailFast`) | `ErrConsecutiveFailures` after the threshold; unchanged |
| Unclassified, detached (default `Retry`) | Same as retryable |
| Ownership lost | Stop the projection; unchanged |

## Tests

Unit tests (165 total, 10 new): the `failure` package, including wrapping and
outermost-wins; default policy per shape; explicit policy override; poison
handler storage; `skipPoisonBatch` advancing the checkpoint and its no-op path;
and `batchError` carrying the batch through wrapping.

Integration tests (66 total, 3 new):

- a retryable failure on a `FailFast` projection does not kill the daemon, emits
  `OnProjectionDegraded`, and recovers once the destination heals
- a `Permanent` projection stops while a sibling keeps processing and the daemon
  stays up, and it is not restarted
- a poison handler receives the whole batch, `OnPoisonBatchSkipped` fires, the
  checkpoint advances past the batch, and the projection continues

## Verification

```bash
rtk make check
go test -p 1 -race -tags=integration -timeout=300s ./integration_test/...
```

Lint clean, unit tests green, integration tests green under the race detector.

## Out of scope

A circuit breaker for sustained retryable failures is tracked separately in #21.
