# eventsalsa/projector

[![CI](https://github.com/eventsalsa/projector/actions/workflows/ci.yml/badge.svg)](https://github.com/eventsalsa/projector/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/eventsalsa/projector.svg)](https://pkg.go.dev/github.com/eventsalsa/projector)

`github.com/eventsalsa/projector` is a horizontally scalable, PostgreSQL-native projection processing module for event-sourced systems.

It builds on [`github.com/eventsalsa/store`](https://github.com/eventsalsa/store) and adds daemon coordination, leader election, projection assignment, checkpointing, wakeup dispatching, and batched transactional event processing with no external coordination service.

## Features

- **Daemon orchestrator** for starting, coordinating, and stopping projection goroutines
- **Four projection shapes**: transactional or detached, each per-event or batch, selected by a registry builder
- **Detached projections** for read models outside PostgreSQL, so remote network I/O never holds a pooled connection or the assignment and checkpoint row locks
- **Batch handlers** for destinations with bulk APIs (multi-row upserts, search-engine bulk import)
- **Pluggable leader election**: choose between PostgreSQL session-level advisory locks (`pg_try_advisory_lock`) or a PgBouncer-safe table lease heartbeat strategy
- **Horizontal scaling** through round-robin projection assignment across active projector instances
- **Gap-aware checkpointing**: probe the frontier, handle only safe rows, and audit stale-gap advances
- **Adaptive polling** with exponential backoff and low-latency wakeups
- **Wakeup dispatchers**:
  - polling via periodic latest-position checks
  - PostgreSQL `LISTEN`/`NOTIFY` with reconciliation polling fallback
- **Stream and event filtering** through registration options (`OnStreamTypes`, `OnEventTypes`) or the deprecated decorators (`FilterStreamTypes`, `FilterEventTypes`)
- **Pluggable telemetry and metrics observer** for monitoring projection lag, batch duration, throughput, gap skips, and rebalances with zero extra database queries
- **Migration generation** for projector infrastructure tables
- **Customizable configuration** via the functional options pattern

## How it works

At runtime, each projector daemon instance:

1. Performs a best-effort cleanup of very stale instance registrations, then registers itself in PostgreSQL and updates its heartbeat periodically.
2. Starts a dispatcher that detects newly appended events.
3. Participates in leader election (using advisory locks or database-backed leases).
4. Lets the elected leader rebalance projection assignments across live projector instances.
5. Runs projection goroutines only for the projections assigned to that instance.
6. Probes the global frontier outside the batch transaction, then processes only the current safe frontier inside the batch transaction.
7. For detached projections, applies the safe frontier with no transaction held and persists the checkpoint afterwards in a short transaction.

This design keeps coordination inside PostgreSQL, making the module straightforward to operate in environments that already depend on Postgres.

## Architecture decisions

The module intentionally favors simple, database-native coordination:

- **Single leader, many instances**: only the elected leader recalculates assignments; every projector instance still heartbeats and processes its own assigned projections.
- **Advisory-lock or lease-based leadership**: choose between zero-overhead session advisory locks or PgBouncer transaction pooling-safe database leases.
- **Conservative instance cleanup**: startup may prune `projector_instances` rows only when they are much older than the live-instance timeout, so housekeeping stays less aggressive than rebalance liveness checks.
- **Scoped handling after frontier probe**: projections registered with stream or event filters receive only matching events, but checkpoint correctness comes from an unscoped frontier probe rather than from the last matching filtered row.
- **Broadcast wakeups via close-and-replace channels**: dispatchers notify all waiting projection loops by closing the current wakeup channel and replacing it with a new one.
- **Adaptive polling**: projection loops start at a base poll interval, back off exponentially when idle, stay hot while blocked on known gaps, and reset immediately when new events are found or a wakeup arrives.

## Package layout

```text
.
├── cmd/migrate-gen/         # Stable CLI for generating projector infrastructure migrations
├── daemon.go / config.go    # Projector Daemon and configuration
├── projection.go            # Projection interfaces and filter decorators
├── registry.go              # Projection registry builder and registration filters
├── applier.go               # Handler-shape adapters and the internal filter decorator
├── frontier.go              # Safe frontier and safe harbor calculation
├── dispatcher/              # PollDispatcher and NotifyDispatcher
├── postgres/                # PostgreSQL DAL for registration, leadership, assignment, checkpoints, gap-skip audit
├── migrations/              # SQL migration generator for projector metadata tables
└── integration_test/        # Integration tests against real PostgreSQL
```

## Installation

```bash
go get github.com/eventsalsa/projector
```

The module is intended to be used alongside `github.com/eventsalsa/store` and its PostgreSQL implementation.

## Prerequisites

- Go 1.24+
- PostgreSQL 16+
- [`github.com/eventsalsa/store`](https://github.com/eventsalsa/store)
- `golangci-lint` for local linting
- Docker (optional, for local integration testing)

## Quick start

### 1. Create or open a PostgreSQL database

You need:

- the event store tables from `github.com/eventsalsa/store`
- the projector metadata tables generated from `github.com/eventsalsa/projector/cmd/migrate-gen`

### 2. Build and run the projector daemon

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/eventsalsa/projector"
    "github.com/eventsalsa/store"
    storepostgres "github.com/eventsalsa/store/postgres"
)

type AccountProjection struct{}

func (p *AccountProjection) Name() string {
    return "account_projection"
}

func (p *AccountProjection) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
    _ = ctx
    _ = tx
    _ = event
    return nil
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    connStr := os.Getenv("DATABASE_URL")
    db, err := pgxpool.New(ctx, connStr)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

    registry := projector.NewRegistry()
    if err := registry.Add(&AccountProjection{}, projector.OnStreamTypes("Account")); err != nil {
        log.Fatal(err)
    }

    daemon := projector.New(
        db,
        eventStore,
        registry,
        projector.WithBatchSize(100),
        projector.WithPollInterval(500*time.Millisecond),
    )

    if err := daemon.Start(ctx); err != nil {
        log.Fatal(err)
    }
}
```

> [!NOTE]
> `(*projector.Daemon).Start` blocks until the context is canceled or a fatal runtime error occurs.

## Configuration

Daemons are configured with functional options:

```go
daemon := projector.New(db, eventStore, registry,
    projector.WithBatchSize(100),
    projector.WithPollInterval(500*time.Millisecond),
    projector.WithMaxPollInterval(5*time.Second),
    projector.WithDispatcherInterval(200*time.Millisecond),
    projector.WithHeartbeatInterval(5*time.Second),
    projector.WithHeartbeatTimeout(30*time.Second),
    projector.WithRebalanceInterval(5*time.Second),
    projector.WithBatchPause(200*time.Millisecond),
    projector.WithDispatcherStrategy(projector.DispatcherStrategyNotify),
    projector.WithNotifyConnectionString(connStr),
    projector.WithNotifyChannel("projector_events"),
    projector.WithLeaderStrategy(projector.LeaderStrategyLease),
    projector.WithLogger(myLogger),
)
```

### Available options

| Option | Description | Default |
| --- | --- | --- |
| `WithBatchSize(n int)` | Maximum size of the probed/handled batch window | `100` |
| `WithPollInterval(d time.Duration)` | Base projection poll interval | `1s` |
| `WithMaxPollInterval(d time.Duration)` | Maximum adaptive poll backoff | `30s` |
| `WithDispatcherInterval(d time.Duration)` | Poll dispatcher interval | `200ms` |
| `WithHeartbeatInterval(d time.Duration)` | Instance heartbeat interval | `5s` |
| `WithHeartbeatTimeout(d time.Duration)` | Heartbeat staleness timeout | `30s` |
| `WithRebalanceInterval(d time.Duration)` | Leader rebalance check interval | `5s` |
| `WithBatchPause(d time.Duration)` | Pause between consecutive full batches during catch-up | `200ms` |
| `WithBatchTimeout(d time.Duration)` | Maximum duration for a single batch processing cycle (for detached projections, the external apply) | `30s` |
| `WithShutdownTimeout(d time.Duration)` | Maximum duration to wait for graceful daemon shutdown | `5s` |
| `WithMaxConsecutiveFailures(n int)` | Maximum consecutive unclassified failures before a `FailurePolicyFailFast` projection triggers fatal shutdown | `5` |
| `WithLogger(l store.Logger)` | Custom logger implementation | `store.NoOpLogger{}` |
| `WithProjectorInstancesTable(name string)` | Override instance registration table name | `projector_instances` |
| `WithProjectionAssignmentsTable(name string)` | Override assignment table name | `projection_assignments` |
| `WithProjectionCheckpointsTable(name string)` | Override checkpoint table name | `projection_checkpoints` |
| `WithProjectionGapSkipsTable(name string)` | Override stale-gap audit table name | `projection_gap_skips` |
| `WithProjectorLeaderLeasesTable(name string)` | Override leader election lease table name | `projector_leader_leases` |
| `WithStaleGapThreshold(d time.Duration)` | How long the daemon waits on the same missing position before safe-harbor advancement | `30s` |
| `WithStaleGapHarborLag(n int)` | How far behind the visible head the daemon stays when advancing past a stale gap | `8` |
| `WithDispatcherStrategy(strategy)` | Wakeup strategy: `projector.DispatcherStrategyPoll` or `projector.DispatcherStrategyNotify` | `projector.DispatcherStrategyPoll` |
| `WithNotifyConnectionString(connStr string)` | PostgreSQL connection string used by the notify dispatcher | empty |
| `WithNotifyChannel(channel string)` | PostgreSQL notification channel for the notify dispatcher | empty (`""`) |
| `WithLeaderStrategy(strategy)` | Leader election strategy: `projector.LeaderStrategyAdvisory` or `projector.LeaderStrategyLease` | `projector.LeaderStrategyAdvisory` |
| `WithObserver(observer Observer)` | Telemetry and lifecycle observer | `nil` |

### Leader election strategies

#### Advisory lock strategy (Default)

`projector.LeaderStrategyAdvisory` uses PostgreSQL session-level advisory locks. This strategy is extremely lightweight and releases immediately when a node goes down, but it requires a dedicated connection and is **incompatible** with connection poolers like PgBouncer in transaction pooling mode.

#### Lease-based strategy (PgBouncer-safe)

`projector.LeaderStrategyLease` coordinates leadership through a central lease table (`projector_leader_leases`) using short-lived transactions. The leader periodically heartbeats/renews its lease record. If a leader crashes, other nodes can take over leadership after the lease duration (`HeartbeatTimeout`) has expired. This strategy is fully safe for deployments running behind PgBouncer in transaction pooling mode.

### Dispatcher strategies

#### Poll dispatcher

`projector.DispatcherStrategyPoll` periodically checks the latest global event position and wakes projections when it advances.

Use it when:

- you want the simplest setup
- low-latency wakeups are not critical
- you do not want an extra listener connection

#### Notify dispatcher

`projector.DispatcherStrategyNotify` listens for PostgreSQL notifications and also performs reconciliation polling as a safety net.

Use it when:

- you want lower event-to-projection latency
- your store append path emits PostgreSQL notifications
- you can provide a dedicated notification connection string

When you use notify mode, configure both sides to use the same channel:

- the **store** appends events and emits `NOTIFY`
- the **projector** listens on that channel and wakes assigned projections

Example:

```go
storeConfig := storepostgres.NewStoreConfig(
    storepostgres.WithNotifyChannel("projector_events"),
)

eventStore := storepostgres.NewStore(storeConfig)

daemon := projector.New(
    db,
    eventStore,
    registry,
    projector.WithDispatcherStrategy(projector.DispatcherStrategyNotify),
    projector.WithNotifyConnectionString(connStr),
    projector.WithNotifyChannel("projector_events"),
)
```

## Projection contract

A projection may implement one of four handler shapes. Because Go does not allow two methods with the same name on a type, each type implements exactly one shape and the registry selects its processing path automatically.

```go
// Transactional per-event: the read model is in the daemon's database, so the
// handler writes through tx and the read model and checkpoint commit atomically.
type Projection interface {
    Name() string
    Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error
}

// Detached per-event: the read model is remote. The handler runs with no
// transaction held, then the checkpoint is persisted in a short transaction.
type DetachedProjection interface {
    Name() string
    Handle(ctx context.Context, event store.PersistedEvent) error
}

// Transactional batch: one call per batch, still inside the daemon transaction.
type BatchProjection interface {
    Name() string
    Handle(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) error
}

// Detached batch: one call per batch with no transaction held.
type DetachedBatchProjection interface {
    Name() string
    Handle(ctx context.Context, events []store.PersistedEvent) error
}
```

Register projections with a `Registry`. `Add` infers the shape; the typed `AddProjection`, `AddDetached`, `AddBatch`, and `AddDetachedBatch` methods are explicit alternatives.

```go
registry := projector.NewRegistry()

// Transactional per-event, filtered by stream type.
_ = registry.AddProjection(&AccountProjection{}, projector.OnStreamTypes("Account"))

// Remote search index, applied outside any transaction.
_ = registry.AddDetached(&TypesenseProductProjection{}, projector.OnStreamTypes("Product"))

daemon := projector.New(db, eventStore, registry)
```

For daemons that only use the classic shape, `projector.FromProjections(p1, p2)` builds a registry in one call.

### Registration filters

`OnStreamTypes` and `OnEventTypes` limit a projection to matching events. Non-matching events are skipped without error and the checkpoint still advances to the **unscoped** safe frontier, so a filtered projection that matches nothing in a window does not stall.

```go
_ = registry.AddProjection(myProjection, projector.OnStreamTypes("Account", "Order"))
_ = registry.AddProjection(myProjection, projector.OnEventTypes("AccountCreated", "AccountClosed"))
```

`FilterStreamTypes` and `FilterEventTypes` remain for the classic `Projection` shape and are deprecated in favor of registration options.

### Important projection semantics

- Projection names must be unique across the projector daemon.
- A projection with an empty name is invalid; registration fails.
- Transactional handlers receive a transaction that also owns checkpoint persistence and must **not** call `Commit` or `Rollback` on it.
- If a handler returns an error, the batch fails and the checkpoint is not advanced.
- Filtered events are safely skipped and checkpoints advance normally.
- `BatchProjection` and `DetachedBatchProjection` receive only events at or below the current safe frontier.

### Detached projections and idempotency

Detached projections are applied outside the daemon transaction and checkpointed afterwards, so remote network I/O never holds a pooled connection or the assignment and checkpoint row locks. In exchange, detached delivery is **at-least-once**: events can be applied again

- after a crash between the external apply and the checkpoint write,
- when the checkpoint transaction is retried on a serialization failure,
- when a projection is reassigned during an apply.

Detached handlers must therefore be idempotent. Use a deterministic document id (for example the entity's stream id for state projections) and a monotonic version guard built from `StreamVersion` or `GlobalPosition` so a replayed older event cannot overwrite newer state. Destination error `k` of `N` fails the whole batch; the checkpoint is never advanced past work that was not applied.

The safe-harbor stale-gap behavior applies to detached projections too. Before applying, the daemon revalidates the plan in a short read-only transaction and freezes the applied set and checkpoint target. If the gap closes while the batch is being applied, the checkpoint write is discarded and the batch is re-probed rather than skipping the newly available event.

### Failure policy

Projection failures are classified so the daemon can decide whether to keep going. Classify an error at the failure site with the `failure` package:

```go
import projectorfailure "github.com/eventsalsa/projector/failure"

func (p *ProductProjection) Handle(ctx context.Context, event store.PersistedEvent) error {
    if err := p.client.Upsert(ctx, documentFor(event)); err != nil {
        if isTransient(err) { // network error, rate limit, 5xx
            return projectorfailure.Retryable(err)
        }
        return projectorfailure.Permanent(err)
    }
    return nil
}
```

`Retryable` and `Permanent` wrap the original error, so `errors.Is` and `errors.As` still reach the cause. A classified error always takes precedence over the projection's failure policy.

Unclassified errors follow the projection's `FailurePolicy`, which defaults by shape:

- transactional projections (same-database read model): `FailurePolicyFailFast`, the historical behavior
- detached projections (remote read model): `FailurePolicyRetry`

Override it at registration:

```go
_ = registry.AddDetached(projection, projector.WithFailurePolicy(projector.FailurePolicyRetry))
```

| Situation | Daemon behavior |
| --- | --- |
| `failure.Retryable(err)`, or a context deadline or cancellation | Back off up to `MaxPollInterval`, retry the batch, never fatal, report `OnProjectionDegraded` |
| `failure.Permanent(err)`, no poison handler | Stop that projection and report `OnProjectionDegraded`; the daemon, its other projections, and its leadership keep running |
| `failure.Permanent(err)` with a poison handler returning `nil` | Skip the whole batch, advance the checkpoint past it, report `OnPoisonBatchSkipped`, continue |
| `failure.Permanent(err)` with a poison handler returning an error | Stop that projection |
| Unclassified, `FailurePolicyFailFast` | `ErrConsecutiveFailures` after `MaxConsecutiveFailures`; `Start` returns the error |
| Unclassified, `FailurePolicyRetry` | Same as retryable |

A stopped projection stays stopped for the lifetime of the daemon; fix the cause and restart.

#### Poison handler and dead letters

```go
_ = registry.AddDetached(projection, projector.WithPoisonHandler(
    func(ctx context.Context, projectionName string, events []store.PersistedEvent, cause error) error {
        return myDLQ.Record(ctx, projectionName, events, cause)
    },
))
```

The contract is whole-batch: the handler receives every event in the failing batch and the daemon skips all of them, so a batch that contains one bad document sends its good events to the handler too. For a transactional projection nothing was applied before the skip, because the transaction rolled back. For a detached projection the handler may already have applied some events before the failure; record the whole batch and rely on the at-least-once idempotency rules above. There is no audit table: the handler is the durable record, alongside the `OnPoisonBatchSkipped` signal and a log line.

#### Degradation signals

- `OnProjectionDegraded` fires for every non-fatal failure, with the projection name, the time of the first failure in the current run, the consecutive failure count, and the last error.
- `OnPoisonBatchSkipped` fires after a poison batch is recorded and skipped.

### Decorators

Filter decorators allow filtering events before they reach your handler without complicating the handler itself:

```go
// Deprecated: prefer registration options on the registry.
p := projector.FilterStreamTypes(myProjection, "Account", "Order")
p = projector.FilterEventTypes(myProjection, "AccountCreated", "AccountClosed")
```

These wrappers only support the classic `Projection` shape. New code should use `OnStreamTypes` and `OnEventTypes` at registration, which work for detached and batch handlers too.

### Checkpoint semantics

Projection checkpoints track the **highest safe global position** the daemon has advanced to, not the last matching filtered event handled by that projection.

That means:

- filtered projections can handle zero events in a batch while the checkpoint still advances
- later matching events never define the checkpoint target by themselves
- stale-gap advances are durably recorded in `projection_gap_skips`

## Processing model

For each assigned projection, the daemon repeatedly:

1. loads the current checkpoint
2. performs an **unscoped frontier probe** outside the batch transaction
3. computes the current safe frontier from that probe
4. waits on unresolved gaps until they either resolve or become stale
5. when a gap is stale, advances conservatively to a safe harbor behind the current visible head, or to the earliest reachable visible frontier when the probe window is smaller than the configured lag
6. opens the batch transaction and calls `Handle` only for rows at or below the target frontier
7. saves the checkpoint target, records any stale-gap skip, and commits the transaction
8. for detached projections, applies the frozen rows with no transaction held and then writes the checkpoint, and any stale-gap audit record, in a short transaction

That means read-model updates performed through `tx`, checkpoint moves, and stale-gap audit records stay atomic with one another for transactional projections. Detached projections give that up in exchange for never holding a pooled connection or the assignment and checkpoint row locks during network I/O; they are at-least-once and require idempotent handlers.

### Stale-gap behavior

`global_position` values are sequence-backed, so a lower position can appear later than a higher committed position. The daemon therefore:

- refuses to checkpoint past a visible gap immediately
- keeps polling the same gap for up to `StaleGapThreshold`
- if the gap stays unresolved, advances conservatively to a safe harbor behind the current visible head, falling back to the earliest reachable visible frontier when the probe window is smaller than the configured lag
- records that decision in `projection_gap_skips` so operators can inspect it later

If a stale-gap decision later proves too aggressive for a projection, the recovery path is to rewind or rebuild from a safe checkpoint.

## Migration generation

For the quickest path, use the stable `cmd/migrate-gen` entrypoint.

```bash
go run github.com/eventsalsa/projector/cmd/migrate-gen \
  -output ./db/migrations \
  -filename 002_projector_tables.sql
```

The CLI defaults match `migrations.DefaultConfig()`, and you can override table names to line up with `projector.With*Table(...)` options:

```bash
go run github.com/eventsalsa/projector/cmd/migrate-gen \
  -output ./db/migrations \
  -projector-instances-table infra.projector_instances \
  -projection-assignments-table infra.projection_assignments \
  -projection-checkpoints-table infra.projection_checkpoints \
  -projection-gap-skips-table infra.projection_gap_skips \
  -projector-leader-leases-table infra.projector_leader_leases
```

For more advanced integration, use the `migrations` package directly from your own program:

```go
package main

import (
    "log"

    "github.com/eventsalsa/projector/migrations"
)

func main() {
    config := migrations.DefaultConfig()
    config.OutputFolder = "./db/migrations"
    config.OutputFilename = "002_projector_tables.sql"
    config.ProjectorInstancesTable = "infra.projector_instances"
    config.ProjectionAssignmentsTable = "infra.projection_assignments"
    config.ProjectionCheckpointsTable = "infra.projection_checkpoints"
    config.ProjectionGapSkipsTable = "infra.projection_gap_skips"
    config.ProjectorLeaderLeasesTable = "infra.projector_leader_leases"

    if err := migrations.GeneratePostgres(&config); err != nil {
        log.Fatal(err)
    }
}
```

The generated migration creates:

- `projector_instances`
- `projection_assignments`
- `projection_checkpoints`
- `projection_gap_skips`
- `projector_leader_leases`

It also creates schemas automatically when a configured table name includes a schema prefix.

## Running multiple projector instances

To scale horizontally, start multiple instances of the same projector daemon configuration against the same PostgreSQL database.

Each instance will:

- register itself with a unique instance ID
- heartbeat into `projector_instances`
- observe leader election
- receive a subset of projection assignments
- stop processing projections that are reassigned elsewhere

This makes scaling operationally simple: add more projector daemon processes and let PostgreSQL-backed assignment rebalancing distribute the projections.

On startup, an instance may also prune `projector_instances` rows whose heartbeats are older than twice the configured heartbeat timeout. That cleanup is best-effort and intentionally more conservative than rebalance liveness detection. If an instance ever loses its own registration row later, it shuts down instead of continuing to run invisibly.

### Running multiple domain daemons in a single process (`errgroup`)

When building modular monoliths or multi-context services, applications often run multiple independent `Daemon` instances within a single OS process (e.g. separate domain stores like `ledger.events`, `billing.events`, each with dedicated projections, checkpoints, and leases).

The `*projector.Daemon` is designed to be a seamless citizen in [`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup) workflows:

- **Clean shutdown**: `daemon.Start(ctx)` returns `nil` when `ctx` is canceled (both during steady-state processing and during startup registration), preventing canceled contexts from polluting `errgroup.Wait()`.
- **Fatal failure propagation**: Fatal faults (e.g. `ErrConsecutiveFailures`) return non-nil errors, immediately triggering `errgroup` context cancellation for sibling daemons.
- **Graceful batch draining**: When `ctx` is canceled, in-flight batches are granted up to `WithShutdownTimeout(d)` (default 5s) to commit before forced cancellation.
- **State inspection**: `daemon.IsRunning()` and `daemon.IsLeader()` allow HTTP readiness and health probes (`/healthz`, `/readyz`) to query status without out-of-band queries.

#### Example: Multi-Daemon worker process

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"
    "time"

    "golang.org/x/sync/errgroup"

    "github.com/eventsalsa/projector"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    // 1. Initialize domain daemons with lease leader election
    ledgerDaemon := projector.New(db, ledgerStore, projector.FromProjections(ledgerProjections...),
        projector.WithLeaderStrategy(projector.LeaderStrategyLease),
        projector.WithProjectorLeaderLeasesTable("ledger.projector_leader_leases"),
        projector.WithProjectorInstancesTable("ledger.projector_instances"),
        projector.WithProjectionAssignmentsTable("ledger.projection_assignments"),
        projector.WithProjectionCheckpointsTable("ledger.projection_checkpoints"),
        projector.WithShutdownTimeout(15*time.Second),
    )

    billingDaemon := projector.New(db, billingStore, projector.FromProjections(billingProjections...),
        projector.WithLeaderStrategy(projector.LeaderStrategyLease),
        projector.WithProjectorLeaderLeasesTable("billing.projector_leader_leases"),
        projector.WithProjectorInstancesTable("billing.projector_instances"),
        projector.WithProjectionAssignmentsTable("billing.projection_assignments"),
        projector.WithProjectionCheckpointsTable("billing.projection_checkpoints"),
        projector.WithShutdownTimeout(15*time.Second),
    )

    daemons := []*projector.Daemon{ledgerDaemon, billingDaemon}

    // 2. Coordinate concurrent execution via errgroup
    g, gCtx := errgroup.WithContext(ctx)
    for _, d := range daemons {
        g.Go(func() error {
            return d.Start(gCtx)
        })
    }

    if err := g.Wait(); err != nil {
        log.Fatalf("projector worker exited with fatal error: %v", err)
    }
}
```

#### Operational considerations for multi-daemon processes

1. **Connection Pool Sizing:** Ensure your shared `pgxpool.Pool` has sufficient connections for all concurrent daemons:
   $$\text{PoolSize}_{\text{min}} \ge N_{\text{daemons}} \times (1 + \text{MaxConcurrentProjections}) + \text{Headroom}$$

   Detached projections do not hold a pooled connection while applying to their read model, so they count toward `MaxConcurrentProjections` only for their short probe and checkpoint transactions. Transactional projections hold one connection for the whole handler loop, and the advisory leader strategy pins one additional connection per daemon.
2. **Lease Tables vs Advisory Locks:** Multi-daemon deployments sharing a PostgreSQL database should use `projector.LeaderStrategyLease` with domain-specific lease tables (e.g. `schema.projector_leader_leases`). `projector.LeaderStrategyAdvisory` uses a single well-known lock key across the database, which would cause unrelated domain daemons to contend for leadership.
3. **Distinct NOTIFY Channels:** If using `projector.DispatcherStrategyNotify`, assign each domain daemon a unique channel via `projector.WithNotifyChannel("domain_events")` to prevent wakeup crosstalk.

## Observability and telemetry

The projector daemon exposes internal runtime events and metrics via a pluggable `Observer` interface without executing any extra out-of-band database polling queries:

```go
type Observer interface {
    OnBatchProcessed(ctx context.Context, stats BatchStats)
    OnHeartbeat(ctx context.Context, stats DaemonStats)
    OnGapDetected(ctx context.Context, stats GapStats)
    OnGapSkipped(ctx context.Context, stats GapStats)
    OnProjectionDegraded(ctx context.Context, stats DegradedStats)
    OnPoisonBatchSkipped(ctx context.Context, stats PoisonBatchStats)
    OnRebalance(ctx context.Context, assignments map[string]uuid.UUID)
}
```

### Telemetry data structures

- **`BatchStats`**: contains batch execution latency (`Duration`), global checkpoint positions (`StartPosition`, `LastPosition`, `HeadPosition`), calculated event lag (`Lag = max(0, HeadPosition - LastPosition)`), throughput counts (`EventsRead` for the window and `EventsHandled` for events applied after registration filtering), a safe-harbor stale skip indicator (`StaleSkipped`), a `Detached` flag, and error status (`Error`).
- **`DaemonStats`**: contains instance UUID (`InstanceID`) and leadership status (`IsLeader`).
- **`GapStats`**: contains missing sequence coordinate (`GapPosition`), visible stream head (`HighestVisible`), and elapsed duration (`StaleFor`).
- **`DegradedStats`**: contains the projection name, the time of the first failure in the current run (`FirstFailureAt`), the consecutive failure count, and the last error, reported for failures that do not stop the daemon.
- **`PoisonBatchStats`**: contains the projection name, the checkpoint target the batch was skipped to, the event count, and the cause, reported after a poison handler records a batch.

### Convenience utilities

- **`NoopObserver`**: empty struct implementing all `Observer` methods. Embed it into your struct to implement only the callbacks you need.
- **`MultiObserver(observers ...Observer) Observer`**: combines multiple observers (e.g. Prometheus + OpenTelemetry + structured logger) into a single composite listener, automatically stripping `nil` entries and flattening nested multi-observers.

### Prometheus metrics recipe

```go
package main

import (
    "context"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"

    "github.com/eventsalsa/projector"
)

type PrometheusObserver struct {
    projector.NoopObserver
    batchDuration *prometheus.HistogramVec
    lagGauge      *prometheus.GaugeVec
    eventsHandled *prometheus.CounterVec
    checkpointPos *prometheus.GaugeVec
}

func NewPrometheusObserver() *PrometheusObserver {
    return &PrometheusObserver{
        batchDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
            Name: "projector_batch_duration_seconds",
            Help: "Duration of projection batch processing in seconds.",
        }, []string{"projection", "status"}),
        lagGauge: promauto.NewGaugeVec(prometheus.GaugeOpts{
            Name: "projector_lag_events",
            Help: "Current projection lag in global events behind event store head.",
        }, []string{"projection"}),
        checkpointPos: promauto.NewGaugeVec(prometheus.GaugeOpts{
            Name: "projector_checkpoint_position",
            Help: "Current global checkpoint position reached by projection.",
        }, []string{"projection"}),
        eventsHandled: promauto.NewCounterVec(prometheus.CounterOpts{
            Name: "projector_events_handled_total",
            Help: "Total number of events handled by projection.",
        }, []string{"projection"}),
    }
}

func (p *PrometheusObserver) OnBatchProcessed(_ context.Context, stats projector.BatchStats) {
    status := "success"
    if stats.Error != nil {
        status = "error"
    }

    p.batchDuration.WithLabelValues(stats.ProjectionName, status).Observe(stats.Duration.Seconds())
    p.lagGauge.WithLabelValues(stats.ProjectionName).Set(float64(stats.Lag))
    p.checkpointPos.WithLabelValues(stats.ProjectionName).Set(float64(stats.LastPosition))
    p.eventsHandled.WithLabelValues(stats.ProjectionName).Add(float64(stats.EventsHandled))
}
```

See [`examples/telemetry/main.go`](examples/telemetry/main.go) for a complete runnable sample.

## Development

### Clone and set up

```bash
git clone https://github.com/eventsalsa/projector.git
cd projector
go mod download
```

### Common commands

```bash
make build
make test
make lint
make test-integration
```

### Make targets

| Target | Description |
| --- | --- |
| `make help` | Show available targets |
| `make test` | Run the default test suite |
| `make test-unit` | Run unit tests with race detection and coverage |
| `make test-integration` | Run integration tests (automatically manages PostgreSQL via testcontainers-go) |
| `make test-integration-local` | Alias for `test-integration` |
| `make lint` | Run `golangci-lint` |
| `make fmt` | Run `gofmt` and `goimports` |
| `make build` | Build all packages |
| `make check` | Run all checks (lint, unit tests, integration tests) |

### Integration testing

Integration tests automatically provision and manage ephemeral PostgreSQL instances using `testcontainers-go` (requires Docker running locally).

To run against a pre-existing external PostgreSQL instance instead of using testcontainers, disable testcontainers and supply the connection variables:

```bash
TESTCONTAINERS=false \
POSTGRES_HOST=localhost \
POSTGRES_PORT=5432 \
POSTGRES_USER=postgres \
POSTGRES_PASSWORD=postgres \
POSTGRES_DB=eventsalsa_projector_test \
make test-integration
```

## Notes

- The projector module coordinates projections; it does not replace the event store.
- The projector depends on PostgreSQL for both persistence and coordination.
- For most applications, start with the poll dispatcher and move to notify mode when lower wakeup latency is needed.
