package projector

import (
	"time"

	"github.com/eventsalsa/store"

	"github.com/eventsalsa/projector/postgres"
)

// DispatcherStrategy selects how the daemon wakeup dispatcher detects new events.
type DispatcherStrategy string

const (
	// DispatcherStrategyPoll uses periodic polling to detect new events.
	DispatcherStrategyPoll DispatcherStrategy = "poll"
	// DispatcherStrategyNotify uses PostgreSQL LISTEN/NOTIFY plus reconciliation polling.
	DispatcherStrategyNotify DispatcherStrategy = "notify"
)

// LeaderStrategy selects how the daemon coordinates leadership.
type LeaderStrategy string

const (
	// LeaderStrategyAdvisory uses PostgreSQL session-level advisory locks.
	LeaderStrategyAdvisory LeaderStrategy = "advisory"
	// LeaderStrategyLease uses a lease table with heartbeat updates (PgBouncer-safe).
	LeaderStrategyLease LeaderStrategy = "lease"
)

// Config holds all configurable values for a Daemon.
type Config struct { //nolint:govet // fieldalignment: readability over marginal memory savings
	Logger                     store.Logger
	ProjectorInstancesTable    string
	ProjectionAssignmentsTable string
	ProjectionCheckpointsTable string
	ProjectionGapSkipsTable    string
	LeaderStrategy             LeaderStrategy
	ProjectorLeaderLeasesTable string
	DispatcherStrategy         DispatcherStrategy
	NotifyConnectionString     string
	NotifyChannel              string
	BatchSize                  int
	MaxConsecutiveFailures     int
	PollInterval               time.Duration
	MaxPollInterval            time.Duration
	DispatcherInterval         time.Duration
	HeartbeatInterval          time.Duration
	HeartbeatTimeout           time.Duration
	RebalanceInterval          time.Duration
	BatchPause                 time.Duration
	BatchTimeout               time.Duration
	DetachedApplyTimeout       time.Duration
	CheckpointTimeout          time.Duration
	ShutdownTimeout            time.Duration
	StaleGapThreshold          time.Duration
	StaleGapHarborLag          int
	Observer                   Observer
}

// Option configures a Daemon.
type Option func(*Config)

// DefaultConfig returns sensible defaults for daemon processing, coordination,
// and observability.
func DefaultConfig() Config {
	return Config{
		BatchSize:                  100,
		MaxConsecutiveFailures:     5,
		PollInterval:               time.Second,
		MaxPollInterval:            30 * time.Second,
		DispatcherInterval:         200 * time.Millisecond,
		HeartbeatInterval:          5 * time.Second,
		HeartbeatTimeout:           30 * time.Second,
		RebalanceInterval:          5 * time.Second,
		BatchPause:                 200 * time.Millisecond,
		BatchTimeout:               30 * time.Second,
		DetachedApplyTimeout:       30 * time.Second,
		CheckpointTimeout:          5 * time.Second,
		ShutdownTimeout:            5 * time.Second,
		StaleGapThreshold:          30 * time.Second,
		Logger:                     store.NoOpLogger{},
		ProjectorInstancesTable:    postgres.DefaultProjectorInstancesTable,
		ProjectionAssignmentsTable: postgres.DefaultProjectionAssignmentsTable,
		ProjectionCheckpointsTable: postgres.DefaultProjectionCheckpointsTable,
		ProjectionGapSkipsTable:    postgres.DefaultProjectionGapSkipsTable,
		LeaderStrategy:             LeaderStrategyAdvisory,
		ProjectorLeaderLeasesTable: postgres.DefaultProjectorLeaderLeasesTable,
		DispatcherStrategy:         DispatcherStrategyPoll,
		NotifyChannel:              "",
		StaleGapHarborLag:          8,
	}
}

// WithBatchSize sets the maximum number of events processed per batch.
func WithBatchSize(n int) Option {
	return func(c *Config) {
		c.BatchSize = n
	}
}

// WithPollInterval sets the base interval between projection polls.
func WithPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.PollInterval = d
	}
}

// WithMaxPollInterval sets the maximum interval used by adaptive polling
// backoff.
func WithMaxPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.MaxPollInterval = d
	}
}

// WithDispatcherInterval sets the interval used by the wakeup dispatcher.
func WithDispatcherInterval(d time.Duration) Option {
	return func(c *Config) {
		c.DispatcherInterval = d
	}
}

// WithHeartbeatInterval sets how often an instance refreshes its heartbeat.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *Config) {
		c.HeartbeatInterval = d
	}
}

// WithHeartbeatTimeout sets the maximum age of a heartbeat before an instance is
// considered dead.
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.HeartbeatTimeout = d
	}
}

// WithRebalanceInterval sets how often the leader checks whether projection
// assignments need to be recalculated.
func WithRebalanceInterval(d time.Duration) Option {
	return func(c *Config) {
		c.RebalanceInterval = d
	}
}

// WithBatchPause sets the pause between consecutive catch-up batches.
func WithBatchPause(d time.Duration) Option {
	return func(c *Config) {
		c.BatchPause = d
	}
}

// WithBatchTimeout sets the maximum duration for a single batch processing cycle.
// If a batch (including DB operations and event handling) exceeds this duration,
// the context is canceled and the batch is rolled back.
//
// For detached projections it bounds the frontier probe and the external apply.
// The post-apply checkpoint phase has its own budget (WithCheckpointTimeout), so a
// detached batch cycle can take up to BatchTimeout plus CheckpointTimeout.
func WithBatchTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.BatchTimeout = d
	}
}

// WithDetachedApplyTimeout sets the maximum duration of the external apply for a
// detached projection. It defaults to the same 30s as BatchTimeout, and the
// effective budget is min(BatchTimeout, DetachedApplyTimeout). Lower it to fail
// a slow destination sooner than the batch deadline.
//
// Handlers must return promptly when the apply context is done; a handler that
// ignores the context cannot be interrupted.
func WithDetachedApplyTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.DetachedApplyTimeout = d
	}
}

// WithCheckpointTimeout sets the maximum duration of the checkpoint phase that
// follows a detached apply: the checkpoint transaction and its ownership check.
// The budget starts when that phase starts and is independent of BatchTimeout,
// so a slow apply cannot starve the checkpoint write.
func WithCheckpointTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.CheckpointTimeout = d
	}
}

// WithShutdownTimeout sets the maximum duration to wait for graceful daemon shutdown.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.ShutdownTimeout = d
	}
}

// WithStaleGapThreshold sets how long the daemon waits on the same missing
// global position before applying safe-harbor advancement.
func WithStaleGapThreshold(d time.Duration) Option {
	return func(c *Config) {
		c.StaleGapThreshold = d
	}
}

// WithStaleGapHarborLag sets how far behind the visible head the daemon stays
// when advancing past a stale gap.
func WithStaleGapHarborLag(n int) Option {
	return func(c *Config) {
		c.StaleGapHarborLag = n
	}
}

// WithMaxConsecutiveFailures sets how many consecutive batch failures a projection
// tolerates before the daemon triggers a fatal shutdown. This prevents the daemon
// from appearing healthy (heartbeat alive) while projections make no progress.
func WithMaxConsecutiveFailures(n int) Option {
	return func(c *Config) {
		c.MaxConsecutiveFailures = n
	}
}

// WithLogger sets the logger used by the daemon.
func WithLogger(l store.Logger) Option {
	return func(c *Config) {
		c.Logger = l
	}
}

// WithProjectorInstancesTable sets the projector instance registration table name.
func WithProjectorInstancesTable(name string) Option {
	return func(c *Config) {
		c.ProjectorInstancesTable = name
	}
}

// WithProjectionAssignmentsTable sets the projection assignment table name.
func WithProjectionAssignmentsTable(name string) Option {
	return func(c *Config) {
		c.ProjectionAssignmentsTable = name
	}
}

// WithProjectionCheckpointsTable sets the projection checkpoint table name.
func WithProjectionCheckpointsTable(name string) Option {
	return func(c *Config) {
		c.ProjectionCheckpointsTable = name
	}
}

// WithProjectionGapSkipsTable sets the projection gap skip audit table name.
func WithProjectionGapSkipsTable(name string) Option {
	return func(c *Config) {
		c.ProjectionGapSkipsTable = name
	}
}

// WithDispatcherStrategy sets the daemon wakeup dispatcher strategy.
func WithDispatcherStrategy(strategy DispatcherStrategy) Option {
	return func(c *Config) {
		c.DispatcherStrategy = strategy
	}
}

// WithNotifyConnectionString sets the PostgreSQL connection string used by the
// LISTEN/NOTIFY dispatcher.
func WithNotifyConnectionString(connStr string) Option {
	return func(c *Config) {
		c.NotifyConnectionString = connStr
	}
}

// WithNotifyChannel sets the PostgreSQL notification channel used by the notify dispatcher.
func WithNotifyChannel(channel string) Option {
	return func(c *Config) {
		c.NotifyChannel = channel
	}
}

// WithLeaderStrategy sets the leader election strategy.
func WithLeaderStrategy(strategy LeaderStrategy) Option {
	return func(c *Config) {
		c.LeaderStrategy = strategy
	}
}

// WithProjectorLeaderLeasesTable sets the table name used for lease-based leader election.
func WithProjectorLeaderLeasesTable(name string) Option {
	return func(c *Config) {
		c.ProjectorLeaderLeasesTable = name
	}
}

// WithObserver sets the telemetry and lifecycle observer for the daemon.
func WithObserver(observer Observer) Option {
	return func(c *Config) {
		c.Observer = observer
	}
}
