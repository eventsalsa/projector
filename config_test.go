package projector

import (
	"context"
	"testing"
	"time"

	"github.com/eventsalsa/store"

	"github.com/eventsalsa/projector/postgres"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.BatchSize != 100 {
		t.Fatalf("BatchSize = %d, want 100", config.BatchSize)
	}
	if config.PollInterval != time.Second {
		t.Fatalf("PollInterval = %v, want %v", config.PollInterval, time.Second)
	}
	if config.MaxPollInterval != 30*time.Second {
		t.Fatalf("MaxPollInterval = %v, want %v", config.MaxPollInterval, 30*time.Second)
	}
	if config.DispatcherInterval != 200*time.Millisecond {
		t.Fatalf("DispatcherInterval = %v, want %v", config.DispatcherInterval, 200*time.Millisecond)
	}
	if config.HeartbeatInterval != 5*time.Second {
		t.Fatalf("HeartbeatInterval = %v, want %v", config.HeartbeatInterval, 5*time.Second)
	}
	if config.HeartbeatTimeout != 30*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want %v", config.HeartbeatTimeout, 30*time.Second)
	}
	if config.RebalanceInterval != 5*time.Second {
		t.Fatalf("RebalanceInterval = %v, want %v", config.RebalanceInterval, 5*time.Second)
	}
	if config.BatchPause != 200*time.Millisecond {
		t.Fatalf("BatchPause = %v, want %v", config.BatchPause, 200*time.Millisecond)
	}
	if config.BatchTimeout != 30*time.Second {
		t.Fatalf("BatchTimeout = %v, want %v", config.BatchTimeout, 30*time.Second)
	}
	if config.DetachedApplyTimeout != 30*time.Second {
		t.Fatalf("DetachedApplyTimeout = %v, want %v", config.DetachedApplyTimeout, 30*time.Second)
	}
	if config.CheckpointTimeout != 5*time.Second {
		t.Fatalf("CheckpointTimeout = %v, want %v", config.CheckpointTimeout, 5*time.Second)
	}
	if config.ShutdownTimeout != 5*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want %v", config.ShutdownTimeout, 5*time.Second)
	}
	if config.StaleGapThreshold != 30*time.Second {
		t.Fatalf("StaleGapThreshold = %v, want %v", config.StaleGapThreshold, 30*time.Second)
	}
	if config.StaleGapHarborLag != 8 {
		t.Fatalf("StaleGapHarborLag = %d, want 8", config.StaleGapHarborLag)
	}
	if config.MaxConsecutiveFailures != 5 {
		t.Fatalf("MaxConsecutiveFailures = %d, want 5", config.MaxConsecutiveFailures)
	}
	if config.ProjectorInstancesTable != postgres.DefaultProjectorInstancesTable {
		t.Fatalf("ProjectorInstancesTable = %q, want %q", config.ProjectorInstancesTable, postgres.DefaultProjectorInstancesTable)
	}
	if config.ProjectionAssignmentsTable != postgres.DefaultProjectionAssignmentsTable {
		t.Fatalf("ProjectionAssignmentsTable = %q, want %q", config.ProjectionAssignmentsTable, postgres.DefaultProjectionAssignmentsTable)
	}
	if config.ProjectionCheckpointsTable != postgres.DefaultProjectionCheckpointsTable {
		t.Fatalf("ProjectionCheckpointsTable = %q, want %q", config.ProjectionCheckpointsTable, postgres.DefaultProjectionCheckpointsTable)
	}
	if config.ProjectionGapSkipsTable != postgres.DefaultProjectionGapSkipsTable {
		t.Fatalf("ProjectionGapSkipsTable = %q, want %q", config.ProjectionGapSkipsTable, postgres.DefaultProjectionGapSkipsTable)
	}
	if config.ProjectorLeaderLeasesTable != postgres.DefaultProjectorLeaderLeasesTable {
		t.Fatalf("ProjectorLeaderLeasesTable = %q, want %q", config.ProjectorLeaderLeasesTable, postgres.DefaultProjectorLeaderLeasesTable)
	}
	if config.NotifyChannel != "" {
		t.Fatalf("NotifyChannel = %q, want empty", config.NotifyChannel)
	}
	if _, ok := config.Logger.(store.NoOpLogger); !ok {
		t.Fatalf("Logger = %T, want store.NoOpLogger", config.Logger)
	}
	if config.Observer != nil {
		t.Fatalf("Observer = %v, want nil", config.Observer)
	}
}

func TestApplyOptionsComposesMultipleOptions(t *testing.T) {
	logger := configTestLogger{}

	config := applyOptions(
		WithBatchSize(17),
		WithPollInterval(2*time.Second),
		WithMaxPollInterval(9*time.Second),
		WithDispatcherInterval(250*time.Millisecond),
		WithHeartbeatInterval(6*time.Second),
		WithHeartbeatTimeout(45*time.Second),
		WithRebalanceInterval(7*time.Second),
		WithBatchPause(300*time.Millisecond),
		WithBatchTimeout(15*time.Second),
		WithDetachedApplyTimeout(11*time.Second),
		WithCheckpointTimeout(3*time.Second),
		WithShutdownTimeout(12*time.Second),
		WithStaleGapThreshold(45*time.Second),
		WithStaleGapHarborLag(5),
		WithProjectorInstancesTable("custom_instances"),
		WithProjectionAssignmentsTable("custom_assignments"),
		WithProjectionCheckpointsTable("custom_checkpoints"),
		WithProjectionGapSkipsTable("custom_gap_skips"),
		WithProjectorLeaderLeasesTable("custom_leases"),
		WithLogger(logger),
		WithObserver(&recordingObserver{}),
	)

	if config.BatchSize != 17 {
		t.Fatalf("BatchSize = %d, want 17", config.BatchSize)
	}
	if config.PollInterval != 2*time.Second {
		t.Fatalf("PollInterval = %v, want %v", config.PollInterval, 2*time.Second)
	}
	if config.MaxPollInterval != 9*time.Second {
		t.Fatalf("MaxPollInterval = %v, want %v", config.MaxPollInterval, 9*time.Second)
	}
	if config.DispatcherInterval != 250*time.Millisecond {
		t.Fatalf("DispatcherInterval = %v, want %v", config.DispatcherInterval, 250*time.Millisecond)
	}
	if config.HeartbeatInterval != 6*time.Second {
		t.Fatalf("HeartbeatInterval = %v, want %v", config.HeartbeatInterval, 6*time.Second)
	}
	if config.HeartbeatTimeout != 45*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want %v", config.HeartbeatTimeout, 45*time.Second)
	}
	if config.RebalanceInterval != 7*time.Second {
		t.Fatalf("RebalanceInterval = %v, want %v", config.RebalanceInterval, 7*time.Second)
	}
	if config.BatchPause != 300*time.Millisecond {
		t.Fatalf("BatchPause = %v, want %v", config.BatchPause, 300*time.Millisecond)
	}
	if config.BatchTimeout != 15*time.Second {
		t.Fatalf("BatchTimeout = %v, want %v", config.BatchTimeout, 15*time.Second)
	}
	if config.DetachedApplyTimeout != 11*time.Second {
		t.Fatalf("DetachedApplyTimeout = %v, want %v", config.DetachedApplyTimeout, 11*time.Second)
	}
	if config.CheckpointTimeout != 3*time.Second {
		t.Fatalf("CheckpointTimeout = %v, want %v", config.CheckpointTimeout, 3*time.Second)
	}
	if config.ShutdownTimeout != 12*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want %v", config.ShutdownTimeout, 12*time.Second)
	}
	if config.StaleGapThreshold != 45*time.Second {
		t.Fatalf("StaleGapThreshold = %v, want %v", config.StaleGapThreshold, 45*time.Second)
	}
	if config.StaleGapHarborLag != 5 {
		t.Fatalf("StaleGapHarborLag = %d, want 5", config.StaleGapHarborLag)
	}
	if config.ProjectorInstancesTable != "custom_instances" {
		t.Fatalf("ProjectorInstancesTable = %q, want %q", config.ProjectorInstancesTable, "custom_instances")
	}
	if config.ProjectionAssignmentsTable != "custom_assignments" {
		t.Fatalf("ProjectionAssignmentsTable = %q, want %q", config.ProjectionAssignmentsTable, "custom_assignments")
	}
	if config.ProjectionCheckpointsTable != "custom_checkpoints" {
		t.Fatalf("ProjectionCheckpointsTable = %q, want %q", config.ProjectionCheckpointsTable, "custom_checkpoints")
	}
	if config.ProjectionGapSkipsTable != "custom_gap_skips" {
		t.Fatalf("ProjectionGapSkipsTable = %q, want %q", config.ProjectionGapSkipsTable, "custom_gap_skips")
	}
	if config.ProjectorLeaderLeasesTable != "custom_leases" {
		t.Fatalf("ProjectorLeaderLeasesTable = %q, want %q", config.ProjectorLeaderLeasesTable, "custom_leases")
	}
	if config.Logger != logger {
		t.Fatalf("Logger = %v, want %v", config.Logger, logger)
	}
	if _, ok := config.Observer.(*recordingObserver); !ok {
		t.Fatalf("Observer = %T, want *recordingObserver", config.Observer)
	}
}

func TestOptionFunctions(t *testing.T) {
	logger := store.NoOpLogger{}
	obs := &recordingObserver{}
	config := Config{}

	WithBatchSize(17)(&config)
	WithPollInterval(2 * time.Second)(&config)
	WithMaxPollInterval(9 * time.Second)(&config)
	WithDispatcherInterval(250 * time.Millisecond)(&config)
	WithHeartbeatInterval(6 * time.Second)(&config)
	WithHeartbeatTimeout(45 * time.Second)(&config)
	WithRebalanceInterval(7 * time.Second)(&config)
	WithBatchPause(300 * time.Millisecond)(&config)
	WithBatchTimeout(20 * time.Second)(&config)
	WithDetachedApplyTimeout(14 * time.Second)(&config)
	WithCheckpointTimeout(4 * time.Second)(&config)
	WithShutdownTimeout(8 * time.Second)(&config)
	WithStaleGapThreshold(45 * time.Second)(&config)
	WithStaleGapHarborLag(5)(&config)
	WithProjectorInstancesTable("custom_instances")(&config)
	WithProjectionAssignmentsTable("custom_assignments")(&config)
	WithProjectionCheckpointsTable("custom_checkpoints")(&config)
	WithProjectionGapSkipsTable("custom_gap_skips")(&config)
	WithProjectorLeaderLeasesTable("custom_leases")(&config)
	WithLogger(logger)(&config)
	WithObserver(obs)(&config)

	if config.BatchSize != 17 {
		t.Fatalf("BatchSize = %d, want 17", config.BatchSize)
	}
	if config.PollInterval != 2*time.Second {
		t.Fatalf("PollInterval = %v, want %v", config.PollInterval, 2*time.Second)
	}
	if config.MaxPollInterval != 9*time.Second {
		t.Fatalf("MaxPollInterval = %v, want %v", config.MaxPollInterval, 9*time.Second)
	}
	if config.DispatcherInterval != 250*time.Millisecond {
		t.Fatalf("DispatcherInterval = %v, want %v", config.DispatcherInterval, 250*time.Millisecond)
	}
	if config.HeartbeatInterval != 6*time.Second {
		t.Fatalf("HeartbeatInterval = %v, want %v", config.HeartbeatInterval, 6*time.Second)
	}
	if config.HeartbeatTimeout != 45*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want %v", config.HeartbeatTimeout, 45*time.Second)
	}
	if config.RebalanceInterval != 7*time.Second {
		t.Fatalf("RebalanceInterval = %v, want %v", config.RebalanceInterval, 7*time.Second)
	}
	if config.BatchPause != 300*time.Millisecond {
		t.Fatalf("BatchPause = %v, want %v", config.BatchPause, 300*time.Millisecond)
	}
	if config.BatchTimeout != 20*time.Second {
		t.Fatalf("BatchTimeout = %v, want %v", config.BatchTimeout, 20*time.Second)
	}
	if config.DetachedApplyTimeout != 14*time.Second {
		t.Fatalf("DetachedApplyTimeout = %v, want %v", config.DetachedApplyTimeout, 14*time.Second)
	}
	if config.CheckpointTimeout != 4*time.Second {
		t.Fatalf("CheckpointTimeout = %v, want %v", config.CheckpointTimeout, 4*time.Second)
	}
	if config.ShutdownTimeout != 8*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want %v", config.ShutdownTimeout, 8*time.Second)
	}
	if config.StaleGapThreshold != 45*time.Second {
		t.Fatalf("StaleGapThreshold = %v, want %v", config.StaleGapThreshold, 45*time.Second)
	}
	if config.StaleGapHarborLag != 5 {
		t.Fatalf("StaleGapHarborLag = %d, want 5", config.StaleGapHarborLag)
	}
	if config.ProjectorInstancesTable != "custom_instances" {
		t.Fatalf("ProjectorInstancesTable = %q, want %q", config.ProjectorInstancesTable, "custom_instances")
	}
	if config.ProjectionAssignmentsTable != "custom_assignments" {
		t.Fatalf("ProjectionAssignmentsTable = %q, want %q", config.ProjectionAssignmentsTable, "custom_assignments")
	}
	if config.ProjectionCheckpointsTable != "custom_checkpoints" {
		t.Fatalf("ProjectionCheckpointsTable = %q, want %q", config.ProjectionCheckpointsTable, "custom_checkpoints")
	}
	if config.ProjectionGapSkipsTable != "custom_gap_skips" {
		t.Fatalf("ProjectionGapSkipsTable = %q, want %q", config.ProjectionGapSkipsTable, "custom_gap_skips")
	}
	if config.ProjectorLeaderLeasesTable != "custom_leases" {
		t.Fatalf("ProjectorLeaderLeasesTable = %q, want %q", config.ProjectorLeaderLeasesTable, "custom_leases")
	}
	if config.Logger != logger {
		t.Fatalf("Logger = %v, want %v", config.Logger, logger)
	}
	if config.Observer != obs {
		t.Fatalf("Observer = %v, want %v", config.Observer, obs)
	}
}

func TestApplyOptionsSkipsNilOptions(t *testing.T) {
	defaults := DefaultConfig()
	nilOption := Option(nil)

	config := applyOptions(nilOption, WithBatchSize(23))

	if config.BatchSize != 23 {
		t.Fatalf("BatchSize = %d, want 23", config.BatchSize)
	}
	if config.PollInterval != defaults.PollInterval {
		t.Fatalf("PollInterval = %v, want default %v", config.PollInterval, defaults.PollInterval)
	}
}

func TestApplyOptionsNormalizesInvalidValues(t *testing.T) {
	defaults := DefaultConfig()
	customLogger := &configTestLogger{}

	config := applyOptions(
		WithBatchSize(0),
		WithPollInterval(5*time.Second),
		WithMaxPollInterval(time.Second),
		WithDispatcherInterval(0),
		WithHeartbeatInterval(0),
		WithHeartbeatTimeout(0),
		WithRebalanceInterval(0),
		WithBatchPause(-time.Second),
		WithBatchTimeout(0),
		WithDetachedApplyTimeout(0),
		WithCheckpointTimeout(0),
		WithShutdownTimeout(0),
		WithStaleGapThreshold(0),
		WithStaleGapHarborLag(-1),
		WithProjectorInstancesTable(""),
		WithProjectionAssignmentsTable(""),
		WithProjectionCheckpointsTable(""),
		WithProjectionGapSkipsTable(""),
		WithProjectorLeaderLeasesTable(""),
		WithLogger(nil),
		WithObserver(nil),
		nil,
	)

	if config.Observer != nil {
		t.Fatalf("Observer = %v, want nil", config.Observer)
	}

	if config.BatchSize != defaults.BatchSize {
		t.Fatalf("BatchSize = %d, want default %d", config.BatchSize, defaults.BatchSize)
	}
	if config.PollInterval != 5*time.Second {
		t.Fatalf("PollInterval = %v, want %v", config.PollInterval, 5*time.Second)
	}
	if config.MaxPollInterval != defaults.MaxPollInterval {
		t.Fatalf("MaxPollInterval = %v, want default %v", config.MaxPollInterval, defaults.MaxPollInterval)
	}
	if config.DispatcherInterval != defaults.DispatcherInterval {
		t.Fatalf("DispatcherInterval = %v, want default %v", config.DispatcherInterval, defaults.DispatcherInterval)
	}
	if config.HeartbeatInterval != defaults.HeartbeatInterval {
		t.Fatalf("HeartbeatInterval = %v, want default %v", config.HeartbeatInterval, defaults.HeartbeatInterval)
	}
	if config.HeartbeatTimeout != defaults.HeartbeatTimeout {
		t.Fatalf("HeartbeatTimeout = %v, want default %v", config.HeartbeatTimeout, defaults.HeartbeatTimeout)
	}
	if config.RebalanceInterval != defaults.RebalanceInterval {
		t.Fatalf("RebalanceInterval = %v, want default %v", config.RebalanceInterval, defaults.RebalanceInterval)
	}
	if config.BatchPause != defaults.BatchPause {
		t.Fatalf("BatchPause = %v, want default %v", config.BatchPause, defaults.BatchPause)
	}
	if config.BatchTimeout != defaults.BatchTimeout {
		t.Fatalf("BatchTimeout = %v, want default %v", config.BatchTimeout, defaults.BatchTimeout)
	}
	if config.DetachedApplyTimeout != defaults.DetachedApplyTimeout {
		t.Fatalf("DetachedApplyTimeout = %v, want default %v", config.DetachedApplyTimeout, defaults.DetachedApplyTimeout)
	}
	if config.CheckpointTimeout != defaults.CheckpointTimeout {
		t.Fatalf("CheckpointTimeout = %v, want default %v", config.CheckpointTimeout, defaults.CheckpointTimeout)
	}
	if config.ShutdownTimeout != defaults.ShutdownTimeout {
		t.Fatalf("ShutdownTimeout = %v, want default %v", config.ShutdownTimeout, defaults.ShutdownTimeout)
	}
	if config.StaleGapHarborLag != defaults.StaleGapHarborLag {
		t.Fatalf("StaleGapHarborLag = %d, want default %d", config.StaleGapHarborLag, defaults.StaleGapHarborLag)
	}
	if config.ProjectorInstancesTable != defaults.ProjectorInstancesTable {
		t.Fatalf("ProjectorInstancesTable = %q, want default %q", config.ProjectorInstancesTable, defaults.ProjectorInstancesTable)
	}
	if config.ProjectionAssignmentsTable != defaults.ProjectionAssignmentsTable {
		t.Fatalf("ProjectionAssignmentsTable = %q, want default %q", config.ProjectionAssignmentsTable, defaults.ProjectionAssignmentsTable)
	}
	if config.ProjectionCheckpointsTable != defaults.ProjectionCheckpointsTable {
		t.Fatalf("ProjectionCheckpointsTable = %q, want default %q", config.ProjectionCheckpointsTable, defaults.ProjectionCheckpointsTable)
	}
	if config.ProjectionGapSkipsTable != defaults.ProjectionGapSkipsTable {
		t.Fatalf("ProjectionGapSkipsTable = %q, want default %q", config.ProjectionGapSkipsTable, defaults.ProjectionGapSkipsTable)
	}
	if config.ProjectorLeaderLeasesTable != defaults.ProjectorLeaderLeasesTable {
		t.Fatalf("ProjectorLeaderLeasesTable = %q, want default %q", config.ProjectorLeaderLeasesTable, defaults.ProjectorLeaderLeasesTable)
	}
	if _, ok := config.Logger.(store.NoOpLogger); !ok {
		t.Fatalf("Logger = %T, want store.NoOpLogger", config.Logger)
	}

	config = applyOptions(WithLogger(customLogger))
	if config.Logger != customLogger {
		t.Fatalf("Logger = %v, want %v", config.Logger, customLogger)
	}
}

func TestApplyOptionsDetachedTimeoutsKeepIndependentDefaults(t *testing.T) {
	defaults := DefaultConfig()
	config := applyOptions(WithBatchTimeout(2 * time.Second))

	if config.DetachedApplyTimeout != defaults.DetachedApplyTimeout {
		t.Fatalf("DetachedApplyTimeout = %v, want default %v", config.DetachedApplyTimeout, defaults.DetachedApplyTimeout)
	}
	if config.CheckpointTimeout != defaults.CheckpointTimeout {
		t.Fatalf("CheckpointTimeout = %v, want default %v", config.CheckpointTimeout, defaults.CheckpointTimeout)
	}

	config = applyOptions(
		WithBatchTimeout(2*time.Second),
		WithDetachedApplyTimeout(500*time.Millisecond),
		WithCheckpointTimeout(750*time.Millisecond),
	)

	if config.DetachedApplyTimeout != 500*time.Millisecond {
		t.Fatalf("DetachedApplyTimeout = %v, want %v", config.DetachedApplyTimeout, 500*time.Millisecond)
	}
	if config.CheckpointTimeout != 750*time.Millisecond {
		t.Fatalf("CheckpointTimeout = %v, want %v", config.CheckpointTimeout, 750*time.Millisecond)
	}
}

func TestApplyOptionsCapsStaleGapHarborLagToBatchWindow(t *testing.T) {
	config := applyOptions(
		WithBatchSize(5),
		WithStaleGapHarborLag(99),
	)

	if config.StaleGapHarborLag != 4 {
		t.Fatalf("StaleGapHarborLag = %d, want 4", config.StaleGapHarborLag)
	}
}

func TestApplyOptionsAllowsZeroStaleGapHarborLag(t *testing.T) {
	config := applyOptions(WithStaleGapHarborLag(0))

	if config.StaleGapHarborLag != 0 {
		t.Fatalf("StaleGapHarborLag = %d, want 0", config.StaleGapHarborLag)
	}
}

type configTestLogger struct{}

func (configTestLogger) Debug(_ context.Context, _ string, _ ...interface{}) {}
func (configTestLogger) Info(_ context.Context, _ string, _ ...interface{})  {}
func (configTestLogger) Error(_ context.Context, _ string, _ ...interface{}) {}
