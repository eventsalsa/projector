package projector

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// BatchStats contains execution telemetry for a single projection batch.
type BatchStats struct { //nolint:govet // fieldalignment: readability over marginal memory savings
	ProjectionName string
	StartPosition  int64         // Checkpoint position prior to batch execution
	LastPosition   int64         // Checkpoint position after batch execution
	HeadPosition   int64         // Highest visible/known global position
	Lag            int64         // Global event lag: max(0, HeadPosition - LastPosition)
	EventsRead     int           // Total events read in the batch window
	EventsHandled  int           // Events applied by the projection after registration filtering
	Duration       time.Duration // Total batch duration (fetch + handle + checkpoint commit)
	StaleSkipped   bool          // True if safe-harbor advanced past an unresolvable stale gap
	Detached       bool          // True if the projection was applied outside the daemon transaction
	Error          error         // Non-nil if batch execution failed
}

// DaemonStats contains instance-level lifecycle telemetry.
type DaemonStats struct {
	InstanceID uuid.UUID
	IsLeader   bool
}

// GapStats contains details when a projection encounters a missing sequence position.
type GapStats struct {
	ProjectionName string
	GapPosition    int64
	HighestVisible int64
	StaleFor       time.Duration
}

// Observer receives real-time telemetry and lifecycle events from the projector daemon.
// All implementations must be safe for concurrent execution across multiple goroutines.
type Observer interface {
	// OnBatchProcessed is called after every batch processing attempt (success or error).
	//nolint:gocritic // hugeParam: pass by value for immutable snapshot
	OnBatchProcessed(ctx context.Context, stats BatchStats)

	// OnHeartbeat is called after each successful heartbeat refresh.
	OnHeartbeat(ctx context.Context, stats DaemonStats)

	// OnGapDetected is called when a projection is blocked by a missing global sequence position.
	OnGapDetected(ctx context.Context, stats GapStats)

	// OnGapSkipped is called when safe-harbor advancement skips an unresolvable stale gap.
	OnGapSkipped(ctx context.Context, stats GapStats)

	// OnRebalance is called when projection partition assignments are updated.
	OnRebalance(ctx context.Context, assignments map[string]uuid.UUID)
}

// NoopObserver provides empty implementations of all Observer methods.
// Embed NoopObserver into custom structs to implement only the desired callbacks.
type NoopObserver struct{}

// OnBatchProcessed is a no-op implementation.
//
//nolint:gocritic // hugeParam: pass by value for immutable snapshot
func (NoopObserver) OnBatchProcessed(context.Context, BatchStats) {}

// OnHeartbeat is a no-op implementation.
func (NoopObserver) OnHeartbeat(context.Context, DaemonStats) {}

// OnGapDetected is a no-op implementation.
func (NoopObserver) OnGapDetected(context.Context, GapStats) {}

// OnGapSkipped is a no-op implementation.
func (NoopObserver) OnGapSkipped(context.Context, GapStats) {}

// OnRebalance is a no-op implementation.
func (NoopObserver) OnRebalance(context.Context, map[string]uuid.UUID) {}

type multiObserver []Observer

// MultiObserver combines multiple Observers into a single composite Observer.
// Nil observers are stripped and nested MultiObservers are flattened.
// If no non-nil observers are provided, a NoopObserver is returned.
func MultiObserver(observers ...Observer) Observer {
	flattened := make([]Observer, 0, len(observers))
	for _, o := range observers {
		if o == nil {
			continue
		}
		if mo, ok := o.(multiObserver); ok {
			flattened = append(flattened, mo...)
			continue
		}
		flattened = append(flattened, o)
	}

	if len(flattened) == 0 {
		return NoopObserver{}
	}
	if len(flattened) == 1 {
		return flattened[0]
	}

	return multiObserver(flattened)
}

//nolint:gocritic // hugeParam: pass by value for immutable snapshot
func (mo multiObserver) OnBatchProcessed(ctx context.Context, stats BatchStats) {
	for _, o := range mo {
		o.OnBatchProcessed(ctx, stats)
	}
}

func (mo multiObserver) OnHeartbeat(ctx context.Context, stats DaemonStats) {
	for _, o := range mo {
		o.OnHeartbeat(ctx, stats)
	}
}

func (mo multiObserver) OnGapDetected(ctx context.Context, stats GapStats) {
	for _, o := range mo {
		o.OnGapDetected(ctx, stats)
	}
}

func (mo multiObserver) OnGapSkipped(ctx context.Context, stats GapStats) {
	for _, o := range mo {
		o.OnGapSkipped(ctx, stats)
	}
}

func (mo multiObserver) OnRebalance(ctx context.Context, assignments map[string]uuid.UUID) {
	for _, o := range mo {
		o.OnRebalance(ctx, assignments)
	}
}
