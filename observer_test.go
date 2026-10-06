package projector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type recordingObserver struct {
	mu         sync.Mutex
	batches    []BatchStats
	heartbeats []DaemonStats
	gaps       []GapStats
	skipped    []GapStats
	degraded   []DegradedStats
	poison     []PoisonBatchStats
	rebalances []map[string]uuid.UUID
}

//nolint:gocritic // hugeParam: test mock implementation
func (r *recordingObserver) OnBatchProcessed(_ context.Context, stats BatchStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, stats)
}

func (r *recordingObserver) OnHeartbeat(_ context.Context, stats DaemonStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heartbeats = append(r.heartbeats, stats)
}

func (r *recordingObserver) OnGapDetected(_ context.Context, stats GapStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaps = append(r.gaps, stats)
}

func (r *recordingObserver) OnGapSkipped(_ context.Context, stats GapStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.skipped = append(r.skipped, stats)
}

func (r *recordingObserver) OnProjectionDegraded(_ context.Context, stats DegradedStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.degraded = append(r.degraded, stats)
}

func (r *recordingObserver) OnPoisonBatchSkipped(_ context.Context, stats PoisonBatchStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.poison = append(r.poison, stats)
}

func (r *recordingObserver) OnRebalance(_ context.Context, assignments map[string]uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make(map[string]uuid.UUID, len(assignments))
	for k, v := range assignments {
		copied[k] = v
	}
	r.rebalances = append(r.rebalances, copied)
}

func (r *recordingObserver) Batches() []BatchStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]BatchStats(nil), r.batches...)
}

func (r *recordingObserver) Heartbeats() []DaemonStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]DaemonStats(nil), r.heartbeats...)
}

func (r *recordingObserver) Gaps() []GapStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]GapStats(nil), r.gaps...)
}

func (r *recordingObserver) Skipped() []GapStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]GapStats(nil), r.skipped...)
}

func (r *recordingObserver) Degraded() []DegradedStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]DegradedStats(nil), r.degraded...)
}

func (r *recordingObserver) PoisonSkipped() []PoisonBatchStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]PoisonBatchStats(nil), r.poison...)
}

func (r *recordingObserver) Rebalances() []map[string]uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make([]map[string]uuid.UUID, len(r.rebalances))
	for i, reb := range r.rebalances {
		m := make(map[string]uuid.UUID, len(reb))
		for k, v := range reb {
			m[k] = v
		}
		copied[i] = m
	}
	return copied
}

type partialCustomObserver struct {
	NoopObserver
	batches []BatchStats
}

//nolint:gocritic // hugeParam: test mock implementation
func (p *partialCustomObserver) OnBatchProcessed(_ context.Context, stats BatchStats) {
	p.batches = append(p.batches, stats)
}

func TestNoopObserver(_ *testing.T) {
	ctx := context.Background()
	noop := NoopObserver{}

	// Calling all methods must not panic
	noop.OnBatchProcessed(ctx, BatchStats{
		ProjectionName: "test",
		StartPosition:  0,
		LastPosition:   10,
		HeadPosition:   10,
		Lag:            0,
		EventsRead:     10,
		EventsHandled:  10,
		Duration:       time.Millisecond,
		StaleSkipped:   false,
		Error:          errors.New("err"),
	})
	noop.OnHeartbeat(ctx, DaemonStats{
		InstanceID: uuid.New(),
		IsLeader:   true,
	})
	noop.OnGapDetected(ctx, GapStats{
		ProjectionName: "test",
		GapPosition:    5,
		HighestVisible: 10,
		StaleFor:       time.Second,
	})
	noop.OnGapSkipped(ctx, GapStats{
		ProjectionName: "test",
		GapPosition:    5,
		HighestVisible: 10,
		StaleFor:       time.Second,
	})
	noop.OnRebalance(ctx, map[string]uuid.UUID{"test": uuid.New()})
}

func TestEmbeddedNoopObserver(t *testing.T) {
	ctx := context.Background()
	custom := &partialCustomObserver{}

	// Custom implements Observer through embedding
	var observer Observer = custom

	observer.OnBatchProcessed(ctx, BatchStats{ProjectionName: "p1", EventsHandled: 3})
	observer.OnHeartbeat(ctx, DaemonStats{InstanceID: uuid.New(), IsLeader: false})
	observer.OnGapDetected(ctx, GapStats{ProjectionName: "p1", GapPosition: 2})
	observer.OnGapSkipped(ctx, GapStats{ProjectionName: "p1", GapPosition: 2})
	observer.OnRebalance(ctx, map[string]uuid.UUID{"p1": uuid.New()})

	if len(custom.batches) != 1 {
		t.Fatalf("batches len = %d, want 1", len(custom.batches))
	}
	if custom.batches[0].ProjectionName != "p1" || custom.batches[0].EventsHandled != 3 {
		t.Fatalf("unexpected batch stats: %+v", custom.batches[0])
	}
}

func TestMultiObserver_Construction(t *testing.T) {
	t.Run("empty returns NoopObserver", func(t *testing.T) {
		mo := MultiObserver()
		if _, ok := mo.(NoopObserver); !ok {
			t.Fatalf("MultiObserver() = %T, want NoopObserver", mo)
		}
	})

	t.Run("all nils returns NoopObserver", func(t *testing.T) {
		mo := MultiObserver(nil, nil, nil)
		if _, ok := mo.(NoopObserver); !ok {
			t.Fatalf("MultiObserver(nils) = %T, want NoopObserver", mo)
		}
	})

	t.Run("single non-nil unwrapped", func(t *testing.T) {
		rec := &recordingObserver{}
		mo := MultiObserver(nil, rec, nil)
		if mo != rec {
			t.Fatalf("MultiObserver(rec) = %v, want %v", mo, rec)
		}
	})

	t.Run("multiple observers preserved", func(t *testing.T) {
		rec1 := &recordingObserver{}
		rec2 := &recordingObserver{}
		mo := MultiObserver(rec1, rec2)

		multi, ok := mo.(multiObserver)
		if !ok {
			t.Fatalf("MultiObserver(rec1, rec2) type = %T, want multiObserver", mo)
		}
		if len(multi) != 2 {
			t.Fatalf("multiObserver len = %d, want 2", len(multi))
		}
		if multi[0] != rec1 || multi[1] != rec2 {
			t.Fatalf("multiObserver elements mismatch")
		}
	})

	t.Run("nested multiObservers flattened", func(t *testing.T) {
		rec1 := &recordingObserver{}
		rec2 := &recordingObserver{}
		rec3 := &recordingObserver{}
		rec4 := &recordingObserver{}

		nested1 := MultiObserver(rec1, rec2)
		nested2 := MultiObserver(rec3, rec4)
		combined := MultiObserver(nil, nested1, nil, nested2, nil)

		multi, ok := combined.(multiObserver)
		if !ok {
			t.Fatalf("combined type = %T, want multiObserver", combined)
		}
		if len(multi) != 4 {
			t.Fatalf("flattened len = %d, want 4", len(multi))
		}
		if multi[0] != rec1 || multi[1] != rec2 || multi[2] != rec3 || multi[3] != rec4 {
			t.Fatalf("flattened elements mismatch")
		}
	})
}

func TestMultiObserver_Fanout(t *testing.T) {
	ctx := context.Background()
	rec1 := &recordingObserver{}
	rec2 := &recordingObserver{}
	mo := MultiObserver(rec1, rec2)

	instanceID := uuid.New()

	batch := BatchStats{
		ProjectionName: "proj_test",
		StartPosition:  10,
		LastPosition:   20,
		HeadPosition:   25,
		Lag:            5,
		EventsRead:     10,
		EventsHandled:  10,
		Duration:       5 * time.Millisecond,
		StaleSkipped:   false,
		Error:          nil,
	}
	daemonStats := DaemonStats{
		InstanceID: instanceID,
		IsLeader:   true,
	}
	gapStats := GapStats{
		ProjectionName: "proj_test",
		GapPosition:    21,
		HighestVisible: 25,
		StaleFor:       2 * time.Second,
	}
	skippedStats := GapStats{
		ProjectionName: "proj_test",
		GapPosition:    21,
		HighestVisible: 25,
		StaleFor:       35 * time.Second,
	}
	assignments := map[string]uuid.UUID{
		"proj_test": instanceID,
	}

	mo.OnBatchProcessed(ctx, batch)
	mo.OnHeartbeat(ctx, daemonStats)
	mo.OnGapDetected(ctx, gapStats)
	mo.OnGapSkipped(ctx, skippedStats)
	mo.OnRebalance(ctx, assignments)

	for idx, rec := range []*recordingObserver{rec1, rec2} {
		if len(rec.batches) != 1 || rec.batches[0] != batch {
			t.Fatalf("rec[%d] batch = %+v, want %+v", idx, rec.batches, batch)
		}
		if len(rec.heartbeats) != 1 || rec.heartbeats[0] != daemonStats {
			t.Fatalf("rec[%d] heartbeat = %+v, want %+v", idx, rec.heartbeats, daemonStats)
		}
		if len(rec.gaps) != 1 || rec.gaps[0] != gapStats {
			t.Fatalf("rec[%d] gap = %+v, want %+v", idx, rec.gaps, gapStats)
		}
		if len(rec.skipped) != 1 || rec.skipped[0] != skippedStats {
			t.Fatalf("rec[%d] skipped = %+v, want %+v", idx, rec.skipped, skippedStats)
		}
		if len(rec.rebalances) != 1 || rec.rebalances[0]["proj_test"] != instanceID {
			t.Fatalf("rec[%d] rebalance = %+v, want %+v", idx, rec.rebalances, assignments)
		}
	}
}
