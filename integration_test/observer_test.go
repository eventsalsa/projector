//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpkg "github.com/eventsalsa/projector"
)

type testIntegrationObserver struct {
	mu         sync.Mutex
	batches    []projectorpkg.BatchStats
	heartbeats []projectorpkg.DaemonStats
	gaps       []projectorpkg.GapStats
	skipped    []projectorpkg.GapStats
	degraded   []projectorpkg.DegradedStats
	poison     []projectorpkg.PoisonBatchStats
	rebalances []map[string]uuid.UUID
}

//nolint:gocritic // hugeParam: test mock
func (o *testIntegrationObserver) OnBatchProcessed(_ context.Context, stats projectorpkg.BatchStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.batches = append(o.batches, stats)
}

func (o *testIntegrationObserver) OnHeartbeat(_ context.Context, stats projectorpkg.DaemonStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.heartbeats = append(o.heartbeats, stats)
}

func (o *testIntegrationObserver) OnGapDetected(_ context.Context, stats projectorpkg.GapStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.gaps = append(o.gaps, stats)
}

func (o *testIntegrationObserver) OnGapSkipped(_ context.Context, stats projectorpkg.GapStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.skipped = append(o.skipped, stats)
}

func (o *testIntegrationObserver) OnProjectionDegraded(_ context.Context, stats projectorpkg.DegradedStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.degraded = append(o.degraded, stats)
}

func (o *testIntegrationObserver) OnPoisonBatchSkipped(_ context.Context, stats projectorpkg.PoisonBatchStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.poison = append(o.poison, stats)
}

func (o *testIntegrationObserver) OnRebalance(_ context.Context, assignments map[string]uuid.UUID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	copied := make(map[string]uuid.UUID, len(assignments))
	for k, v := range assignments {
		copied[k] = v
	}
	o.rebalances = append(o.rebalances, copied)
}

func (o *testIntegrationObserver) Batches() []projectorpkg.BatchStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.BatchStats(nil), o.batches...)
}

func (o *testIntegrationObserver) Heartbeats() []projectorpkg.DaemonStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.DaemonStats(nil), o.heartbeats...)
}

func (o *testIntegrationObserver) Gaps() []projectorpkg.GapStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.GapStats(nil), o.gaps...)
}

func (o *testIntegrationObserver) Skipped() []projectorpkg.GapStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.GapStats(nil), o.skipped...)
}

func (o *testIntegrationObserver) Degraded() []projectorpkg.DegradedStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.DegradedStats(nil), o.degraded...)
}

func (o *testIntegrationObserver) PoisonSkipped() []projectorpkg.PoisonBatchStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.PoisonBatchStats(nil), o.poison...)
}

func (o *testIntegrationObserver) Rebalances() []map[string]uuid.UUID {
	o.mu.Lock()
	defer o.mu.Unlock()
	copied := make([]map[string]uuid.UUID, len(o.rebalances))
	for i, reb := range o.rebalances {
		m := make(map[string]uuid.UUID, len(reb))
		for k, v := range reb {
			m[k] = v
		}
		copied[i] = m
	}
	return copied
}

func TestIntegration_Observer_FullLifecycle(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("projection-observer-lifecycle", "projector-1", nil)
	observer := &testIntegrationObserver{}

	projector := startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(), projectorpkg.WithObserver(observer))...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		if len(observer.Heartbeats()) == 0 {
			return fmt.Errorf("no heartbeats received by observer yet")
		}
		if len(observer.Rebalances()) == 0 {
			return fmt.Errorf("no rebalances received by observer yet")
		}
		return nil
	})

	appended := appendTestEvents(t, controlDB, eventStore, 5, "Account")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		batches := observer.Batches()
		if len(batches) == 0 {
			return fmt.Errorf("no batch stats observed yet")
		}
		lastBatch := batches[len(batches)-1]
		if lastBatch.LastPosition != latest {
			return fmt.Errorf("last batch position=%d want %d", lastBatch.LastPosition, latest)
		}
		if lastBatch.EventsHandled != 5 {
			return fmt.Errorf("last batch handled=%d want 5", lastBatch.EventsHandled)
		}
		if lastBatch.Error != nil {
			return fmt.Errorf("batch error = %v, want nil", lastBatch.Error)
		}
		return nil
	})

	heartbeats := observer.Heartbeats()
	if len(heartbeats) == 0 {
		t.Fatal("expected heartbeats in observer")
	}
	if heartbeats[0].InstanceID != projector.daemon.ID() {
		t.Fatalf("heartbeat InstanceID = %v, want %v", heartbeats[0].InstanceID, projector.daemon.ID())
	}

	rebalances := observer.Rebalances()
	if len(rebalances) == 0 {
		t.Fatal("expected rebalances in observer")
	}
	if rebalances[0][projection.Name()] != projector.daemon.ID() {
		t.Fatalf("rebalance mapping for %s = %v, want %v", projection.Name(), rebalances[0][projection.Name()], projector.daemon.ID())
	}
}

func TestIntegration_Observer_BacklogCatchupLag(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	// Seed 60 events prior to starting projector
	appended := appendTestEvents(t, controlDB, eventStore, 60, "Invoice")
	latest := appended[len(appended)-1].GlobalPosition

	projection := newTestProjection("projection-observer-lag", "projector-1", nil)
	observer := &testIntegrationObserver{}

	_ = startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(),
			projectorpkg.WithBatchSize(20),
			projectorpkg.WithObserver(observer),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		batches := observer.Batches()
		if len(batches) < 3 {
			return fmt.Errorf("expected at least 3 batches for 60 events with batch size 20, got %d", len(batches))
		}
		return nil
	})

	batches := observer.Batches()
	for i, b := range batches {
		if b.Error != nil {
			t.Fatalf("batch %d error: %v", i, b.Error)
		}
		if b.Duration <= 0 {
			t.Fatalf("batch %d duration = %v, want > 0", i, b.Duration)
		}
	}
	// Last batch should be caught up
	last := batches[len(batches)-1]
	if last.LastPosition != latest {
		t.Fatalf("last batch LastPosition = %d, want %d", last.LastPosition, latest)
	}
}

func TestIntegration_Observer_GapDetectionAndStaleSkip(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("projection-observer-gap", "projector-1", nil)
	observer := &testIntegrationObserver{}

	projector := startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(),
			projectorpkg.WithStaleGapThreshold(250*time.Millisecond),
			projectorpkg.WithStaleGapHarborLag(1),
			projectorpkg.WithPollInterval(200*time.Millisecond),
			projectorpkg.WithObserver(observer),
		)...,
	)

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Order", Count: 1})
	later := appendTestEvents(t, controlDB, eventStore, 4, "Order")
	expectedSkipTo := later[len(later)-2].GlobalPosition

	// Wait for gap detection
	waitForErr(t, defaultWaitTimeout, func() error {
		if len(observer.Gaps()) == 0 {
			return fmt.Errorf("no gap detected by observer yet")
		}
		return nil
	})

	gapStats := observer.Gaps()[0]
	if gapStats.ProjectionName != projection.Name() {
		t.Fatalf("gap ProjectionName = %q, want %q", gapStats.ProjectionName, projection.Name())
	}
	if gapStats.GapPosition != held.events[0].GlobalPosition {
		t.Fatalf("gap GapPosition = %d, want %d", gapStats.GapPosition, held.events[0].GlobalPosition)
	}

	// Wait for stale gap skip
	waitForErr(t, defaultWaitTimeout, func() error {
		if len(observer.Skipped()) == 0 {
			return fmt.Errorf("no gap skip observed yet")
		}
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != expectedSkipTo {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, expectedSkipTo)
		}
		return nil
	})

	projector.stop(t)
	held.Rollback(t)

	skipped := observer.Skipped()[0]
	if skipped.ProjectionName != projection.Name() {
		t.Fatalf("skipped ProjectionName = %q, want %q", skipped.ProjectionName, projection.Name())
	}
	if skipped.GapPosition != held.events[0].GlobalPosition {
		t.Fatalf("skipped GapPosition = %d, want %d", skipped.GapPosition, held.events[0].GlobalPosition)
	}

	batches := observer.Batches()
	foundStaleSkipped := false
	for _, b := range batches {
		if b.StaleSkipped {
			foundStaleSkipped = true
			if b.LastPosition != expectedSkipTo {
				t.Fatalf("stale skipped batch LastPosition = %d, want %d", b.LastPosition, expectedSkipTo)
			}
		}
	}
	if !foundStaleSkipped {
		t.Fatal("expected at least one batch with StaleSkipped = true")
	}
}

func TestIntegration_Observer_BatchFailureAndRecovery(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("projection-observer-retry", "projector-1", nil)
	observer := &testIntegrationObserver{}

	_ = startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(), projectorpkg.WithObserver(observer))...,
	)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Customer")
	failingPos := appended[1].GlobalPosition
	lastPos := appended[2].GlobalPosition

	projection.FailUntilCleared(failingPos, errors.New("simulated transient handler failure"))

	// Wait until failure is observed
	waitForErr(t, defaultWaitTimeout, func() error {
		batches := observer.Batches()
		for _, b := range batches {
			if b.Error != nil {
				return nil
			}
		}
		return fmt.Errorf("no failed batch observed yet")
	})

	// Clear failure and verify recovery
	projection.ClearFailure(failingPos)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != lastPos {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, lastPos)
		}
		batches := observer.Batches()
		last := batches[len(batches)-1]
		if last.Error != nil {
			return fmt.Errorf("last batch still has error: %v", last.Error)
		}
		if last.LastPosition != lastPos {
			return fmt.Errorf("last batch position=%d want %d", last.LastPosition, lastPos)
		}
		return nil
	})
}
