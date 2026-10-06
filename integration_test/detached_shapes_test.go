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

func TestDetachedProjection_FilterWithNoMatchesStillAdvancesCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-nomatch", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection, projectorpkg.OnStreamTypes("Nonexistent")); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	if applied := projection.uniqueApplied(); applied != 0 {
		t.Fatalf("detached applied %d events, want 0 for a filter that matches nothing", applied)
	}

	harness.stop(t)
}

func TestDetachedProjection_FilterOnlyAppliesMatchingEvents(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-filter", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection, projectorpkg.OnStreamTypes("Product")); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Product", Count: 2},
		testEventBatch{StreamType: "User", Count: 3},
	)
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	if applied := projection.uniqueApplied(); applied != 2 {
		t.Fatalf("detached applied %d events, want 2 matching Product events", applied)
	}

	harness.stop(t)
}

func TestDetachedProjection_BlockedOnGapThenResolves(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-gap", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithStaleGapThreshold(30*time.Second))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Product", Count: 1})
	later := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := later[len(later)-1].GlobalPosition

	time.Sleep(time.Second)

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d want 0 while the lower position is unresolved", checkpoint)
	}
	if applied := projection.uniqueApplied(); applied != 0 {
		t.Fatalf("detached applied %d events, want 0 while blocked on the gap", applied)
	}

	held.Commit(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after the gap resolved", checkpoint, latest)
		}
		if applied := projection.uniqueApplied(); applied != 4 {
			return fmt.Errorf("detached applied %d unique events, want 4", applied)
		}
		return nil
	})

	harness.stop(t)
}

func TestDetachedProjection_GapResolvedDuringApplyRetriesInsteadOfSkipping(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	gate := make(chan struct{})
	var gateOnce sync.Once
	release := func() { gateOnce.Do(func() { close(gate) }) }
	defer release()

	projection := &scriptedDetachedProjection{name: "detached-gap-race", gate: gate, entered: make(chan struct{}, 8)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithStaleGapThreshold(200*time.Millisecond),
	)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Product", Count: 1})
	later := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := later[len(later)-1].GlobalPosition

	// The handler is only entered once the plan is a stale-gap advance, because
	// gap-blocked batches never reach the applier.
	waitForErr(t, defaultWaitTimeout, func() error {
		select {
		case <-projection.entered:
			return nil
		default:
			return errors.New("detached handler was not entered for the stale-gap advance")
		}
	})

	held.Commit(t)
	release()

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	if applied := projection.uniqueApplied(); applied != 4 {
		t.Fatalf("unique applied=%d, want 4: the resolved position must be handled, not skipped", applied)
	}
	if skips := getGapSkipRows(t, controlDB, projection.Name()); len(skips) != 0 {
		t.Fatalf("gap skips=%d, want 0 when the gap resolved during the apply", len(skips))
	}

	harness.stop(t)
}

func TestDetachedBatchProjection_AppliesWindowInOneCall(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedBatchProjection{name: "detached-batch-bulk"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetachedBatch(projection); err != nil {
		t.Fatalf("register detached batch projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 5, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	if calls := projection.callsCount(); calls != 1 {
		t.Fatalf("batch calls = %d, want 1", calls)
	}
	history := projection.batchSizeHistory()
	if len(history) != 1 || history[0] != 5 {
		t.Fatalf("batch sizes = %v, want [5]", history)
	}

	harness.stop(t)
}

func TestDetachedProjection_PerEventPartialFailureKeepsCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-partial", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 4, "Product")
	latest := appended[len(appended)-1].GlobalPosition
	failing := appended[2].GlobalPosition
	projection.failAt(failing, -1, errors.New("destination rejected document"))

	waitForErr(t, defaultWaitTimeout, func() error {
		if projection.timesApplied(appended[0].GlobalPosition) == 0 {
			return errors.New("handler was not attempted yet")
		}
		return nil
	})

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d want 0 while the batch has a failing event", checkpoint)
	}

	projection.failAt(failing, 0, nil)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after recovery", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

func TestDetachedProjection_DoesNotHoldAssignmentLockWhileApplying(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	gate := make(chan struct{})
	defer close(gate)
	projection := &scriptedDetachedProjection{name: "detached-lock", gate: gate, entered: make(chan struct{}, 8)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	appendTestEvents(t, controlDB, eventStore, 1, "Product")
	waitForErr(t, defaultWaitTimeout, func() error {
		select {
		case <-projection.entered:
			return nil
		default:
			return errors.New("detached handler never entered")
		}
	})

	ctx := context.Background()
	conn, err := controlDB.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire lock connection: %v", err)
	}
	lockTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock transaction: %v", err)
	}
	if _, err := lockTx.Exec(ctx, "SET LOCAL lock_timeout = '1s'"); err != nil {
		t.Fatalf("set lock timeout: %v", err)
	}

	var owner *uuid.UUID
	if err := lockTx.QueryRow(ctx,
		`SELECT instance_id FROM projection_assignments WHERE projection_name = $1 FOR UPDATE`,
		projection.Name()).Scan(&owner); err != nil {
		t.Fatalf("assignment row was locked while the detached handler was blocked: %v", err)
	}

	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release assignment lock: %v", err)
	}
	conn.Release()

	harness.stop(t)
}

func TestBatchProjection_RespectsWindowBoundary(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &batchTestProjection{name: "batch-window", instanceLabel: "projector-1"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddBatch(projection); err != nil {
		t.Fatalf("register batch projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithBatchSize(2))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 5, "Order")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	if history := projection.BatchSizes(); len(history) < 3 {
		t.Fatalf("batch sizes = %v, want at least 3 batches for 5 events with window 2", history)
	}

	harness.stop(t)
}

func TestBatchProjection_DoesNotReceiveGappedPosition(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &batchTestProjection{name: "batch-gap", instanceLabel: "projector-1"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddBatch(projection); err != nil {
		t.Fatalf("register batch projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithStaleGapThreshold(30*time.Second))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Order", Count: 1})
	later := appendTestEvents(t, controlDB, eventStore, 3, "Order")
	latest := later[len(later)-1].GlobalPosition

	time.Sleep(time.Second)

	if calls := projection.Calls(); calls != 0 {
		t.Fatalf("batch handler called %d times while blocked on a gap, want 0", calls)
	}

	held.Commit(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 4 {
			return fmt.Errorf("handled rows = %d, want 4", len(rows))
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistry_FromProjectionsKeepsClassicBehavior(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("classic-projection", "projector-1", nil)

	registry := projectorpkg.FromProjections(projection)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Order")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 3 {
			return fmt.Errorf("handled rows = %d, want 3", len(rows))
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistry_DeprecatedFilterWrapperStillRegisters(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("wrapped-projection", "projector-1", nil)

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(projectorpkg.FilterStreamTypes(projection, "Order")); err != nil {
		t.Fatalf("register wrapped projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Order", Count: 2},
		testEventBatch{StreamType: "User", Count: 2},
	)
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		rows := getHandledRows(t, controlDB, projection.Name())
		if len(rows) != 2 {
			return fmt.Errorf("handled rows = %d, want 2 Order events", len(rows))
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistrationFilter_CombinedStreamAndEventTypes(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("combined-filter", "projector-1", nil)

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(projection,
		projectorpkg.OnStreamTypes("Order"),
		projectorpkg.OnEventTypes("Order.event.1"),
	); err != nil {
		t.Fatalf("register filtered projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Order", Count: 3},
		testEventBatch{StreamType: "User", Count: 2},
	)
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		rows := getHandledRows(t, controlDB, projection.Name())
		if len(rows) != 1 {
			return fmt.Errorf("handled rows = %d, want 1", len(rows))
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistrationFilter_TwoProjectionsDifferentFilters(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	orders := newTestProjection("orders-only", "projector-1", nil)
	users := newTestProjection("users-only", "projector-1", nil)

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(orders, projectorpkg.OnStreamTypes("Order")); err != nil {
		t.Fatalf("register orders projection: %v", err)
	}
	if err := registry.AddProjection(users, projectorpkg.OnStreamTypes("User")); err != nil {
		t.Fatalf("register users projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Order", Count: 2},
		testEventBatch{StreamType: "User", Count: 3},
	)
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, projection := range []*testProjection{orders, users} {
			if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
				return fmt.Errorf("%s checkpoint=%d want %d", projection.Name(), checkpoint, latest)
			}
		}
		if rows := getHandledRows(t, controlDB, orders.Name()); len(rows) != 2 {
			return fmt.Errorf("orders handled %d rows, want 2", len(rows))
		}
		if rows := getHandledRows(t, controlDB, users.Name()); len(rows) != 3 {
			return fmt.Errorf("users handled %d rows, want 3", len(rows))
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistrationFilter_ObserverAccounting(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("accounted-filter", "projector-1", nil)
	observer := &testIntegrationObserver{}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(projection, projectorpkg.OnStreamTypes("Order")); err != nil {
		t.Fatalf("register filtered projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithObserver(observer))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Order", Count: 2},
		testEventBatch{StreamType: "User", Count: 3},
	)
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	batches := observer.Batches()
	if len(batches) == 0 {
		t.Fatal("no batches observed")
	}

	totalHandled := 0
	reportedTwo := false
	for _, batch := range batches {
		if batch.EventsHandled > batch.EventsRead {
			t.Fatalf("batch reported EventsHandled=%d > EventsRead=%d", batch.EventsHandled, batch.EventsRead)
		}
		totalHandled += batch.EventsHandled
		if batch.EventsHandled == 2 {
			reportedTwo = true
		}
	}
	if !reportedTwo {
		t.Fatalf("no batch reported the 2 matching events; batches=%v", batches)
	}
	if totalHandled != 2 {
		t.Fatalf("total EventsHandled=%d, want 2 (filtered events must not count as handled)", totalHandled)
	}

	harness.stop(t)
}

func TestCheckpointContention_BlocksUntilLockReleased(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("contended", "projector-1", nil)

	ctx := context.Background()
	if _, err := controlDB.Exec(ctx,
		`INSERT INTO projection_checkpoints (projection_name, last_position, created_at, updated_at) VALUES ($1, 0, NOW(), NOW()) ON CONFLICT (projection_name) DO NOTHING`,
		projection.Name()); err != nil {
		t.Fatalf("seed checkpoint row: %v", err)
	}

	conn, err := controlDB.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire lock connection: %v", err)
	}
	lockTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock transaction: %v", err)
	}
	var locked int64
	if err := lockTx.QueryRow(ctx,
		`SELECT last_position FROM projection_checkpoints WHERE projection_name = $1 FOR UPDATE`,
		projection.Name()).Scan(&locked); err != nil {
		t.Fatalf("lock checkpoint row: %v", err)
	}

	harness := startTestProjector(t, "projector-1", []*testProjection{projection}, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 1, "Order")
	latest := appended[len(appended)-1].GlobalPosition

	time.Sleep(700 * time.Millisecond)

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d want 0 while another session holds the checkpoint row", checkpoint)
	}

	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release checkpoint lock: %v", err)
	}
	conn.Release()

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after the lock was released", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

func TestPermanentHole_ResumesAfterRestartWithoutDuplicateSkip(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	name := "permanent-hole"

	options := append(defaultProjectorOptions(),
		projectorpkg.WithStaleGapThreshold(250*time.Millisecond),
		projectorpkg.WithStaleGapHarborLag(1),
		projectorpkg.WithPollInterval(500*time.Millisecond),
		projectorpkg.WithMaxPollInterval(500*time.Millisecond),
		projectorpkg.WithBatchPause(500*time.Millisecond),
	)

	projector1 := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection(name, "projector-1", nil),
	}, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Shipment", Count: 1})
	later := appendTestEvents(t, controlDB, eventStore, 4, "Shipment")
	expectedSkipTo := later[len(later)-2].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if skips := getGapSkipRows(t, controlDB, name); len(skips) != 1 {
			return fmt.Errorf("gap skips=%d, want 1", len(skips))
		}
		return nil
	})

	projector1.stop(t)
	held.Rollback(t)

	projector2 := startTestProjector(t, "projector-2", []*testProjection{
		newTestProjection(name, "projector-2", nil),
	}, options...)

	more := appendTestEvents(t, controlDB, eventStore, 2, "Shipment")
	finalLatest := more[len(more)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != finalLatest {
			return fmt.Errorf("checkpoint=%d want %d after restart", checkpoint, finalLatest)
		}
		return nil
	})

	time.Sleep(500 * time.Millisecond)

	skips := getGapSkipRows(t, controlDB, name)
	if len(skips) != 1 {
		t.Fatalf("gap skips=%d after restart, want 1 (no duplicate audit row)", len(skips))
	}
	if skips[0].SkipToPosition != expectedSkipTo {
		t.Fatalf("skip_to_position=%d want %d", skips[0].SkipToPosition, expectedSkipTo)
	}

	projector2.stop(t)
}
