//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/eventsalsa/store"
	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpkg "github.com/eventsalsa/projector"
	projectorfailure "github.com/eventsalsa/projector/failure"
)

func TestFailurePolicy_RetryableFailureIsNotFatal(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	appended := appendTestEvents(t, controlDB, eventStore, 1, "Product")
	latest := appended[0].GlobalPosition

	projection := &scriptedDetachedProjection{name: "retryable-policy", entered: make(chan struct{}, 64)}
	projection.failAt(latest, -1, projectorfailure.Retryable(errors.New("search index unavailable")))
	observer := &testIntegrationObserver{}

	registry := projectorpkg.NewRegistry()
	// FailFast would be fatal for unclassified errors; the sentinel must win.
	if err := registry.AddDetached(projection, projectorpkg.WithFailurePolicy(projectorpkg.FailurePolicyFailFast)); err != nil {
		t.Fatalf("register projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithObserver(observer))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		if len(observer.Degraded()) < 2 {
			return errors.New("no degradation reported yet")
		}
		return nil
	})

	select {
	case <-harness.done:
		t.Fatalf("daemon exited on a retryable failure: %v", harness.result)
	default:
	}

	projection.failAt(latest, 0, nil)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after recovery", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

func TestFailurePolicy_PermanentStopsProjectionButKeepsDaemonAlive(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	poisoned := &scriptedDetachedProjection{name: "permanent-policy", entered: make(chan struct{}, 64)}
	poisoned.failEveryEvent(projectorfailure.Permanent(errors.New("schema mismatch")))
	healthy := newTestProjection("healthy-policy", "projector-1", []string{"Order"})

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(poisoned, projectorpkg.OnStreamTypes("Order")); err != nil {
		t.Fatalf("register poisoned projection: %v", err)
	}
	if err := registry.AddProjection(healthy, projectorpkg.OnStreamTypes("Order")); err != nil {
		t.Fatalf("register healthy projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	first := appendTestEvents(t, controlDB, eventStore, 2, "Order")

	waitForErr(t, defaultWaitTimeout, func() error {
		if poisoned.Attempts() == 0 {
			return errors.New("poisoned projection was not attempted yet")
		}
		if rows := getHandledRows(t, controlDB, healthy.Name()); len(rows) != len(first) {
			return fmt.Errorf("healthy projection handled %d rows, want %d", len(rows), len(first))
		}
		return nil
	})

	attemptsAfterStop := poisoned.Attempts()

	second := appendTestEvents(t, controlDB, eventStore, 2, "Order")
	total := len(first) + len(second)

	waitForErr(t, defaultWaitTimeout, func() error {
		if rows := getHandledRows(t, controlDB, healthy.Name()); len(rows) != total {
			return fmt.Errorf("healthy projection handled %d rows, want %d", len(rows), total)
		}
		return nil
	})

	select {
	case <-harness.done:
		t.Fatalf("daemon exited when a single projection stopped: %v", harness.result)
	default:
	}

	if attempts := poisoned.Attempts(); attempts != attemptsAfterStop {
		t.Fatalf("poisoned projection was restarted (attempts %d -> %d), want it to stay stopped", attemptsAfterStop, attempts)
	}

	harness.stop(t)
}

func TestFailurePolicy_PoisonHandlerSkipsBatchAndAdvancesCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	appended := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	projection := &scriptedDetachedProjection{name: "poison-policy", entered: make(chan struct{}, 64)}
	projection.failAt(appended[1].GlobalPosition, -1, projectorfailure.Permanent(errors.New("malformed document")))

	var mu sync.Mutex
	var recorded [][]store.PersistedEvent
	poison := func(_ context.Context, _ string, events []store.PersistedEvent, _ error) error {
		mu.Lock()
		defer mu.Unlock()
		recorded = append(recorded, append([]store.PersistedEvent(nil), events...))
		return nil
	}

	observer := &testIntegrationObserver{}
	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection, projectorpkg.WithPoisonHandler(poison)); err != nil {
		t.Fatalf("register projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithObserver(observer))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		mu.Lock()
		calls := len(recorded)
		mu.Unlock()
		if calls == 0 {
			return errors.New("poison handler was not called yet")
		}
		if len(observer.PoisonSkipped()) == 0 {
			return errors.New("no poison skip observed yet")
		}
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after the skip", checkpoint, latest)
		}
		return nil
	})

	mu.Lock()
	batchSize := len(recorded[0])
	mu.Unlock()
	if batchSize != len(appended) {
		t.Fatalf("poison handler received %d events, want the whole %d-event batch", batchSize, len(appended))
	}

	// The projection must continue after the skip.
	more := appendTestEvents(t, controlDB, eventStore, 1, "Product")
	finalLatest := more[0].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != finalLatest {
			return fmt.Errorf("checkpoint=%d want %d after continuing past the poison batch", checkpoint, finalLatest)
		}
		return nil
	})

	harness.stop(t)
}
