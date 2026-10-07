//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/eventsalsa/store"
	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpkg "github.com/eventsalsa/projector"
)

// stubbornTxProjection blocks in its handler and deliberately ignores context
// cancellation, so shutdown has to give up after ShutdownTimeout.
type stubbornTxProjection struct {
	name     string
	label    string
	entered  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (p *stubbornTxProjection) Name() string { return p.name }

//nolint:gocritic // hugeParam: implements the Projection contract
func (p *stubbornTxProjection) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
	defer close(p.finished)

	select {
	case p.entered <- struct{}{}:
	default:
	}

	<-p.release

	_, err := tx.Exec(ctx, `
INSERT INTO test_projection_events (
projection_name,
global_position,
stream_type,
stream_id,
event_type,
handled_by,
attempt_no
) VALUES ($1, $2, $3, $4, $5, $6, $7)
`, p.name, event.GlobalPosition, event.StreamType, event.StreamID, event.EventType, p.label, 1)
	if err != nil {
		return fmt.Errorf("insert stubborn projection row: %w", err)
	}

	return nil
}

func TestShutdownTimeout_ForceCancelsHandlerThatIgnoresContext(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &stubbornTxProjection{
		name:     "stubborn-projection",
		label:    "projector-1",
		entered:  make(chan struct{}, 8),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(projection); err != nil {
		t.Fatalf("register projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithShutdownTimeout(300*time.Millisecond))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	appendTestEvents(t, controlDB, eventStore, 1, "Order")

	waitForErr(t, defaultWaitTimeout, func() error {
		select {
		case <-projection.entered:
			return nil
		default:
			return errors.New("stubborn handler was not entered yet")
		}
	})

	start := time.Now()
	harness.stop(t)
	elapsed := time.Since(start)

	// Release the leaked handler goroutine and wait for it to finish before the
	// test tears down the pool or the tables.
	close(projection.release)
	select {
	case <-projection.finished:
	case <-time.After(3 * time.Second):
		t.Fatal("stubborn handler did not finish after release")
	}

	if elapsed > 5*time.Second {
		t.Fatalf("shutdown took %v; it should give up after ShutdownTimeout instead of blocking", elapsed)
	}
	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d, want 0: a batch canceled by shutdown must not be checkpointed", checkpoint)
	}
}

func TestMixedShapes_AllFourRunInOneDaemon(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	txEvent := newTestProjection("mixed-tx-event", "projector-1", nil)
	txBatch := &batchTestProjection{name: "mixed-tx-batch", instanceLabel: "projector-1"}
	detEvent := &scriptedDetachedProjection{name: "mixed-det-event", entered: make(chan struct{}, 64)}
	detBatch := &scriptedDetachedBatchProjection{name: "mixed-det-batch"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(txEvent, projectorpkg.OnStreamTypes("tx-event")); err != nil {
		t.Fatalf("register transactional per-event projection: %v", err)
	}
	if err := registry.AddBatch(txBatch, projectorpkg.OnStreamTypes("tx-batch")); err != nil {
		t.Fatalf("register transactional batch projection: %v", err)
	}
	if err := registry.AddDetached(detEvent, projectorpkg.OnStreamTypes("det-event")); err != nil {
		t.Fatalf("register detached per-event projection: %v", err)
	}
	if err := registry.AddDetachedBatch(detBatch, projectorpkg.OnStreamTypes("det-batch")); err != nil {
		t.Fatalf("register detached batch projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "tx-event", Count: 1},
		testEventBatch{StreamType: "tx-batch", Count: 1},
		testEventBatch{StreamType: "det-event", Count: 1},
		testEventBatch{StreamType: "det-batch", Count: 1},
	)
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{txEvent.Name(), txBatch.Name(), detEvent.Name(), detBatch.Name()} {
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != latest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, latest)
			}
		}

		if rows := getHandledRows(t, controlDB, txEvent.Name()); len(rows) != 1 {
			return fmt.Errorf("transactional per-event rows=%d, want 1", len(rows))
		}
		if rows := getHandledRows(t, controlDB, txBatch.Name()); len(rows) != 1 {
			return fmt.Errorf("transactional batch rows=%d, want 1", len(rows))
		}
		if applied := detEvent.uniqueApplied(); applied != 1 {
			return fmt.Errorf("detached per-event applied=%d, want 1", applied)
		}
		if calls := detBatch.callsCount(); calls != 1 {
			return fmt.Errorf("detached batch calls=%d, want 1", calls)
		}
		return nil
	})

	harness.stop(t)
}
