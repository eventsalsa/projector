//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/eventsalsa/store"
	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpkg "github.com/eventsalsa/projector"
)

// gatedDetachedProjection is a DetachedProjection whose handler can be blocked
// and whose applied events are recorded in memory.
type gatedDetachedProjection struct {
	name    string
	gate    chan struct{}
	entered chan struct{}

	mu      sync.Mutex
	applied []store.PersistedEvent
	err     error
}

func (p *gatedDetachedProjection) Name() string { return p.name }

//nolint:gocritic // hugeParam: implements the DetachedProjection contract
func (p *gatedDetachedProjection) Handle(ctx context.Context, event store.PersistedEvent) error {
	if p.entered != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
	}

	if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil {
		return p.err
	}

	p.applied = append(p.applied, event)
	return nil
}

func (p *gatedDetachedProjection) AppliedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.applied)
}

func (p *gatedDetachedProjection) SetError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// batchTestProjection is a BatchProjection that writes its whole window through
// the provided transaction in one handler call.
type batchTestProjection struct {
	name          string
	instanceLabel string

	mu         sync.Mutex
	calls      int
	batchSizes []int
	err        error
}

func (p *batchTestProjection) Name() string { return p.name }

func (p *batchTestProjection) Handle(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) error {
	p.mu.Lock()
	p.calls++
	p.batchSizes = append(p.batchSizes, len(events))
	handleErr := p.err
	p.mu.Unlock()

	if handleErr != nil {
		return handleErr
	}

	for _, event := range events {
		if _, err := tx.Exec(ctx, `
INSERT INTO test_projection_events (
projection_name,
global_position,
stream_type,
stream_id,
event_type,
handled_by,
attempt_no
) VALUES ($1, $2, $3, $4, $5, $6, $7)
`, p.name, event.GlobalPosition, event.StreamType, event.StreamID, event.EventType, p.instanceLabel, 1); err != nil {
			return fmt.Errorf("insert batch projection row: %w", err)
		}
	}

	return nil
}

func (p *batchTestProjection) Calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *batchTestProjection) FirstBatchSize() (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.batchSizes) == 0 {
		return 0, false
	}
	return p.batchSizes[0], true
}

func (p *batchTestProjection) BatchSizes() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int(nil), p.batchSizes...)
}

func (p *batchTestProjection) SetError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// gatedTxProjection is a transactional per-event projection whose handler can be
// blocked, used to exercise BatchTimeout cancellation.
type gatedTxProjection struct {
	name          string
	instanceLabel string
	gate          chan struct{}
	entered       chan struct{}
}

func (p *gatedTxProjection) Name() string { return p.name }

//nolint:gocritic // hugeParam: implements the Projection contract
func (p *gatedTxProjection) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
	if p.entered != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
	}

	if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if _, err := tx.Exec(ctx, `
INSERT INTO test_projection_events (
projection_name,
global_position,
stream_type,
stream_id,
event_type,
handled_by,
attempt_no
) VALUES ($1, $2, $3, $4, $5, $6, $7)
`, p.name, event.GlobalPosition, event.StreamType, event.StreamID, event.EventType, p.instanceLabel, 1); err != nil {
		return fmt.Errorf("insert gated projection row: %w", err)
	}

	return nil
}

func TestDetachedProjection_ProcessesAndAdvancesCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &gatedDetachedProjection{name: "detached-products", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 5, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if count := projection.AppliedCount(); count != 5 {
			return fmt.Errorf("detached applied %d events, want 5", count)
		}
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

func TestDetachedProjection_ApplyErrorDoesNotAdvanceCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &gatedDetachedProjection{
		name:    "detached-failing",
		entered: make(chan struct{}, 64),
		err:     errors.New("destination unavailable"),
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 2, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		select {
		case <-projection.entered:
			return nil
		default:
			return errors.New("detached handler was not attempted yet")
		}
	})

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d want 0 while the destination is failing", checkpoint)
	}

	projection.SetError(nil)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after recovery", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

func TestDetachedProjection_DoesNotHoldConnectionDuringRemoteIO(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	gate := make(chan struct{})
	detached := &gatedDetachedProjection{name: "detached-blocking", gate: gate, entered: make(chan struct{}, 64)}
	txProjection := newTestProjection("tx-orders", "projector-1", []string{"Tx"})

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(detached, projectorpkg.OnStreamTypes("Remote")); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}
	if err := registry.AddProjection(txProjection, projectorpkg.OnStreamTypes("Tx")); err != nil {
		t.Fatalf("register tx projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		// The advisory leader strategy pins one pooled connection for its session
		// lock, which would exhaust a single-connection pool by itself. The lease
		// strategy uses short transactions, so the only thing that could hold the
		// connection is the detached handler.
		projectorpkg.WithLeaderStrategy(projectorpkg.LeaderStrategyLease),
	)
	// A single pooled connection: if the detached handler held it during network
	// I/O, the transactional projection could never run. The application name lets
	// us attribute sessions to this daemon in pg_stat_activity.
	const appName = "detached_io_test"
	pool := openTestDBWithAppName(t, 1, appName)
	t.Cleanup(pool.Close)

	harness := startTestProjectorWithPool(t, "projector-1", pool, registry, options...)

	appendTestEvents(t, controlDB, eventStore, 1, "Remote")
	waitForErr(t, defaultWaitTimeout, func() error {
		select {
		case <-detached.entered:
			return nil
		default:
			return errors.New("detached handler never entered")
		}
	})

	appendedEvents := appendTestEvents(t, controlDB, eventStore, 2, "Tx")

	waitForErr(t, defaultWaitTimeout, func() error {
		rows := getHandledRows(t, controlDB, txProjection.Name())
		if len(rows) != 2 {
			return fmt.Errorf("transactional projection handled %d rows, want 2 while the detached handler is blocked", len(rows))
		}
		return nil
	})

	ctx := context.Background()
	var sessions int
	if err := controlDB.QueryRow(ctx,
		`SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = $1`, appName).Scan(&sessions); err != nil {
		t.Fatalf("query pg_stat_activity sessions: %v", err)
	}
	if sessions == 0 {
		t.Fatal("no daemon sessions found for the test application name; attribution is broken")
	}

	var idleInTransaction int
	if err := controlDB.QueryRow(ctx,
		`SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = $1 AND state = 'idle in transaction'`,
		appName).Scan(&idleInTransaction); err != nil {
		t.Fatalf("query pg_stat_activity idle-in-transaction: %v", err)
	}
	if idleInTransaction != 0 {
		t.Fatalf("%d daemon session(s) idle in transaction while the detached handler was blocked", idleInTransaction)
	}

	close(gate)
	if len(appendedEvents) != 2 {
		t.Fatalf("appended %d Tx events, want 2", len(appendedEvents))
	}

	harness.stop(t)
}

func TestBatchTransactionalProjection_HandlesWindowInOneCall(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &batchTestProjection{name: "batch-orders", instanceLabel: "projector-1"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddBatch(projection); err != nil {
		t.Fatalf("register batch projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 5, "Order")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	if projection.Calls() != 1 {
		t.Fatalf("batch handler calls = %d, want 1 for a single window", projection.Calls())
	}
	size, ok := projection.FirstBatchSize()
	if !ok || size != 5 {
		t.Fatalf("first batch size = %d (ok=%v), want 5", size, ok)
	}

	harness.stop(t)
}

func TestBatchTransactionalProjection_ErrorRollsBackWholeBatch(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &batchTestProjection{
		name:          "batch-failing",
		instanceLabel: "projector-1",
		err:           errors.New("bulk import failed"),
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddBatch(projection); err != nil {
		t.Fatalf("register batch projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Order")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if projection.Calls() == 0 {
			return errors.New("batch handler was not called yet")
		}
		return nil
	})

	if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 0 {
		t.Fatalf("read model rows = %d, want 0 while the batch is failing", len(rows))
	}
	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d want 0 while the batch is failing", checkpoint)
	}

	projection.SetError(nil)

	waitForErr(t, defaultWaitTimeout, func() error {
		if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 3 {
			return fmt.Errorf("read model rows = %d, want 3 after recovery", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistrationEventTypeFilter_AdvancesCheckpointPastSkippedEvents(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("filtered-by-event-type", "projector-1", nil)

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(projection, projectorpkg.OnEventTypes("Order.event.1")); err != nil {
		t.Fatalf("register filtered projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore, testEventBatch{StreamType: "Order", Count: 2})
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d (unscoped frontier)", checkpoint, latest)
		}
		rows := getHandledRows(t, controlDB, projection.Name())
		if len(rows) != 1 {
			return fmt.Errorf("handled rows = %d, want 1", len(rows))
		}
		if rows[0].EventType != "Order.event.1" {
			return fmt.Errorf("handled event type = %q, want Order.event.1", rows[0].EventType)
		}
		return nil
	})

	harness.stop(t)
}

func TestBatchTimeout_CancelsBlockedHandlerWithoutAdvancingCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	gate := make(chan struct{})
	projection := &gatedTxProjection{
		name:          "slow-tx",
		instanceLabel: "projector-1",
		gate:          gate,
		entered:       make(chan struct{}, 8),
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(projection); err != nil {
		t.Fatalf("register slow projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithBatchTimeout(300*time.Millisecond),
	)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 1, "Order")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		select {
		case <-projection.entered:
			return nil
		default:
			return errors.New("slow handler was not entered yet")
		}
	})

	time.Sleep(600 * time.Millisecond)

	if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 0 {
		t.Fatalf("read model rows = %d, want 0 after the batch timeout", len(rows))
	}
	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d want 0 after the batch timeout", checkpoint)
	}

	close(gate)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after recovery", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}
