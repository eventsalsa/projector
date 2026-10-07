//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/eventsalsa/store"
	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpkg "github.com/eventsalsa/projector"
)

// faultState arms a fault that a faultTx applies to a matching statement.
type faultState struct {
	mu        sync.Mutex
	remaining int
	err       error
	dropConn  bool
}

func (s *faultState) arm(failures int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remaining = failures
	s.err = err
	s.dropConn = false
}

// armDropConn arms the fault to terminate the backend running the matching
// statement, simulating a connection drop rather than a statement error.
func (s *faultState) armDropConn(failures int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remaining = failures
	s.err = nil
	s.dropConn = true
}

func (s *faultState) consume() (error, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remaining == 0 {
		return nil, false, false
	}
	if s.remaining > 0 {
		s.remaining--
	}
	return s.err, s.dropConn, true
}

// faultTx delegates everything to the real transaction but can fail a
// checkpoint write on demand, with a realistic Postgres error shape.
type faultTx struct {
	pgx.Tx
	state *faultState
}

func (t *faultTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "projection_checkpoints") {
		if err, drop, ok := t.state.consume(); ok {
			if drop {
				return t.terminateBackend(ctx)
			}
			return pgconn.CommandTag{}, err
		}
	}
	return t.Tx.Exec(ctx, sql, args...)
}

// terminateBackend kills the server process running this transaction, so the
// client observes a genuine connection drop.
func (t *faultTx) terminateBackend(ctx context.Context) (pgconn.CommandTag, error) {
	if _, err := t.Tx.Exec(ctx, `SELECT pg_terminate_backend(pg_backend_pid())`); err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("terminate checkpoint backend: %w", err)
	}

	return pgconn.CommandTag{}, errors.New("checkpoint connection terminated by the server")
}

// faultPool implements projector.PgxPool over a real pool and injects the fault
// into every transaction it hands out.
type faultPool struct {
	inner projectorpkg.PgxPool
	state *faultState
}

func (p *faultPool) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := p.inner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &faultTx{Tx: tx, state: p.state}, nil
}

func (p *faultPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return p.inner.Acquire(ctx)
}

func (p *faultPool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return p.inner.Exec(ctx, sql, arguments...)
}

func (p *faultPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.inner.Query(ctx, sql, args...)
}

func (p *faultPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.inner.QueryRow(ctx, sql, args...)
}

// scriptedDetachedProjection is a DetachedProjection with a gate, an optional
// per-event hook, per-position failure injection, and an application counter.
type scriptedDetachedProjection struct {
	name     string
	gate     chan struct{}
	entered  chan struct{}
	onHandle func(event store.PersistedEvent)

	mu       sync.Mutex
	failures map[int64]int
	err      error
	failAll  error
	applied  map[int64]int
	attempts int
}

func (p *scriptedDetachedProjection) Name() string { return p.name }

//nolint:gocritic // hugeParam: implements the DetachedProjection contract
func (p *scriptedDetachedProjection) Handle(ctx context.Context, event store.PersistedEvent) error {
	if p.entered != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
	}

	if p.onHandle != nil {
		p.onHandle(event)
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
	p.attempts++

	if p.failAll != nil {
		return p.failAll
	}

	if remaining, ok := p.failures[event.GlobalPosition]; ok && remaining != 0 {
		if remaining > 0 {
			p.failures[event.GlobalPosition] = remaining - 1
		}
		return p.err
	}

	if p.applied == nil {
		p.applied = make(map[int64]int)
	}
	p.applied[event.GlobalPosition]++
	return nil
}

func (p *scriptedDetachedProjection) failAt(position int64, times int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures == nil {
		p.failures = make(map[int64]int)
	}
	p.failures[position] = times
	p.err = err
}

// failEveryEvent makes the handler return err for every event, regardless of
// position.
func (p *scriptedDetachedProjection) failEveryEvent(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failAll = err
}

func (p *scriptedDetachedProjection) timesApplied(position int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.applied[position]
}

func (p *scriptedDetachedProjection) uniqueApplied() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.applied)
}

func (p *scriptedDetachedProjection) Attempts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts
}

// scriptedDetachedBatchProjection is a DetachedBatchProjection that applies its
// batch in order and can fail at a chosen position.
type scriptedDetachedBatchProjection struct {
	name string

	mu         sync.Mutex
	calls      int
	batchSizes []int
	failures   map[int64]int
	err        error
	applied    map[int64]int
}

func (p *scriptedDetachedBatchProjection) Name() string { return p.name }

func (p *scriptedDetachedBatchProjection) Handle(_ context.Context, events []store.PersistedEvent) error {
	p.mu.Lock()
	p.calls++
	p.batchSizes = append(p.batchSizes, len(events))
	p.mu.Unlock()

	for _, event := range events {
		p.mu.Lock()
		if remaining, ok := p.failures[event.GlobalPosition]; ok && remaining != 0 {
			if remaining > 0 {
				p.failures[event.GlobalPosition] = remaining - 1
			}
			err := p.err
			p.mu.Unlock()
			return err
		}
		if p.applied == nil {
			p.applied = make(map[int64]int)
		}
		p.applied[event.GlobalPosition]++
		p.mu.Unlock()
	}

	return nil
}

func (p *scriptedDetachedBatchProjection) failAt(position int64, times int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures == nil {
		p.failures = make(map[int64]int)
	}
	p.failures[position] = times
	p.err = err
}

func (p *scriptedDetachedBatchProjection) timesApplied(position int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.applied[position]
}

func (p *scriptedDetachedBatchProjection) callsCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *scriptedDetachedBatchProjection) batchSizeHistory() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int(nil), p.batchSizes...)
}

func newFaultDaemonHarness(
	t *testing.T,
	realDB *pgxpool.Pool,
	faults *faultState,
	registry *projectorpkg.Registry,
	opts ...projectorpkg.Option,
) *testProjectorHarness {
	t.Helper()

	pool := &faultPool{inner: realDB, state: faults}
	return startTestProjectorWithPool(t, "projector-1", pool, registry, opts...)
}

func TestDetachedProjection_CheckpointWriteFailureReplaysAfterFailure(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	realDB := openTestDBWithMaxConns(t, 8)
	defer realDB.Close()
	faults := &faultState{}
	faults.arm(1, errors.New("checkpoint write failed"))

	projection := &scriptedDetachedProjection{name: "detached-crash", entered: make(chan struct{}, 64)}
	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := newFaultDaemonHarness(t, realDB, faults, registry, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	first := appended[0].GlobalPosition
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		if times := projection.timesApplied(first); times < 2 {
			return fmt.Errorf("first event applied %d times, want at least 2 after the checkpoint failure", times)
		}
		return nil
	})

	harness.stop(t)
}

func TestDetachedProjection_CheckpointSerializationRetryAppliesOnce(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	realDB := openTestDBWithMaxConns(t, 8)
	defer realDB.Close()
	faults := &faultState{}
	faults.arm(1, &pgconn.PgError{Code: "40001", Message: "serialization failure"})

	projection := &scriptedDetachedProjection{name: "detached-serializable", entered: make(chan struct{}, 64)}
	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := newFaultDaemonHarness(t, realDB, faults, registry, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	for _, event := range appended {
		if times := projection.timesApplied(event.GlobalPosition); times != 1 {
			t.Fatalf("event %d applied %d times, want exactly 1 (the checkpoint retry must not repeat the apply)", event.GlobalPosition, times)
		}
	}

	harness.stop(t)
}

func TestDetachedProjection_CheckpointConnectionDropRecovers(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	realDB := openTestDBWithMaxConns(t, 8)
	defer realDB.Close()
	faults := &faultState{}
	// The first checkpoint write terminates its own backend, so the client sees a
	// real connection drop inside the checkpoint transaction.
	faults.armDropConn(1)

	projection := &scriptedDetachedProjection{name: "detached-conn-drop", entered: make(chan struct{}, 64)}
	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := newFaultDaemonHarness(t, realDB, faults, registry, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		checkpoint := getCheckpoint(t, controlDB, projection.Name())
		if checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after recovering from the connection drop", checkpoint, latest)
		}
		return nil
	})

	select {
	case <-harness.done:
		t.Fatalf("daemon exited after a checkpoint connection drop: %v", harness.result)
	default:
	}

	harness.stop(t)
}

func TestDetachedBatchProjection_PartialFailureKeepsCheckpointAndRecovers(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedBatchProjection{name: "detached-batch-partial"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetachedBatch(projection); err != nil {
		t.Fatalf("register detached batch projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 4, "Product")
	latest := appended[len(appended)-1].GlobalPosition
	failing := appended[2].GlobalPosition
	projection.failAt(failing, -1, errors.New("bulk import rejected document"))

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
			return fmt.Errorf("checkpoint=%d want 0 while the batch is failing", checkpoint)
		}
		if times := projection.timesApplied(appended[0].GlobalPosition); times == 0 {
			return errors.New("the applied prefix was not delivered before the failure")
		}
		return nil
	})

	if times := projection.timesApplied(appended[3].GlobalPosition); times != 0 {
		t.Fatalf("event after the failure applied %d times, want 0", times)
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

func TestDetachedProjection_CheckpointMovedDuringApplyDoesNotRegress(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-moved", entered: make(chan struct{}, 64)}

	var once sync.Once
	projection.onHandle = func(store.PersistedEvent) {
		once.Do(func() {
			// Simulate another worker advancing the checkpoint past our target
			// while the batch is being applied externally.
			_, err := controlDB.Exec(context.Background(),
				`UPDATE projection_checkpoints SET last_position = 99, updated_at = NOW() WHERE projection_name = $1`,
				projection.Name())
			if err != nil {
				t.Errorf("advance checkpoint during apply: %v", err)
			}
		})
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appendTestEvents(t, controlDB, eventStore, 2, "Product")

	waitForErr(t, defaultWaitTimeout, func() error {
		checkpoint := getCheckpoint(t, controlDB, projection.Name())
		if checkpoint < 99 {
			return fmt.Errorf("checkpoint=%d, moved value 99 was regressed", checkpoint)
		}
		return nil
	})

	harness.stop(t)
}

func TestDetachedProjection_OwnershipLostBetweenApplyAndCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-ownership", entered: make(chan struct{}, 64)}

	var once sync.Once
	projection.onHandle = func(store.PersistedEvent) {
		once.Do(func() {
			newOwner := uuid.New()
			ctx := context.Background()
			if _, err := controlDB.Exec(ctx,
				`INSERT INTO projector_instances (instance_id, heartbeat_at, created_at, updated_at) VALUES ($1, NOW(), NOW(), NOW())`,
				newOwner); err != nil {
				t.Errorf("insert new owner: %v", err)
				return
			}
			if _, err := controlDB.Exec(ctx,
				`UPDATE projection_assignments SET instance_id = $1, updated_at = NOW() WHERE projection_name = $2`,
				newOwner, projection.Name()); err != nil {
				t.Errorf("reassign projection: %v", err)
			}
		})
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithRebalanceInterval(10*time.Second),
	)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 2, "Product")

	waitForErr(t, defaultWaitTimeout, func() error {
		if projection.timesApplied(appended[0].GlobalPosition) == 0 {
			return errors.New("handler was not applied yet")
		}
		return nil
	})

	time.Sleep(500 * time.Millisecond)

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d, want 0: the old owner must not write a checkpoint after losing ownership", checkpoint)
	}

	harness.stop(t)
}

func TestDetachedProjection_ShutdownDuringApplyDoesNotAdvanceCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	gate := make(chan struct{})
	defer close(gate)
	projection := &scriptedDetachedProjection{name: "detached-shutdown", gate: gate, entered: make(chan struct{}, 8)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appendTestEvents(t, controlDB, eventStore, 2, "Product")

	waitForErr(t, defaultWaitTimeout, func() error {
		select {
		case <-projection.entered:
			return nil
		default:
			return errors.New("detached handler never entered")
		}
	})

	harness.stop(t)

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d, want 0: an interrupted detached apply must not be checkpointed", checkpoint)
	}
}

func TestDetachedProjection_ApplyExceedsBatchTimeoutRecovers(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	gate := make(chan struct{})
	projection := &scriptedDetachedProjection{name: "detached-timeout", gate: gate, entered: make(chan struct{}, 8)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithBatchTimeout(300*time.Millisecond),
	)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 1, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		select {
		case <-projection.entered:
			return nil
		default:
			return errors.New("detached handler never entered")
		}
	})

	time.Sleep(600 * time.Millisecond)

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d want 0 while the apply is blocked past the batch timeout", checkpoint)
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
