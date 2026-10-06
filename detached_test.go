package projector

import (
	"context"
	"errors"
	"testing"

	"github.com/eventsalsa/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func newDetachedTestDaemon(t *testing.T, state *stubDBState, storeStub *stubProjectorStore) *Daemon {
	t.Helper()

	return &Daemon{
		id:     uuid.MustParse(state.ownerID),
		db:     openStubDB(t, state),
		store:  storeStub,
		config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
	}
}

func TestProcessDetachedBatchCommitsCheckpoint(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	projection := &stubDetachedProjection{name: "detached-orders"}
	entry := registrationForTest(t, projection)
	daemon := newDetachedTestDaemon(t, state, &stubProjectorStore{})

	plan := batchPlan{
		checkpoint:       0,
		targetCheckpoint: 2,
		rows:             []store.PersistedEvent{unitTestEvent(1, "order"), unitTestEvent(2, "order")},
	}

	result, err := daemon.processDetachedBatch(context.Background(), entry, &plan)
	if err != nil {
		t.Fatalf("processDetachedBatch() error = %v", err)
	}
	if !result.progressed || result.checkpoint != 2 {
		t.Fatalf("result = %#v, want progressed checkpoint 2", result)
	}
	if len(projection.handled) != 2 {
		t.Fatalf("handled = %d, want 2", len(projection.handled))
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.commitCalls != 1 {
		t.Fatalf("commitCalls = %d, want 1", state.commitCalls)
	}
}

func TestProcessDetachedBatchApplyErrorSkipsCheckpoint(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	projection := &stubDetachedProjection{name: "detached-orders", err: errors.New("destination down")}
	entry := registrationForTest(t, projection)
	daemon := newDetachedTestDaemon(t, state, &stubProjectorStore{})

	plan := batchPlan{checkpoint: 0, targetCheckpoint: 1, rows: []store.PersistedEvent{unitTestEvent(1, "order")}}

	if _, err := daemon.processDetachedBatch(context.Background(), entry, &plan); err == nil {
		t.Fatal("processDetachedBatch() error = nil, want destination error")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.beginCalls != 0 {
		t.Fatalf("beginCalls = %d, want 0 (no checkpoint transaction when the apply fails)", state.beginCalls)
	}
	if state.commitCalls != 0 {
		t.Fatalf("commitCalls = %d, want 0", state.commitCalls)
	}
}

func TestProcessDetachedBatchCheckpointMoved(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true, checkpointPos: 5}
	projection := &stubDetachedProjection{name: "detached-orders"}
	entry := registrationForTest(t, projection)
	daemon := newDetachedTestDaemon(t, state, &stubProjectorStore{})

	plan := batchPlan{checkpoint: 0, targetCheckpoint: 1, rows: []store.PersistedEvent{unitTestEvent(1, "order")}}

	result, err := daemon.processDetachedBatch(context.Background(), entry, &plan)
	if err != nil {
		t.Fatalf("processDetachedBatch() error = %v", err)
	}
	if result.progressed {
		t.Fatal("progressed = true, want false when the checkpoint moved")
	}
	if !result.blockedByGap || result.checkpoint != 5 {
		t.Fatalf("result = %#v, want blocked at checkpoint 5", result)
	}
	if len(projection.handled) != 1 {
		t.Fatalf("handled = %d, want 1 (apply happens before the checkpoint check)", len(projection.handled))
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.commitCalls != 0 {
		t.Fatalf("commitCalls = %d, want 0", state.commitCalls)
	}
}

func TestProcessDetachedBatchGapResolvedRequestsRetry(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	storeStub := &stubProjectorStore{
		readBatches: [][]store.PersistedEvent{
			{unitTestEvent(2, "order"), unitTestEvent(3, "order")},
			{unitTestEvent(1, "order"), unitTestEvent(2, "order"), unitTestEvent(3, "order")},
		},
	}
	projection := &stubDetachedProjection{name: "detached-orders"}
	entry := registrationForTest(t, projection)
	daemon := newDetachedTestDaemon(t, state, storeStub)

	plan := batchPlan{
		checkpoint:       0,
		gapPosition:      1,
		targetCheckpoint: 3,
		staleSkipped:     true,
		rows:             []store.PersistedEvent{unitTestEvent(2, "order"), unitTestEvent(3, "order")},
	}

	result, err := daemon.processDetachedBatch(context.Background(), entry, &plan)
	if err != nil {
		t.Fatalf("processDetachedBatch() error = %v", err)
	}
	if !result.retryRequested {
		t.Fatalf("retryRequested = false, want true when the gap resolves during apply; result=%#v", result)
	}
	if len(projection.handled) != 2 {
		t.Fatalf("handled = %d, want 2 (frozen skip set applied once)", len(projection.handled))
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.commitCalls != 0 {
		t.Fatalf("commitCalls = %d, want 0", state.commitCalls)
	}
}

func TestProcessDetachedBatchSerializationRetryAppliesOnce(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{
		ownerID:      instanceID.String(),
		ownerValid:   true,
		commitErrors: []error{&pgconn.PgError{Code: "40001"}, nil},
	}
	storeStub := &stubProjectorStore{
		readBatches: [][]store.PersistedEvent{
			{unitTestEvent(2, "order")},
			{unitTestEvent(2, "order")},
		},
	}
	projection := &stubDetachedProjection{name: "detached-orders"}
	entry := registrationForTest(t, projection)
	daemon := newDetachedTestDaemon(t, state, storeStub)

	plan := batchPlan{
		checkpoint:       0,
		gapPosition:      1,
		targetCheckpoint: 2,
		staleSkipped:     true,
		rows:             []store.PersistedEvent{unitTestEvent(2, "order")},
	}

	result, err := daemon.processDetachedBatch(context.Background(), entry, &plan)
	if err != nil {
		t.Fatalf("processDetachedBatch() error = %v", err)
	}
	if !result.progressed {
		t.Fatalf("progressed = false, want true after retry; result=%#v", result)
	}
	if len(projection.handled) != 1 {
		t.Fatalf("handled = %d, want 1 (apply must not repeat on checkpoint serialization retry)", len(projection.handled))
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.commitCalls != 2 {
		t.Fatalf("commitCalls = %d, want 2 (one failed and one successful checkpoint commit)", state.commitCalls)
	}
}
