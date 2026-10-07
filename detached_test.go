package projector

import (
	"context"
	"errors"
	"testing"
	"time"

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

	result, err := daemon.processDetachedBatch(context.Background(), context.Background(), entry, &plan)
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

	if _, err := daemon.processDetachedBatch(context.Background(), context.Background(), entry, &plan); err == nil {
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

	result, err := daemon.processDetachedBatch(context.Background(), context.Background(), entry, &plan)
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

	result, err := daemon.processDetachedBatch(context.Background(), context.Background(), entry, &plan)
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

// TestDetachedApplyContextIsCappedByBatchDeadline pins the min() rule: a
// generous apply timeout still cannot outlive the batch deadline.
func TestDetachedApplyContextIsCappedByBatchDeadline(t *testing.T) {
	daemon := &Daemon{config: Config{DetachedApplyTimeout: time.Hour}}

	base, cancelBase := context.WithTimeout(context.Background(), time.Second)
	defer cancelBase()

	ctx, cancel := daemon.detachedApplyContext(base)
	defer cancel()

	baseDeadline, ok := base.Deadline()
	if !ok {
		t.Fatal("base context has no deadline")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("apply context has no deadline")
	}
	if deadline.After(baseDeadline) {
		t.Fatalf("apply deadline = %v, want at or before the batch deadline %v", deadline, baseDeadline)
	}
}

func TestPhaseContextsReturnBaseWithoutTimeout(t *testing.T) {
	daemon := &Daemon{config: Config{}}
	base := context.Background()

	applyCtx, cancelApply := daemon.detachedApplyContext(base)
	defer cancelApply()
	if applyCtx != base {
		t.Fatal("detachedApplyContext() = new context, want the base context when no timeout is configured")
	}

	checkpointCtx, cancelCheckpoint := daemon.checkpointContext(base)
	defer cancelCheckpoint()
	if checkpointCtx != base {
		t.Fatal("checkpointContext() = new context, want the base context when no timeout is configured")
	}
}

// TestProcessDetachedBatchCheckpointContextOutlivesBatchContext models a
// detached handler that consumes the whole batch budget: the batch context is
// already canceled when the apply returns, yet the checkpoint still commits
// because it runs on its own budget rooted in the processing context.
func TestProcessDetachedBatchCheckpointContextOutlivesBatchContext(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	projection := &stubDetachedProjection{name: "detached-orders"}
	entry := registrationForTest(t, projection)
	daemon := newDetachedTestDaemon(t, state, &stubProjectorStore{})

	batchCtx, cancelBatch := context.WithCancel(context.Background())
	cancelBatch()

	plan := batchPlan{
		checkpoint:       0,
		targetCheckpoint: 1,
		rows:             []store.PersistedEvent{unitTestEvent(1, "order")},
	}

	result, err := daemon.processDetachedBatch(context.Background(), batchCtx, entry, &plan)
	if err != nil {
		t.Fatalf("processDetachedBatch() error = %v, want a committed checkpoint on an independent context", err)
	}
	if !result.progressed || result.checkpoint != 1 {
		t.Fatalf("result = %#v, want progressed checkpoint 1", result)
	}
	if !errors.Is(projection.ctxErr, context.Canceled) {
		t.Fatalf("apply context error = %v, want context.Canceled (apply is bounded by the batch context)", projection.ctxErr)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.beginCtxErrs) != 1 {
		t.Fatalf("checkpoint BeginTx calls = %d, want 1", len(state.beginCtxErrs))
	}
	if err := state.beginCtxErrs[0]; err != nil {
		t.Fatalf("checkpoint context error = %v, want nil (checkpoint must not inherit the batch deadline)", err)
	}
}

// TestProcessDetachedBatchCheckpointContextFollowsParent proves the checkpoint
// phase is rooted in the processing context by canceling that context.
func TestProcessDetachedBatchCheckpointContextFollowsParent(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	projection := &stubDetachedProjection{name: "detached-orders"}
	entry := registrationForTest(t, projection)
	daemon := newDetachedTestDaemon(t, state, &stubProjectorStore{})

	parentCtx, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	plan := batchPlan{
		checkpoint:       0,
		targetCheckpoint: 1,
		rows:             []store.PersistedEvent{unitTestEvent(1, "order")},
	}

	if _, err := daemon.processDetachedBatch(parentCtx, context.Background(), entry, &plan); err != nil {
		t.Fatalf("processDetachedBatch() error = %v", err)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.beginCtxErrs) != 1 {
		t.Fatalf("checkpoint BeginTx calls = %d, want 1", len(state.beginCtxErrs))
	}
	if !errors.Is(state.beginCtxErrs[0], context.Canceled) {
		t.Fatalf("checkpoint context error = %v, want context.Canceled", state.beginCtxErrs[0])
	}
}

// TestProcessDetachedBatchOwnershipLostAfterApply guards the post-apply
// checkpoint path: ownership lost during the checkpoint must still surface as
// errProjectionOwnershipLost so the loop stops the projection.
func TestProcessDetachedBatchOwnershipLostAfterApply(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: false}
	projection := &stubDetachedProjection{name: "detached-orders"}
	entry := registrationForTest(t, projection)
	daemon := newDetachedTestDaemon(t, state, &stubProjectorStore{})

	plan := batchPlan{
		checkpoint:       0,
		targetCheckpoint: 1,
		rows:             []store.PersistedEvent{unitTestEvent(1, "order")},
	}

	_, err := daemon.processDetachedBatch(context.Background(), context.Background(), entry, &plan)
	if !errors.Is(err, errProjectionOwnershipLost) {
		t.Fatalf("error = %v, want errProjectionOwnershipLost", err)
	}
	if len(projection.handled) != 1 {
		t.Fatalf("handled = %d, want 1 (the apply completes before ownership is checked)", len(projection.handled))
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

	result, err := daemon.processDetachedBatch(context.Background(), context.Background(), entry, &plan)
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
