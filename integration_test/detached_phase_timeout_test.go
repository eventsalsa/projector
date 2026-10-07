//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpkg "github.com/eventsalsa/projector"
)

// This file covers the phase budgets of a detached projection: the frontier
// probe and the external apply are bounded by BatchTimeout and
// DetachedApplyTimeout, while the post-apply checkpoint phase is bounded by
// CheckpointTimeout rooted in the long-lived processing context.
//
// Matrix coverage:
//
//	Q1/Q2  budgeted apply on both detached shapes
//	Q3/Q7  apply overruns BatchTimeout; the checkpoint context stays live and commits
//	Q4/Q5  DetachedApplyTimeout cancels the apply early; the projection recovers
//	Q8     a blocked checkpoint write is canceled by CheckpointTimeout
//	Q9     a checkpoint blocked on a row lock is bounded and recovers
//	Q10    a stale-gap skip commits despite an overrunning apply
//	Q11    a gap that resolves during an overrunning apply is retried, not skipped
//	Q17    an empty window applies nothing and opens no checkpoint transaction
//
// Q6 (BatchTimeout caps the apply), Q12 (serialization retry), Q13 (ownership
// lost after the apply), Q15 (partial apply), and Q16 (transactional path
// unchanged) are covered by the existing detached and timeout tests.

// setupPhaseTest prepares the isolated schema and returns the control pool. The
// teardown is registered before the daemon starts, so cleanups run in reverse
// and a failing test stops the daemon before the pools close. Closing a pool
// first would block on the daemon's live connections.
func setupPhaseTest(t *testing.T) *pgxpool.Pool {
	t.Helper()

	controlDB := openTestDB(t)
	setupSchema(t, controlDB)
	t.Cleanup(func() {
		cleanupTables(t, controlDB)
		controlDB.Close()
	})

	return controlDB
}

// openPhaseTestPool opens the pool the daemon uses and registers its teardown
// after the schema teardown.
func openPhaseTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool := openTestDBWithMaxConns(t, 8)
	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

// TestDetachedPhase_BudgetedApplyCommitsOnBothShapes is the Q1/Q2 baseline: the
// new phase budgets must not disturb the happy path for either detached shape.
func TestDetachedPhase_BudgetedApplyCommitsOnBothShapes(t *testing.T) {
	controlDB := setupPhaseTest(t)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	perEvent := &scriptedDetachedProjection{
		name:       "detached-phase-per-event",
		entered:    make(chan struct{}, 64),
		applyDelay: 50 * time.Millisecond,
	}
	perBatch := &scriptedDetachedBatchProjection{
		name:       "detached-phase-batch",
		applyDelay: 50 * time.Millisecond,
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(perEvent); err != nil {
		t.Fatalf("register detached per-event projection: %v", err)
	}
	if err := registry.AddDetachedBatch(perBatch); err != nil {
		t.Fatalf("register detached batch projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithBatchTimeout(5*time.Second),
		projectorpkg.WithDetachedApplyTimeout(2*time.Second),
		projectorpkg.WithCheckpointTimeout(time.Second),
	)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{perEvent.Name(), perBatch.Name()} {
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != latest {
				return fmt.Errorf("checkpoint for %s=%d want %d", name, checkpoint, latest)
			}
		}
		return nil
	})

	if applied := perEvent.uniqueApplied(); applied != 3 {
		t.Fatalf("per-event unique applied=%d, want 3", applied)
	}
	if applied := perBatch.timesApplied(latest); applied < 1 {
		t.Fatalf("batch applied the last position %d times, want at least 1", applied)
	}

	harness.stop(t)
}

// TestDetachedPhase_ApplyOverrunsBatchTimeoutCheckpointStillCommits is Q3/Q7.
// The handler deliberately ignores the apply context and sleeps past
// BatchTimeout on every attempt. The checkpoint write afterwards must still
// commit, which proves its budget is measured from the end of the apply rather
// than inherited from the batch deadline.
func TestDetachedPhase_ApplyOverrunsBatchTimeoutCheckpointStillCommits(t *testing.T) {
	controlDB := setupPhaseTest(t)
	realDB := openPhaseTestPool(t)
	faults := &faultState{}

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{
		name: "detached-overrun",
		// Every attempt is slow, so the batch only ever progresses if the
		// checkpoint phase gets a budget of its own.
		entered:    make(chan struct{}, 64),
		applyDelay: 600 * time.Millisecond,
		ignoreCtx:  true,
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithBatchTimeout(300*time.Millisecond),
		// One second, less than the total apply time, so the checkpoint budget
		// only holds if it starts after the apply.
		projectorpkg.WithCheckpointTimeout(time.Second),
	)
	harness := newFaultDaemonHarness(t, realDB, faults, registry, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d while the apply overruns BatchTimeout", checkpoint, latest)
		}
		return nil
	})

	ctxErrs, _, execs, _ := faults.checkpointObservations()
	if execs == 0 {
		t.Fatal("no checkpoint write was observed")
	}
	for index, err := range ctxErrs {
		if err != nil {
			t.Fatalf("checkpoint write %d started with context error %v, want nil after an overrunning apply", index, err)
		}
	}

	harness.stop(t)
}

// TestDetachedPhase_ApplyTimeoutCancelsEarlyAndRecovers is Q4/Q5.
// DetachedApplyTimeout is far shorter than BatchTimeout, so the canceled apply
// and the degraded event must arrive well before the batch cap, and the
// projection must recover once the destination stops being slow.
func TestDetachedPhase_ApplyTimeoutCancelsEarlyAndRecovers(t *testing.T) {
	controlDB := setupPhaseTest(t)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	observer := &testIntegrationObserver{}

	projection := &scriptedDetachedProjection{
		name:          "detached-apply-budget",
		entered:       make(chan struct{}, 64),
		applyDelay:    900 * time.Millisecond,
		delayAttempts: 3,
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	const batchTimeout = 10 * time.Second

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithBatchTimeout(batchTimeout),
		projectorpkg.WithDetachedApplyTimeout(200*time.Millisecond),
		projectorpkg.WithCheckpointTimeout(2*time.Second),
		projectorpkg.WithObserver(observer),
	)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	start := time.Now()
	waitForErr(t, defaultWaitTimeout, func() error {
		degraded := observer.Degraded()
		if len(degraded) == 0 {
			return errors.New("no degraded event yet")
		}
		if !errors.Is(degraded[0].LastError, context.DeadlineExceeded) {
			return fmt.Errorf("degraded error = %v, want context.DeadlineExceeded", degraded[0].LastError)
		}
		return nil
	})

	if elapsed := time.Since(start); elapsed >= batchTimeout/2 {
		t.Fatalf("the first canceled apply took %v, want well below BatchTimeout (%v)", elapsed, batchTimeout)
	}

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after the destination recovered", checkpoint, latest)
		}
		return nil
	})

	if degraded := observer.Degraded(); len(degraded) < 2 {
		t.Fatalf("degraded events=%d, want at least 2 canceled attempts and a retry", len(degraded))
	}

	harness.stop(t)
}

// TestDetachedPhase_CheckpointWriteExceedsCheckpointTimeout is Q8. The
// checkpoint statement blocks far longer than CheckpointTimeout, so each write
// must be canceled and retried rather than running into BatchTimeout.
func TestDetachedPhase_CheckpointWriteExceedsCheckpointTimeout(t *testing.T) {
	controlDB := setupPhaseTest(t)
	realDB := openPhaseTestPool(t)
	faults := &faultState{}
	faults.armCheckpointHang("SELECT pg_sleep(10)")

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-checkpoint-hang", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithBatchTimeout(20*time.Second),
		projectorpkg.WithCheckpointTimeout(300*time.Millisecond),
	)
	harness := newFaultDaemonHarness(t, realDB, faults, registry, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 2, "Product")
	latest := appended[len(appended)-1].GlobalPosition

	// Two canceled writes inside a few seconds prove the write is bounded by
	// CheckpointTimeout and not by the hanging statement or BatchTimeout.
	waitForErr(t, 5*time.Second, func() error {
		_, writeErrs, execs, _ := faults.checkpointObservations()
		if execs < 2 {
			return fmt.Errorf("checkpoint writes=%d, want at least 2 canceled attempts", execs)
		}
		for index, err := range writeErrs {
			if err == nil {
				return fmt.Errorf("checkpoint write %d returned a nil error", index)
			}
		}
		return nil
	})

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d want 0 while the checkpoint write hangs", checkpoint)
	}

	faults.disarmCheckpointHang()

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after the hang was cleared", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

// TestDetachedPhase_CheckpointBlockedByRowLockIsBounded is Q9. A second session
// holds the checkpoint row lock, so the daemon's checkpoint transaction blocks
// on SELECT ... FOR UPDATE. CheckpointTimeout must cancel and retry each attempt
// instead of stalling the projection, and progress must resume on release.
func TestDetachedPhase_CheckpointBlockedByRowLockIsBounded(t *testing.T) {
	controlDB := setupPhaseTest(t)
	realDB := openPhaseTestPool(t)
	faults := &faultState{}

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-checkpoint-lock", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithBatchTimeout(20*time.Second),
		projectorpkg.WithCheckpointTimeout(300*time.Millisecond),
	)
	harness := newFaultDaemonHarness(t, realDB, faults, registry, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 1, "Product")
	firstPosition := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != firstPosition {
			return fmt.Errorf("checkpoint=%d want %d before the lock is taken", checkpoint, firstPosition)
		}
		return nil
	})

	lockPool := openTestDBWithMaxConns(t, 1)
	defer lockPool.Close()

	ctx := context.Background()
	lockConn, err := lockPool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire lock connection: %v", err)
	}
	defer lockConn.Release()

	lockTx, err := lockConn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock transaction: %v", err)
	}

	released := false
	defer func() {
		if !released {
			_ = lockTx.Rollback(ctx)
		}
	}()

	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM projection_checkpoints WHERE projection_name = $1 FOR UPDATE`,
		projection.Name(),
	); err != nil {
		t.Fatalf("lock checkpoint row: %v", err)
	}

	later := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := later[len(later)-1].GlobalPosition

	waitForErr(t, 5*time.Second, func() error {
		_, _, _, attempts := faults.checkpointObservations()
		if attempts < 2 {
			return fmt.Errorf("checkpoint attempts=%d, want at least 2 canceled attempts while the row is locked", attempts)
		}
		return nil
	})

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != firstPosition {
		t.Fatalf("checkpoint=%d want %d while the checkpoint row is locked", checkpoint, firstPosition)
	}

	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release checkpoint lock: %v", err)
	}
	released = true

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after the lock was released", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

// TestDetachedPhase_StaleSkipCommitsDespiteOverrunningApply is Q10. The
// stale-gap skip is recorded and committed on the checkpoint budget even though
// the external apply ignores its context and outlives BatchTimeout.
func TestDetachedPhase_StaleSkipCommitsDespiteOverrunningApply(t *testing.T) {
	controlDB := setupPhaseTest(t)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{
		name:       "detached-stale-overrun",
		entered:    make(chan struct{}, 8),
		applyDelay: time.Second,
		ignoreCtx:  true,
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithBatchTimeout(400*time.Millisecond),
		projectorpkg.WithCheckpointTimeout(2*time.Second),
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
	appendTestEvents(t, controlDB, eventStore, 3, "Product")

	waitForErr(t, defaultWaitTimeout, func() error {
		if skips := getGapSkipRows(t, controlDB, projection.Name()); len(skips) == 0 {
			return errors.New("no stale-gap skip recorded yet")
		}
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint <= 0 {
			return errors.New("checkpoint has not advanced past the stale gap")
		}
		return nil
	})

	held.Commit(t)
	harness.stop(t)
}

// TestDetachedPhase_GapResolvedDuringOverrunningApplyRetries is Q11. The gap
// closes while the overrunning apply is in flight, so the frozen skip must be
// discarded, the batch re-probed, and the resolved position handled.
func TestDetachedPhase_GapResolvedDuringOverrunningApplyRetries(t *testing.T) {
	controlDB := setupPhaseTest(t)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{
		name:       "detached-gap-overrun",
		entered:    make(chan struct{}, 8),
		applyDelay: time.Second,
		ignoreCtx:  true,
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithBatchTimeout(400*time.Millisecond),
		projectorpkg.WithCheckpointTimeout(2*time.Second),
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

	// Gap-blocked batches never reach the applier, so the first entry marks the
	// stale-gap advance whose plan is frozen before the apply starts.
	select {
	case <-projection.entered:
	case <-time.After(defaultWaitTimeout):
		t.Fatal("detached handler was not entered for the stale-gap advance")
	}

	// Resolve the gap while the apply is still running.
	time.Sleep(150 * time.Millisecond)
	held.Commit(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after the gap resolved", checkpoint, latest)
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

// TestDetachedPhase_EmptyWindowSkipsApplyAndCheckpoint is Q17. With no events
// the detached path must not enter the applier or open a checkpoint
// transaction, regardless of how the budgets are set.
func TestDetachedPhase_EmptyWindowSkipsApplyAndCheckpoint(t *testing.T) {
	controlDB := setupPhaseTest(t)

	projection := &scriptedDetachedProjection{name: "detached-empty", entered: make(chan struct{}, 8)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithBatchTimeout(2*time.Second),
		projectorpkg.WithCheckpointTimeout(time.Second),
	)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	time.Sleep(500 * time.Millisecond)

	if attempts := projection.Attempts(); attempts != 0 {
		t.Fatalf("handler attempts=%d, want 0 for an empty window", attempts)
	}
	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
		t.Fatalf("checkpoint=%d, want 0 for an empty window", checkpoint)
	}

	harness.stop(t)
}
