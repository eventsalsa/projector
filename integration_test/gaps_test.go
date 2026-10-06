//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"

	"github.com/eventsalsa/store"
	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpkg "github.com/eventsalsa/projector"
	projectorpostgres "github.com/eventsalsa/projector/postgres"
)

// slowTxProjection is a transactional projection that holds its batch open for a
// fixed delay before writing, used to exercise graceful shutdown draining.
type slowTxProjection struct {
	name    string
	label   string
	delay   time.Duration
	entered chan struct{}
}

func (p *slowTxProjection) Name() string { return p.name }

//nolint:gocritic // hugeParam: implements the Projection contract
func (p *slowTxProjection) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
	select {
	case p.entered <- struct{}{}:
	default:
	}

	select {
	case <-time.After(p.delay):
	case <-ctx.Done():
		return ctx.Err()
	}

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
		return fmt.Errorf("insert slow projection row: %w", err)
	}

	return nil
}

func TestGap_CheckpointNeverRegresses(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	table := projectorpostgres.DefaultProjectionCheckpointsTable
	const name = "monotonic-projection"

	tx, err := controlDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin save transaction: %v", err)
	}
	if err := projectorpostgres.SaveCheckpoint(ctx, tx, table, name, 10); err != nil {
		t.Fatalf("save checkpoint 10: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit checkpoint 10: %v", err)
	}

	lower, err := controlDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lower transaction: %v", err)
	}
	if err := projectorpostgres.SaveCheckpoint(ctx, lower, table, name, 3); err != nil {
		t.Fatalf("save checkpoint 3: %v", err)
	}
	if err := lower.Commit(ctx); err != nil {
		t.Fatalf("commit checkpoint 3: %v", err)
	}

	if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != 10 {
		t.Fatalf("checkpoint=%d, want 10: a lower position must not regress the checkpoint", checkpoint)
	}
}

func TestGap_HeartbeatRegistrationLossIsFatal(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	projection := newTestProjection("heartbeat-fatal", "projector-1", nil)
	harness := startTestProjector(t, "projector-1", []*testProjection{projection}, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	if _, err := controlDB.Exec(context.Background(),
		`DELETE FROM projector_instances WHERE instance_id = $1`, harness.daemon.ID()); err != nil {
		t.Fatalf("delete registration row: %v", err)
	}

	err := harness.awaitExit(t, defaultWaitTimeout)
	if err == nil {
		t.Fatal("daemon exited without an error after losing its registration")
	}
	if !errors.Is(err, projectorpostgres.ErrInstanceRegistrationMissing) {
		t.Fatalf("fatal error = %v, want ErrInstanceRegistrationMissing", err)
	}
}

func TestGap_MaxConsecutiveFailuresIsFatal(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("always-failing", "projector-1", nil)
	projection.handleErr = errors.New("projection always fails")

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(2))
	harness := startTestProjector(t, "projector-1", []*testProjection{projection}, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	appendTestEvents(t, controlDB, eventStore, 1, "Order")

	err := harness.awaitExit(t, defaultWaitTimeout)
	if err == nil {
		t.Fatal("daemon exited without an error after exceeding max consecutive failures")
	}
	if !errors.Is(err, projectorpkg.ErrConsecutiveFailures) {
		t.Fatalf("fatal error = %v, want ErrConsecutiveFailures", err)
	}
}

func TestGap_GracefulShutdownDrainsInFlightBatch(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &slowTxProjection{
		name:    "slow-drain",
		label:   "projector-1",
		delay:   500 * time.Millisecond,
		entered: make(chan struct{}, 8),
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithShutdownTimeout(5*time.Second))
	harness := startTestProjectorWithProjections(t, "projector-1", []projectorpkg.Projection{projection}, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

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

	harness.stop(t)

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
		t.Fatalf("checkpoint=%d want %d: an in-flight batch should drain within ShutdownTimeout", checkpoint, latest)
	}
	if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 1 {
		t.Fatalf("handled rows=%d, want 1 after draining", len(rows))
	}
}

func TestGap_SequentialStaleSkipsAreAudited(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	const name = "sequential-gaps"

	options := append(defaultProjectorOptions(),
		projectorpkg.WithStaleGapThreshold(250*time.Millisecond),
		projectorpkg.WithStaleGapHarborLag(1),
		projectorpkg.WithPollInterval(500*time.Millisecond),
		projectorpkg.WithMaxPollInterval(500*time.Millisecond),
		projectorpkg.WithBatchPause(500*time.Millisecond),
	)

	projector := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection(name, "projector-1", nil),
	}, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	held1 := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Shipment", Count: 1})
	appendTestEvents(t, controlDB, eventStore, 4, "Shipment")

	waitForErr(t, defaultWaitTimeout, func() error {
		if skips := getGapSkipRows(t, controlDB, name); len(skips) != 1 {
			return fmt.Errorf("gap skips=%d, want 1", len(skips))
		}
		return nil
	})

	held1.Rollback(t)

	held2 := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Shipment", Count: 1})
	appendTestEvents(t, controlDB, eventStore, 4, "Shipment")

	waitForErr(t, defaultWaitTimeout, func() error {
		if skips := getGapSkipRows(t, controlDB, name); len(skips) != 2 {
			return fmt.Errorf("gap skips=%d, want 2", len(skips))
		}
		return nil
	})

	projector.stop(t)
	held2.Rollback(t)

	skips := getGapSkipRows(t, controlDB, name)
	if len(skips) != 2 {
		t.Fatalf("gap skips=%d, want 2", len(skips))
	}
	if skips[0].GapPosition == skips[1].GapPosition {
		t.Fatalf("both skip rows report gap position %d", skips[0].GapPosition)
	}
}

func TestGap_RealSerializationFailureIsRetried(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	const name = "serialization-recovery"
	ctx := context.Background()

	if _, err := controlDB.Exec(ctx, `
INSERT INTO projection_checkpoints (projection_name, last_position, created_at, updated_at)
VALUES ($1, 0, NOW(), NOW())
ON CONFLICT (projection_name) DO NOTHING`, name); err != nil {
		t.Fatalf("seed checkpoint row: %v", err)
	}

	// A sequence-backed trigger raises a genuine SQLSTATE 40001 from the server on
	// the first checkpoint update, then lets later updates through. Sequences are
	// not rolled back, so the fault fires exactly once.
	statements := []string{
		`CREATE SEQUENCE IF NOT EXISTS cp_fault_seq`,
		`CREATE OR REPLACE FUNCTION cp_fault_40001() RETURNS trigger AS $$
BEGIN
	IF nextval('cp_fault_seq') <= 1 THEN
		RAISE EXCEPTION 'serialization failure' USING ERRCODE = '40001';
	END IF;
	RETURN NEW;
END $$ LANGUAGE plpgsql`,
		`CREATE TRIGGER cp_fault_trigger BEFORE UPDATE ON projection_checkpoints
FOR EACH ROW EXECUTE FUNCTION cp_fault_40001()`,
	}
	for _, statement := range statements {
		if _, err := controlDB.Exec(ctx, statement); err != nil {
			t.Fatalf("install fault trigger: %v", err)
		}
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection(name, "projector-1", nil),
	}, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	appended := appendTestEvents(t, controlDB, eventStore, 2, "Order")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d after the serialization failure", checkpoint, latest)
		}
		return nil
	})

	var fired int64
	if err := controlDB.QueryRow(ctx, `SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM cp_fault_seq`).Scan(&fired); err != nil {
		t.Fatalf("read fault sequence: %v", err)
	}
	if fired < 1 {
		t.Fatalf("fault sequence fired %d times, want at least 1", fired)
	}

	harness.stop(t)
}

func TestGap_ErrgroupPropagatesFatalToSibling(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	sharedDB := openTestDB(t)
	defer sharedDB.Close()

	projection1 := newTestProjection("errgroup-1", "daemon-1", nil)
	projection2 := newTestProjection("errgroup-2", "daemon-2", nil)

	daemon1 := projectorpkg.New(sharedDB, eventStore,
		projectorpkg.FromProjections(projection1), defaultProjectorOptions()...)
	daemon2 := projectorpkg.New(sharedDB, eventStore,
		projectorpkg.FromProjections(projection2), defaultProjectorOptions()...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return daemon1.Start(gCtx) })
	g.Go(func() error { return daemon2.Start(gCtx) })

	waitForErr(t, defaultWaitTimeout, func() error {
		var count int
		if err := controlDB.QueryRow(context.Background(), `SELECT COUNT(*) FROM projector_instances`).Scan(&count); err != nil {
			return err
		}
		if count != 2 {
			return fmt.Errorf("registered instances=%d, want 2", count)
		}
		return nil
	})

	if _, err := controlDB.Exec(context.Background(),
		`DELETE FROM projector_instances WHERE instance_id = $1`, daemon1.ID()); err != nil {
		t.Fatalf("delete daemon-1 registration: %v", err)
	}

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- g.Wait()
	}()

	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("errgroup.Wait() returned nil, want the fatal error from daemon-1")
		}
	case <-time.After(defaultWaitTimeout):
		t.Fatal("timeout waiting for errgroup.Wait() to return after a sibling fatal")
	}
}
