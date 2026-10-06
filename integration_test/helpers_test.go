//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/eventsalsa/store"
	storemigrations "github.com/eventsalsa/store/migrations"
	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpkg "github.com/eventsalsa/projector"
	projectormigrations "github.com/eventsalsa/projector/migrations"
	projectorpostgres "github.com/eventsalsa/projector/postgres"
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	if os.Getenv("TESTCONTAINERS") == "false" {
		os.Exit(m.Run())
	}

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("eventsalsa_projector_test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start postgres testcontainer: %v\n", err)
		os.Exit(1)
	}

	host, err := pgContainer.Host(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get postgres container host: %v\n", err)
		_ = pgContainer.Terminate(ctx)
		os.Exit(1)
	}

	port, err := pgContainer.MappedPort(ctx, "5432/tcp")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get postgres container port: %v\n", err)
		_ = pgContainer.Terminate(ctx)
		os.Exit(1)
	}

	_ = os.Setenv("POSTGRES_HOST", host)
	_ = os.Setenv("POSTGRES_PORT", port.Port())
	_ = os.Setenv("POSTGRES_USER", "postgres")
	_ = os.Setenv("POSTGRES_PASSWORD", "postgres")
	_ = os.Setenv("POSTGRES_DB", "eventsalsa_projector_test")

	code := m.Run()

	if err := pgContainer.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate postgres testcontainer: %v\n", err)
	}

	os.Exit(code)
}

const (
	testWaitInterval         = 50 * time.Millisecond
	defaultWaitTimeout       = 12 * time.Second
	projectorShutdownTimeout = 10 * time.Second
)

type testEventBatch struct {
	StreamType string
	StreamID   string
	Count      int
}

type controlledAppend struct {
	tx     pgx.Tx
	events []store.PersistedEvent
}

type failurePlan struct {
	err       error
	remaining int
}

type testProjection struct {
	name          string
	instanceLabel string
	streamTypes   []string
	handleErr     error

	mu              sync.Mutex
	processedEvents []store.PersistedEvent
	attempts        map[int64]int
	failures        map[int64]failurePlan
}

type testProjectionEventRow struct {
	ProjectionName string
	GlobalPosition int64
	StreamType     string
	StreamID       string
	EventType      string
	HandledBy      string
	AttemptNo      int
}

type gapSkipRow struct {
	ProjectionName         string
	GapPosition            int64
	SkipToPosition         int64
	HighestVisiblePosition int64
}

type testProjectorHarness struct {
	label    string
	db       *pgxpool.Pool
	daemon   *projectorpkg.Daemon
	cancel      context.CancelFunc
	done        chan struct{}
	result      error
	exitChecked bool
	stopOnce    sync.Once
}

func openTestDB(t testing.TB) *pgxpool.Pool {
	t.Helper()

	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "postgres"
	}

	dbName := os.Getenv("POSTGRES_DB")
	if dbName == "" {
		dbName = "eventsalsa_projector_test"
	}

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user,
		password,
		host,
		port,
		dbName,
	)

	ctx := context.Background()
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("parse test db config: %v", err)
	}

	config.MaxConns = 8

	if os.Getenv("PGX_TEST_SIMPLE_PROTOCOL") == "true" {
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}
	switch os.Getenv("PGX_TEST_QUERY_EXEC_MODE") {
	case "cache_statement":
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	case "cache_describe":
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	case "describe_exec":
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	case "exec":
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	case "simple_protocol":
		config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect to test db: %v", err)
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()

	if err := db.Ping(pingCtx); err != nil {
		db.Close()
		t.Fatalf("ping test db: %v", err)
	}

	return db
}

func setupSchema(t testing.TB, db *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.Exec(ctx, `
DROP TABLE IF EXISTS test_projection_events CASCADE;
DROP TABLE IF EXISTS projector_leader_leases CASCADE;
DROP TABLE IF EXISTS projection_gap_skips CASCADE;
DROP TABLE IF EXISTS projection_checkpoints CASCADE;
DROP TABLE IF EXISTS projection_assignments CASCADE;
DROP TABLE IF EXISTS projector_instances CASCADE;
DROP TABLE IF EXISTS stream_heads CASCADE;
DROP TABLE IF EXISTS events CASCADE;
`)
	if err != nil {
		t.Fatalf("drop existing schema: %v", err)
	}

	tmpDir := t.TempDir()
	storeSQL := generateStoreSQL(t, tmpDir)

	if _, err := db.Exec(ctx, string(storeSQL)); err != nil {
		t.Fatalf("execute store migration: %v", err)
	}

	projectorSQL := generateProjectorSQL(t, tmpDir)

	if _, err := db.Exec(ctx, string(projectorSQL)); err != nil {
		t.Fatalf("execute projector migration: %v", err)
	}

	_, err = db.Exec(ctx, `
CREATE TABLE test_projection_events (
projection_name TEXT NOT NULL,
global_position BIGINT NOT NULL,
stream_type TEXT NOT NULL,
stream_id TEXT NOT NULL,
event_type TEXT NOT NULL,
handled_by TEXT NOT NULL,
attempt_no INT NOT NULL,
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
PRIMARY KEY (projection_name, global_position)
)
`)
	if err != nil {
		t.Fatalf("create test read model table: %v", err)
	}
}

func cleanupTables(t testing.TB, db *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := db.Exec(ctx, `
TRUNCATE TABLE
test_projection_events,
projector_leader_leases,
projection_gap_skips,
projection_checkpoints,
projection_assignments,
projector_instances,
stream_heads,
events
RESTART IDENTITY CASCADE
`); err != nil {
		t.Fatalf("cleanup tables: %v", err)
	}
}

func appendTestEvents(t testing.TB, db *pgxpool.Pool, eventStore *storepostgres.Store, count int, streamType string) []store.PersistedEvent {
	t.Helper()

	return appendTestEventBatches(t, db, eventStore, testEventBatch{
		StreamType: streamType,
		Count:      count,
	})
}

func appendTestEventBatches(t testing.TB, db *pgxpool.Pool, eventStore *storepostgres.Store, batches ...testEventBatch) []store.PersistedEvent {
	t.Helper()

	ctx := context.Background()
	appended := make([]store.PersistedEvent, 0)

	for _, batch := range batches {
		if batch.Count <= 0 {
			continue
		}

		streamID := batch.StreamID
		if streamID == "" {
			streamID = uuid.NewString()
		}

		events := make([]store.Event, 0, batch.Count)
		for idx := range batch.Count {
			events = append(events, store.Event{
				StreamType:   batch.StreamType,
				StreamID:     streamID,
				EventID:      uuid.New(),
				EventType:    fmt.Sprintf("%s.event.%d", batch.StreamType, idx+1),
				EventVersion: 1,
				Payload:      []byte(fmt.Sprintf(`{"stream_type":%q,"event_number":%d}`, batch.StreamType, idx+1)),
				Metadata:     []byte(`{}`),
				CreatedAt:    time.Now().UTC(),
			})
		}

		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin append transaction: %v", err)
		}

		result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("append test events: %v", err)
		}

		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit append transaction: %v", err)
		}

		appended = append(appended, result.Events...)
	}

	sort.Slice(appended, func(i, j int) bool {
		return appended[i].GlobalPosition < appended[j].GlobalPosition
	})

	return appended
}

func beginControlledAppend(t testing.TB, db *pgxpool.Pool, eventStore *storepostgres.Store, batch testEventBatch) *controlledAppend {
	t.Helper()

	ctx := context.Background()
	streamID := batch.StreamID
	if streamID == "" {
		streamID = uuid.NewString()
	}

	events := make([]store.Event, 0, batch.Count)
	for idx := range batch.Count {
		events = append(events, store.Event{
			StreamType:   batch.StreamType,
			StreamID:     streamID,
			EventID:      uuid.New(),
			EventType:    fmt.Sprintf("%s.event.%d", batch.StreamType, idx+1),
			EventVersion: 1,
			Payload:      []byte(fmt.Sprintf(`{"stream_type":%q,"event_number":%d}`, batch.StreamType, idx+1)),
			Metadata:     []byte(`{}`),
			CreatedAt:    time.Now().UTC(),
		})
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin controlled append transaction: %v", err)
	}

	result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("controlled append: %v", err)
	}

	return &controlledAppend{
		tx:     tx,
		events: append([]store.PersistedEvent(nil), result.Events...),
	}
}

func (c *controlledAppend) Commit(t testing.TB) {
	t.Helper()

	if err := c.tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit controlled append transaction: %v", err)
	}
}

func (c *controlledAppend) Rollback(t testing.TB) {
	t.Helper()

	if err := c.tx.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback controlled append transaction: %v", err)
	}
}

func waitFor(t testing.TB, timeout time.Duration, condition func() bool, msg string) {
	t.Helper()

	waitForErr(t, timeout, func() error {
		if condition() {
			return nil
		}
		return errors.New(msg)
	})
}

func waitForErr(t testing.TB, timeout time.Duration, fn func() error) {
	t.Helper()

	if timeout <= 0 {
		timeout = defaultWaitTimeout
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		lastErr = fn()
		if lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s: %v", timeout, lastErr)
		}
		time.Sleep(testWaitInterval)
	}
}

func newTestProjection(name, instanceLabel string, streamTypes []string) *testProjection {
	return &testProjection{
		name:          name,
		instanceLabel: instanceLabel,
		streamTypes:   append([]string(nil), streamTypes...),
		attempts:      make(map[int64]int),
		failures:      make(map[int64]failurePlan),
	}
}

func (c *testProjection) Name() string {
	return c.name
}

func (c *testProjection) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
	c.mu.Lock()
	c.attempts[event.GlobalPosition]++
	attemptNo := c.attempts[event.GlobalPosition]
	handleErr := c.handleErr
	plan, shouldFail := c.failures[event.GlobalPosition]
	if shouldFail {
		if plan.remaining > 0 {
			plan.remaining--
		}
		if plan.remaining == 0 {
			delete(c.failures, event.GlobalPosition)
		} else {
			c.failures[event.GlobalPosition] = plan
		}
	}
	c.mu.Unlock()

	if handleErr != nil {
		return handleErr
	}

	if shouldFail {
		return plan.err
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
`, c.name, event.GlobalPosition, event.StreamType, event.StreamID, event.EventType, c.instanceLabel, attemptNo)
	if err != nil {
		return fmt.Errorf("insert test projection row: %w", err)
	}

	c.mu.Lock()
	c.processedEvents = append(c.processedEvents, event)
	c.mu.Unlock()

	return nil
}

func (c *testProjection) FailTimes(position int64, attempts int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failures[position] = failurePlan{err: err, remaining: attempts}
}

func (c *testProjection) FailUntilCleared(position int64, err error) {
	c.FailTimes(position, -1, err)
}

func (c *testProjection) ClearFailure(position int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.failures, position)
}

func (c *testProjection) AttemptCount(position int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.attempts[position]
}

func (c *testProjection) ProcessedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.processedEvents)
}

func startTestProjector(t *testing.T, label string, projections []*testProjection, opts ...projectorpkg.Option) *testProjectorHarness {
	t.Helper()

	projectionList := make([]projectorpkg.Projection, 0, len(projections))
	for _, p := range projections {
		if len(p.streamTypes) > 0 {
			projectionList = append(projectionList, projectorpkg.FilterStreamTypes(p, p.streamTypes...))
		} else {
			projectionList = append(projectionList, p)
		}
	}

	return startTestProjectorWithProjections(t, label, projectionList, opts...)
}

func startTestProjectorWithProjections(t *testing.T, label string, projections []projectorpkg.Projection, opts ...projectorpkg.Option) *testProjectorHarness {
	t.Helper()

	return startTestProjectorWithTables(t, label, projections, projectorpostgres.DefaultProjectorInstancesTable, opts...)
}

func startTestProjectorWithTables(
	t *testing.T,
	label string,
	projections []projectorpkg.Projection,
	instancesTable string,
	opts ...projectorpkg.Option,
) *testProjectorHarness {
	t.Helper()

	db := openTestDB(t)
	t.Cleanup(func() {
		db.Close()
	})
	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	daemon := projectorpkg.New(db, eventStore, projections, opts...)
	ctx, cancel := context.WithCancel(context.Background())
	harness := &testProjectorHarness{
		label:  label,
		db:     db,
		daemon: daemon,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go func() {
		harness.result = daemon.Start(ctx)
		close(harness.done)
	}()

	t.Cleanup(func() {
		harness.stop(t)
	})

	waitForErr(t, defaultWaitTimeout, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var count int
		err := db.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE instance_id = $1`, instancesTable), daemon.ID()).Scan(&count)
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("projector %s not registered yet", label)
		}
		return nil
	})

	return harness
}

func (h *testProjectorHarness) stop(tb testing.TB) {
	tb.Helper()

	h.stopOnce.Do(func() {
		h.daemon.Stop()
		h.cancel()

		select {
		case <-h.done:
			if h.result != nil && !h.exitChecked &&
				!errors.Is(h.result, context.Canceled) &&
				!errors.Is(h.result, projectorpostgres.ErrInstanceRegistrationMissing) {
				tb.Fatalf("projector %s stopped with error: %v", h.label, h.result)
			}
		case <-time.After(projectorShutdownTimeout):
			tb.Fatalf("timeout waiting for projector %s to stop", h.label)
		}
	})
}

// awaitExit waits for the daemon to return on its own and hands back the error,
// so tests can assert fatal shutdown paths.
func (h *testProjectorHarness) awaitExit(tb testing.TB, timeout time.Duration) error {
	tb.Helper()

	h.exitChecked = true

	select {
	case <-h.done:
		return h.result
	case <-time.After(timeout):
		tb.Fatalf("timeout waiting for projector %s to exit", h.label)
		return nil
	}
}

func defaultProjectorOptions() []projectorpkg.Option {
	return []projectorpkg.Option{
		projectorpkg.WithBatchSize(50),
		projectorpkg.WithPollInterval(50 * time.Millisecond),
		projectorpkg.WithMaxPollInterval(200 * time.Millisecond),
		projectorpkg.WithDispatcherInterval(50 * time.Millisecond),
		projectorpkg.WithHeartbeatInterval(100 * time.Millisecond),
		projectorpkg.WithHeartbeatTimeout(500 * time.Millisecond),
		projectorpkg.WithRebalanceInterval(200 * time.Millisecond),
		projectorpkg.WithBatchPause(10 * time.Millisecond),
	}
}

func latestGlobalPosition(t testing.TB, db *pgxpool.Pool, eventStore *storepostgres.Store) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("begin latest position tx: %v", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	position, err := eventStore.GetLatestGlobalPosition(ctx, tx)
	if err != nil {
		t.Fatalf("get latest global position: %v", err)
	}

	return position
}

func getCheckpoint(t testing.TB, db *pgxpool.Pool, projectionName string) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	position, err := projectorpostgres.GetCheckpoint(ctx, db, projectorpostgres.DefaultProjectionCheckpointsTable, projectionName)
	if err != nil {
		t.Fatalf("get checkpoint for %s: %v", projectionName, err)
	}

	return position
}

func getAssignments(t testing.TB, db *pgxpool.Pool) []projectorpostgres.ProjectionAssignment {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	assignments, err := projectorpostgres.GetAssignments(ctx, db, projectorpostgres.DefaultProjectionAssignmentsTable)
	if err != nil {
		t.Fatalf("get assignments: %v", err)
	}

	return assignments
}

func assignedProjectionCounts(assignments []projectorpostgres.ProjectionAssignment) map[uuid.UUID]int {
	counts := make(map[uuid.UUID]int)
	for _, assignment := range assignments {
		if assignment.Assigned {
			counts[assignment.InstanceID]++
		}
	}
	return counts
}

func getHandledRows(t testing.TB, db *pgxpool.Pool, projectionName string) []testProjectionEventRow {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rows, err := db.Query(ctx, `
SELECT projection_name, global_position, stream_type, stream_id, event_type, handled_by, attempt_no
FROM test_projection_events
WHERE projection_name = $1
ORDER BY global_position ASC
`, projectionName)
	if err != nil {
		t.Fatalf("query handled rows for %s: %v", projectionName, err)
	}
	defer rows.Close()

	result := make([]testProjectionEventRow, 0)
	for rows.Next() {
		var row testProjectionEventRow
		if err := rows.Scan(
			&row.ProjectionName,
			&row.GlobalPosition,
			&row.StreamType,
			&row.StreamID,
			&row.EventType,
			&row.HandledBy,
			&row.AttemptNo,
		); err != nil {
			t.Fatalf("scan handled row for %s: %v", projectionName, err)
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate handled rows for %s: %v", projectionName, err)
	}

	return result
}

func getGapSkipRows(t testing.TB, db *pgxpool.Pool, projectionName string) []gapSkipRow {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rows, err := db.Query(ctx, `
SELECT projection_name, gap_position, skip_to_position, highest_visible_position
FROM projection_gap_skips
WHERE projection_name = $1
ORDER BY id ASC
`, projectionName)
	if err != nil {
		t.Fatalf("query gap skip rows for %s: %v", projectionName, err)
	}
	defer rows.Close()

	result := make([]gapSkipRow, 0)
	for rows.Next() {
		var row gapSkipRow
		if err := rows.Scan(
			&row.ProjectionName,
			&row.GapPosition,
			&row.SkipToPosition,
			&row.HighestVisiblePosition,
		); err != nil {
			t.Fatalf("scan gap skip row for %s: %v", projectionName, err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate gap skip rows for %s: %v", projectionName, err)
	}

	return result
}

func handledByAfter(t testing.TB, db *pgxpool.Pool, cutoff int64) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rows, err := db.Query(ctx, `
SELECT DISTINCT handled_by
FROM test_projection_events
WHERE global_position > $1
ORDER BY handled_by ASC
`, cutoff)
	if err != nil {
		t.Fatalf("query handled_by after %d: %v", cutoff, err)
	}
	defer rows.Close()

	labels := make([]string, 0)
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			t.Fatalf("scan handled_by label: %v", err)
		}
		labels = append(labels, label)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate handled_by labels: %v", err)
	}

	return labels
}

func countProjectorRows(t testing.TB, db *pgxpool.Pool) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM projector_instances`).Scan(&count); err != nil {
		t.Fatalf("count projector rows: %v", err)
	}

	return count
}

func projectorRowExists(t testing.TB, db *pgxpool.Pool, instanceID uuid.UUID) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM projector_instances WHERE instance_id = $1`, instanceID).Scan(&count); err != nil {
		t.Fatalf("check instance row %s: %v", instanceID, err)
	}

	return count == 1
}

func insertProjectorRow(t testing.TB, db *pgxpool.Pool, instanceID uuid.UUID, heartbeatAt time.Time) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := db.Exec(ctx, `
INSERT INTO projector_instances (instance_id, heartbeat_at, created_at, updated_at)
VALUES ($1, $2, NOW(), NOW())
`, instanceID, heartbeatAt)
	if err != nil {
		t.Fatalf("insert instance row %s: %v", instanceID, err)
	}
}

func assignProjectionToInstance(t testing.TB, db *pgxpool.Pool, projectionName string, instanceID uuid.UUID) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := db.Exec(ctx, `
UPDATE projection_assignments
SET instance_id = $1, updated_at = NOW()
WHERE projection_name = $2
`, instanceID, projectionName)
	if err != nil {
		t.Fatalf("assign projection %s to instance %s: %v", projectionName, instanceID, err)
	}
}

func generateStoreSQL(t testing.TB, outputDir string) []byte {
	t.Helper()

	config := &storemigrations.Config{
		OutputFolder:     outputDir,
		OutputFilename:   "store.sql",
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
	}
	if err := storemigrations.GeneratePostgres(config); err != nil {
		t.Fatalf("generate store migration: %v", err)
	}

	sqlBytes, err := os.ReadFile(fmt.Sprintf("%s/%s", outputDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("read store migration: %v", err)
	}

	return sqlBytes
}

func generateProjectorSQL(t testing.TB, outputDir string) []byte {
	t.Helper()

	config := &projectormigrations.Config{
		OutputFolder:               outputDir,
		OutputFilename:             "projector.sql",
		ProjectorInstancesTable:    projectorpostgres.DefaultProjectorInstancesTable,
		ProjectionAssignmentsTable: projectorpostgres.DefaultProjectionAssignmentsTable,
		ProjectionCheckpointsTable: projectorpostgres.DefaultProjectionCheckpointsTable,
		ProjectionGapSkipsTable:    projectorpostgres.DefaultProjectionGapSkipsTable,
		ProjectorLeaderLeasesTable: projectorpostgres.DefaultProjectorLeaderLeasesTable,
	}
	if err := projectormigrations.GeneratePostgres(config); err != nil {
		t.Fatalf("generate projector migration: %v", err)
	}

	sqlBytes, err := os.ReadFile(fmt.Sprintf("%s/%s", outputDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("read projector migration: %v", err)
	}

	return sqlBytes
}
