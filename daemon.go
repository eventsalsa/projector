package projector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/eventsalsa/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/eventsalsa/projector/dispatcher"
	"github.com/eventsalsa/projector/failure"
	"github.com/eventsalsa/projector/postgres"
)

const (
	defaultAssignmentPollInterval = 2 * time.Second
	defaultShutdownTimeout        = 5 * time.Second
	missingRegistrationRetryLimit = 2
	startupCleanupMultiplier      = 2
	staleGapRetryLimit            = 3
)

var (
	// ErrNilDB indicates that a daemon was created without a database handle.
	ErrNilDB = errors.New("daemon requires a database handle")

	// ErrNilStore indicates that a daemon was created without an event store.
	ErrNilStore = errors.New("daemon requires an event store")

	// ErrNilDispatcher indicates that a daemon was created without a dispatcher.
	ErrNilDispatcher = errors.New("daemon requires a dispatcher")

	// ErrAlreadyStarted indicates that Start was called while the daemon is already running.
	ErrAlreadyStarted = errors.New("daemon already started")

	// ErrMissingNotifyConnectionString indicates that the notify dispatcher was selected without a connection string.
	ErrMissingNotifyConnectionString = errors.New("daemon notify dispatcher requires a connection string")

	// ErrMissingNotifyChannel indicates that the notify dispatcher was selected without a channel name.
	ErrMissingNotifyChannel = errors.New("daemon notify dispatcher requires a channel name")

	// ErrConsecutiveFailures indicates that a projection exceeded the maximum allowed
	// consecutive batch failures, signaling an infrastructure-level problem.
	ErrConsecutiveFailures = errors.New("projection exceeded max consecutive failures")

	errProjectionOwnershipLost = errors.New("projection is no longer assigned to this instance")
)

// projectorStore captures the event store behavior needed by Daemon.
type projectorStore interface {
	dispatcher.PositionQuerier
	store.EventReader
}

// PgxPool abstracts the database connection pool capabilities required by the daemon.
type PgxPool interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Daemon orchestrates distributed projection execution for a single projector instance.
type Daemon struct { //nolint:govet // fieldalignment: readability over marginal memory savings
	config             Config
	dispatcher         dispatcher.Dispatcher
	store              projectorStore
	cancel             context.CancelFunc
	processingCancel   context.CancelFunc
	fatalErrCh         chan error
	runningProjections map[string]context.CancelFunc
	projectionDone     map[string]chan struct{}
	failedProjections  map[string]error
	db                 PgxPool
	leaderConn         *pgxpool.Conn
	leaderDone         chan struct{}
	registry           *Registry
	registrations      []registration
	registrationByName map[string]registration
	wg                 sync.WaitGroup
	mu                 sync.Mutex
	id                 uuid.UUID
	lastLeaderSuccess  time.Time
	isLeader           bool
	started            bool
}

type processedBatch struct {
	checkpoint     int64
	handledCount   int
	progressed     bool
	blockedByGap   bool
	fullWindow     bool
	staleSkipped   bool
	retryRequested bool
}

// batchError carries the batch that produced a handler failure so the projection
// loop can classify it and, for a Permanent failure, hand the events to a poison
// handler and advance the checkpoint past them.
type batchError struct {
	cause  error
	rows   []store.PersistedEvent
	target int64
}

func (e *batchError) Error() string { return e.cause.Error() }

func (e *batchError) Unwrap() error { return e.cause }

type batchPlan struct {
	firstSeenAt      time.Time
	rows             []store.PersistedEvent
	checkpoint       int64
	targetCheckpoint int64
	highestVisible   int64
	gapPosition      int64
	fullWindow       bool
	blockedByGap     bool
	staleSkipped     bool
}

// New constructs a Daemon with the provided database handle, event store, and
// projection registry. The registry selects the processing path for each
// projection from its handler shape.
func New(db PgxPool, eventStore projectorStore, registry *Registry, opts ...Option) *Daemon {
	config := applyOptions(opts...)

	daemon := &Daemon{
		id:                 uuid.New(),
		db:                 db,
		store:              eventStore,
		registry:           registry,
		config:             config,
		dispatcher:         newDispatcher(db, eventStore, &config),
		runningProjections: make(map[string]context.CancelFunc),
		failedProjections:  make(map[string]error),
		projectionDone:     make(map[string]chan struct{}),
	}

	if registry != nil {
		daemon.registrations = append([]registration(nil), registry.registrations...)
		daemon.registrationByName = make(map[string]registration, len(daemon.registrations))
		for _, entry := range daemon.registrations {
			daemon.registrationByName[entry.name] = entry
		}
	}

	return daemon
}

// ID returns the unique identifier of this projector instance.
func (d *Daemon) ID() uuid.UUID {
	return d.id
}

// Start registers the instance, starts its background loops, and blocks until the
// provided context is canceled or a fatal internal error occurs.
func (d *Daemon) Start(parent context.Context) (err error) {
	if err := d.validate(); err != nil {
		return err
	}

	controlCtx, controlCancel := context.WithCancel(parent)
	processingCtx, processingCancel := context.WithCancel(context.Background())
	if err := d.markStarted(controlCancel, processingCancel); err != nil {
		controlCancel()
		processingCancel()
		return err
	}
	defer d.markStopped()

	registered := false
	defer func() {
		controlCancel()
		d.shutdown(&registered)
	}()

	if err := d.initialize(controlCtx, &registered); err != nil {
		if controlCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, controlCtx.Err())) {
			return nil
		}
		return err
	}

	d.fatalErrCh = make(chan error, 1)

	leaderDone := make(chan struct{})
	d.leaderDone = leaderDone

	d.startBackground(controlCtx, func() { d.runHeartbeatLoop(controlCtx) })
	d.startBackground(controlCtx, func() {
		defer close(leaderDone)
		d.runLeaderLoop(controlCtx)
	})
	d.startBackground(controlCtx, func() { d.runAssignmentLoop(controlCtx, processingCtx) })
	d.startBackground(controlCtx, func() {
		if runErr := d.dispatcher.Start(controlCtx); runErr != nil && controlCtx.Err() == nil {
			d.reportFatal(fmt.Errorf("start dispatcher: %w", runErr))
		}
	})

	select {
	case runErr := <-d.fatalErrCh:
		return runErr
	case <-controlCtx.Done():
		return nil
	}
}

func (d *Daemon) initialize(ctx context.Context, registered *bool) error {
	cleanupThreshold := d.startupInstanceCleanupThreshold()
	removedRows, err := postgres.CleanupStaleInstances(ctx, d.db, d.projectorInstancesTable(), cleanupThreshold)
	if err != nil {
		d.logger().Error(ctx,
			"failed to clean stale instance registrations",
			"instance_id", d.id,
			"threshold", cleanupThreshold,
			"error", err,
		)
	} else if removedRows > 0 {
		d.logger().Info(ctx,
			"cleaned stale instance registrations",
			"instance_id", d.id,
			"threshold", cleanupThreshold,
			"removed_rows", removedRows,
		)
	}

	if err := postgres.RegisterInstance(ctx, d.db, d.projectorInstancesTable(), d.id); err != nil {
		return fmt.Errorf("register instance: %w", err)
	}
	*registered = true
	d.logger().Info(ctx, "instance registered", "instance_id", d.id)

	projectionNames := d.projectionNames()
	if err := postgres.EnsureProjectionsRegistered(ctx, d.db, d.projectionAssignmentsTable(), projectionNames); err != nil {
		return fmt.Errorf("ensure projections registered: %w", err)
	}

	for _, projectionName := range projectionNames {
		if err := postgres.EnsureCheckpointExists(ctx, d.db, d.projectionCheckpointsTable(), projectionName); err != nil {
			return fmt.Errorf("ensure checkpoint exists for projection %s: %w", projectionName, err)
		}
	}

	return nil
}

func (d *Daemon) shutdown(registered *bool) {
	logger := d.logger()
	d.stopAllProjections()

	timeout := d.shutdownTimeout()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
	defer shutdownCancel()

	if *registered {
		if removeErr := postgres.RemoveInstance(shutdownCtx, d.db, d.projectorInstancesTable(), d.id); removeErr != nil {
			logger.Error(shutdownCtx, "failed to remove instance registration", "instance_id", d.id, "error", removeErr)
		} else {
			logger.Info(shutdownCtx, "instance removed", "instance_id", d.id)
		}
	}

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		if processingCancel := d.getProcessingCancel(); processingCancel != nil {
			logger.Error(shutdownCtx, "daemon shutdown timed out; canceling active batches", "instance_id", d.id)
			processingCancel()
		}

		select {
		case <-done:
		case <-time.After(timeout):
			logger.Error(shutdownCtx, "daemon shutdown did not complete after forced cancellation", "instance_id", d.id)
		}
	}

	// A stuck projection can keep the overall wait above from completing, but the
	// leader loop only needs to stop before the leader connection is released
	// safely, otherwise releasing it races with an in-flight rebalance.
	if leaderDone := d.leaderDone; leaderDone != nil {
		select {
		case <-leaderDone:
		case <-time.After(timeout):
			logger.Error(shutdownCtx, "leader loop did not stop; skipping leader connection release", "instance_id", d.id)
			return
		}
	}

	if releaseErr := d.releaseLeaderConnection(shutdownCtx); releaseErr != nil {
		logger.Error(shutdownCtx, "failed to release leader connection", "instance_id", d.id, "error", releaseErr)
	}
}

// Stop requests shutdown of a running daemon.
func (d *Daemon) Stop() {
	d.mu.Lock()
	cancel := d.cancel
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// reportFatal sends a fatal error to the daemon's error channel and cancels
// the control context to initiate shutdown. Safe for concurrent calls; only
// the first error is captured.
func (d *Daemon) reportFatal(err error) {
	if err == nil {
		return
	}

	select {
	case d.fatalErrCh <- err:
	default:
	}

	d.mu.Lock()
	cancel := d.cancel
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (d *Daemon) validate() error {
	if d.db == nil {
		return ErrNilDB
	}
	if d.store == nil {
		return ErrNilStore
	}
	if d.dispatcher == nil {
		return ErrNilDispatcher
	}
	if d.config.DispatcherStrategy == DispatcherStrategyNotify {
		if strings.TrimSpace(d.config.NotifyConnectionString) == "" {
			return ErrMissingNotifyConnectionString
		}
		if strings.TrimSpace(d.config.NotifyChannel) == "" {
			return ErrMissingNotifyChannel
		}
	}

	if d.registry == nil {
		return ErrNilRegistry
	}
	if err := d.registry.validate(); err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(d.registrations))
	for idx, entry := range d.registrations {
		if strings.TrimSpace(entry.name) == "" {
			return fmt.Errorf("projection at index %d has empty name", idx)
		}

		if _, exists := seen[entry.name]; exists {
			return fmt.Errorf("duplicate projection name %q", entry.name)
		}

		seen[entry.name] = struct{}{}
	}

	return nil
}

func (d *Daemon) markStarted(cancel, processingCancel context.CancelFunc) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.started {
		return ErrAlreadyStarted
	}

	d.cancel = cancel
	d.processingCancel = processingCancel
	d.started = true
	return nil
}

func (d *Daemon) markStopped() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.cancel = nil
	d.processingCancel = nil
	d.started = false
}

func (d *Daemon) startBackground(ctx context.Context, fn func()) {
	d.wg.Add(1)

	go func() {
		defer d.wg.Done()

		select {
		case <-ctx.Done():
			return
		default:
		}

		fn()
	}()
}

func (d *Daemon) runHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(d.config.HeartbeatInterval)
	defer ticker.Stop()

	missingRegistrationCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := postgres.UpdateHeartbeat(ctx, d.db, d.projectorInstancesTable(), d.id); err != nil && ctx.Err() == nil {
				if errors.Is(err, postgres.ErrInstanceRegistrationMissing) {
					missingRegistrationCount++
					if missingRegistrationCount < missingRegistrationRetryLimit {
						d.logger().Error(ctx,
							"instance registration missing during heartbeat; retrying before shutdown",
							"instance_id", d.id,
							"attempt", missingRegistrationCount,
							"error", err,
						)
						continue
					}
					d.reportFatal(fmt.Errorf("instance registration lost during heartbeat: %w", err))
					return
				}
				missingRegistrationCount = 0
				d.logger().Error(ctx, "failed to update instance heartbeat", "instance_id", d.id, "error", err)
				continue
			}

			missingRegistrationCount = 0
			if d.config.Observer != nil {
				d.config.Observer.OnHeartbeat(ctx, DaemonStats{
					InstanceID: d.id,
					IsLeader:   d.leaderActive(),
				})
			}
		}
	}
}

func (d *Daemon) runLeaderLoop(ctx context.Context) {
	if err := d.leaderCycle(ctx); err != nil && ctx.Err() == nil {
		d.logger().Error(ctx, "leader cycle failed", "instance_id", d.id, "error", err)
	}

	ticker := time.NewTicker(d.config.RebalanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.leaderCycle(ctx); err != nil && ctx.Err() == nil {
				d.logger().Error(ctx, "leader cycle failed", "instance_id", d.id, "error", err)
			}
		}
	}
}

func (d *Daemon) leaderCycle(ctx context.Context) error {
	if d.config.LeaderStrategy == LeaderStrategyLease {
		return d.leaderCycleLease(ctx)
	}

	conn, err := d.ensureLeaderConn(ctx)
	if err != nil {
		return err
	}

	if d.leaderActive() {
		if pingErr := conn.Ping(ctx); pingErr != nil {
			d.logger().Error(ctx, "leader connection ping failed; relinquishing leadership", "instance_id", d.id, "error", pingErr)
			d.dropLeaderConnection(ctx)
			return nil
		}

		return d.rebalance(ctx, conn)
	}

	acquired, err := tryAcquireLeaderLock(ctx, conn)
	if err != nil {
		d.logger().Error(ctx, "failed to acquire leader lock", "instance_id", d.id, "error", err)
		d.dropLeaderConnection(ctx)
		return nil
	}

	if !acquired {
		return nil
	}

	d.setLeader(true)
	d.logger().Info(ctx, "instance became leader", "instance_id", d.id)

	if err := d.rebalance(ctx, conn); err != nil {
		return fmt.Errorf("rebalance after leader acquisition: %w", err)
	}

	return nil
}

func (d *Daemon) leaderCycleLease(ctx context.Context) error {
	now := timeNow()

	d.mu.Lock()
	lastSuccess := d.lastLeaderSuccess
	isCurrentlyLeader := d.isLeader
	d.mu.Unlock()

	// Self-demotion check: if we are leader, verify we successfully renewed within HeartbeatTimeout.
	if isCurrentlyLeader && !lastSuccess.IsZero() && now.Sub(lastSuccess) >= d.config.HeartbeatTimeout {
		d.setLeader(false)
		d.logger().Error(ctx, "leadership lease expired locally; relinquishing leadership", "instance_id", d.id, "last_success", lastSuccess, "elapsed", now.Sub(lastSuccess))
		return nil
	}

	acquired, err := postgres.TryAcquireLease(ctx, d.db, d.config.ProjectorLeaderLeasesTable, d.id, d.config.HeartbeatTimeout)
	if err != nil {
		d.logger().Error(ctx, "failed to update leader lease in database", "instance_id", d.id, "error", err)
		return nil
	}

	if !acquired {
		if isCurrentlyLeader {
			d.setLeader(false)
			d.logger().Info(ctx, "instance lost leader lease; relinquishing leadership", "instance_id", d.id)
		}
		return nil
	}

	d.mu.Lock()
	d.lastLeaderSuccess = now
	d.mu.Unlock()

	if !isCurrentlyLeader {
		d.setLeader(true)
		d.logger().Info(ctx, "instance became leader (lease acquired)", "instance_id", d.id)
	}

	conn, err := d.db.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("get connection for lease rebalance: %w", err)
	}
	defer conn.Release()

	if err := d.rebalance(ctx, conn); err != nil {
		return fmt.Errorf("rebalance after leader lease acquisition: %w", err)
	}

	return nil
}

func (d *Daemon) rebalance(ctx context.Context, conn *pgxpool.Conn) error {
	liveInstances, err := postgres.ListLiveInstances(ctx, conn, d.projectorInstancesTable(), d.config.HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("list live instances: %w", err)
	}

	assignments, err := postgres.GetAssignments(ctx, conn, d.projectionAssignmentsTable())
	if err != nil {
		return fmt.Errorf("get assignments: %w", err)
	}

	if !postgres.NeedsRebalance(assignments, liveInstances) {
		d.logger().Debug(ctx, "rebalance skipped; assignments already balanced", "instance_id", d.id, "live_instances", len(liveInstances))
		return nil
	}

	projectionNames := make([]string, 0, len(assignments))
	for _, assignment := range assignments {
		projectionNames = append(projectionNames, assignment.ProjectionName)
	}

	nextAssignments := postgres.ComputeAssignments(projectionNames, liveInstances)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin rebalance transaction: %w", err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}

		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			d.logger().Error(ctx, "rebalance rollback failed", "instance_id", d.id, "error", rollbackErr)
		}
	}()

	if err := applyAssignments(ctx, tx, d.projectionAssignmentsTable(), nextAssignments); err != nil {
		return fmt.Errorf("apply assignments: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rebalance transaction: %w", err)
	}
	committed = true

	d.logger().Info(ctx, "projection assignments updated", "instance_id", d.id, "live_instances", len(liveInstances), "projections", len(projectionNames))
	if d.config.Observer != nil {
		d.config.Observer.OnRebalance(ctx, nextAssignments)
	}
	return nil
}

func (d *Daemon) runAssignmentLoop(ctx, processingCtx context.Context) {
	if err := d.syncAssignments(ctx, processingCtx); err != nil && ctx.Err() == nil {
		d.logger().Error(ctx, "assignment sync failed", "instance_id", d.id, "error", err)
	}

	ticker := time.NewTicker(d.assignmentPollInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.syncAssignments(ctx, processingCtx); err != nil && ctx.Err() == nil {
				d.logger().Error(ctx, "assignment sync failed", "instance_id", d.id, "error", err)
			}
		}
	}
}

func (d *Daemon) syncAssignments(ctx, processingCtx context.Context) error {
	assignments, err := postgres.GetAssignments(ctx, d.db, d.projectionAssignmentsTable())
	if err != nil {
		return fmt.Errorf("get assignments: %w", err)
	}

	desired := make(map[string]registration, len(d.registrations))
	for _, assignment := range assignments {
		if !assignment.Assigned || assignment.InstanceID != d.id {
			continue
		}

		if d.projectionFailed(assignment.ProjectionName) {
			continue
		}

		entry, ok := d.registrationFor(assignment.ProjectionName)
		if !ok {
			continue
		}

		desired[assignment.ProjectionName] = entry
	}

	type projectionStart struct {
		projection registration
		ctx        context.Context
		done       chan struct{}
	}

	type projectionStop struct {
		cancel context.CancelFunc
		name   string
	}

	var toStart []projectionStart
	var toStop []projectionStop

	d.mu.Lock()
	for name, assignedProjection := range desired {
		if _, running := d.runningProjections[name]; running {
			continue
		}

		projectionCtx, projectionCancel := context.WithCancel(context.Background()) //nolint:gosec // cancel stored in runningProjections for later cleanup
		done := make(chan struct{})
		d.runningProjections[name] = projectionCancel
		d.projectionDone[name] = done
		toStart = append(toStart, projectionStart{
			projection: assignedProjection,
			ctx:        projectionCtx,
			done:       done,
		})
	}

	for name, cancel := range d.runningProjections {
		if _, shouldRun := desired[name]; shouldRun {
			continue
		}

		toStop = append(toStop, projectionStop{
			name:   name,
			cancel: cancel,
		})
	}
	d.mu.Unlock()

	for _, stopRequest := range toStop {
		d.logger().Info(ctx, "stopping projection", "instance_id", d.id, "projection", stopRequest.name)
		stopRequest.cancel()
	}

	for _, startRequest := range toStart {
		d.logger().Info(ctx, "starting projection", "instance_id", d.id, "projection", startRequest.projection.name)

		d.wg.Add(1)
		go func(projectionCtx context.Context, done chan struct{}, entry registration) {
			defer d.wg.Done()
			defer close(done)
			defer d.finishProjection(entry.name, done)

			d.runProjection(ctx, processingCtx, projectionCtx, entry)
		}(startRequest.ctx, startRequest.done, startRequest.projection)
	}

	return nil
}

//nolint:gocyclo // orchestration loop with clear structure
func (d *Daemon) runProjection(
	controlCtx, processingCtx, assignmentCtx context.Context,
	entry registration,
) {
	logger := d.logger()
	projectionName := entry.name
	basePollInterval := d.config.PollInterval
	currentPollInterval := basePollInterval
	delay := time.Duration(0)
	consecutiveFailures := 0
	var firstFailureAt time.Time
	gapTracker := &gapState{}

	logger.Info(controlCtx, "projection loop started", "instance_id", d.id, "projection", projectionName)
	defer logger.Info(context.Background(), "projection loop stopped", "instance_id", d.id, "projection", projectionName)

	for {
		if delay > 0 {
			woken, ok := d.waitForProjectionDelay(controlCtx, assignmentCtx, delay)
			if !ok {
				return
			}
			if woken {
				currentPollInterval = basePollInterval
			}
		} else {
			select {
			case <-controlCtx.Done():
				return
			case <-assignmentCtx.Done():
				return
			default:
			}
		}

		select {
		case <-controlCtx.Done():
			return
		case <-assignmentCtx.Done():
			return
		default:
		}

		result, err := d.processBatchWithGapState(processingCtx, entry, gapTracker)
		if controlCtx.Err() != nil || assignmentCtx.Err() != nil {
			return
		}

		if err != nil {
			if errors.Is(err, errProjectionOwnershipLost) {
				logger.Info(controlCtx, "projection ownership lost; stopping projection", "instance_id", d.id, "projection", projectionName)
				return
			}

			if firstFailureAt.IsZero() {
				firstFailureAt = timeNow()
			}
			consecutiveFailures++

			var failedBatch *batchError
			hasFailedBatch := errors.As(err, &failedBatch)

			retryable := failure.IsRetryable(err) ||
				errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, context.Canceled)

			switch {
			case retryable:
				logger.Error(controlCtx, "projection batch failed; retrying",
					"instance_id", d.id, "projection", projectionName,
					"error", err, "consecutive_failures", consecutiveFailures)
				d.observeDegraded(controlCtx, projectionName, firstFailureAt, consecutiveFailures, err)
				currentPollInterval = nextPollInterval(currentPollInterval, d.config.MaxPollInterval, basePollInterval)
				delay = currentPollInterval

			case failure.IsPermanent(err):
				if hasFailedBatch && entry.poison != nil {
					if poisonErr := entry.poison(controlCtx, projectionName, failedBatch.rows, failedBatch.cause); poisonErr != nil {
						logger.Error(controlCtx, "poison handler rejected the batch; stopping projection",
							"instance_id", d.id, "projection", projectionName, "error", poisonErr)
						d.observeDegraded(controlCtx, projectionName, firstFailureAt, consecutiveFailures, poisonErr)
						d.markProjectionFailed(projectionName, poisonErr)
						return
					}

					if skipErr := d.skipPoisonBatch(controlCtx, entry, failedBatch); skipErr != nil {
						logger.Error(controlCtx, "failed to skip poison batch; stopping projection",
							"instance_id", d.id, "projection", projectionName, "error", skipErr)
						d.observeDegraded(controlCtx, projectionName, firstFailureAt, consecutiveFailures, skipErr)
						d.markProjectionFailed(projectionName, skipErr)
						return
					}

					logger.Error(controlCtx, "projection skipped a poison batch",
						"instance_id", d.id, "projection", projectionName,
						"target_position", failedBatch.target, "event_count", len(failedBatch.rows), "error", failedBatch.cause)
					d.observePoisonSkipped(controlCtx, projectionName, failedBatch)

					consecutiveFailures = 0
					firstFailureAt = time.Time{}
					delay = 0
					continue
				}

				logger.Error(controlCtx, "projection returned a permanent error; stopping projection",
					"instance_id", d.id, "projection", projectionName, "error", err)
				d.observeDegraded(controlCtx, projectionName, firstFailureAt, consecutiveFailures, err)
				d.markProjectionFailed(projectionName, err)
				return

			case entry.policy == FailurePolicyRetry:
				logger.Error(controlCtx, "projection batch failed; retrying",
					"instance_id", d.id, "projection", projectionName,
					"error", err, "consecutive_failures", consecutiveFailures)
				d.observeDegraded(controlCtx, projectionName, firstFailureAt, consecutiveFailures, err)
				currentPollInterval = nextPollInterval(currentPollInterval, d.config.MaxPollInterval, basePollInterval)
				delay = currentPollInterval

			default:
				logger.Error(controlCtx, "projection batch failed",
					"instance_id", d.id, "projection", projectionName,
					"error", err, "consecutive_failures", consecutiveFailures)

				if d.config.MaxConsecutiveFailures > 0 && consecutiveFailures >= d.config.MaxConsecutiveFailures {
					d.reportFatal(fmt.Errorf("%w: projection %s failed %d times: %w",
						ErrConsecutiveFailures, projectionName, consecutiveFailures, err))
					return
				}

				delay = currentPollInterval
			}

			continue
		}

		consecutiveFailures = 0
		firstFailureAt = time.Time{}

		if result.retryRequested {
			// A detached stale-gap advance was invalidated by a gap that resolved
			// during the apply. Re-probe immediately; the redundant external apply
			// is covered by handler idempotency.
			currentPollInterval = basePollInterval
			delay = 0
			continue
		}

		if !result.progressed {
			if result.blockedByGap {
				currentPollInterval = basePollInterval
				delay = d.config.BatchPause
				continue
			}

			currentPollInterval = nextPollInterval(currentPollInterval, d.config.MaxPollInterval, basePollInterval)
			delay = currentPollInterval
			continue
		}

		currentPollInterval = basePollInterval

		if assignmentCtx.Err() != nil {
			return
		}

		if result.fullWindow {
			delay = d.config.BatchPause
			continue
		}

		delay = currentPollInterval
	}
}

func (d *Daemon) projectionFailed(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, failed := d.failedProjections[name]

	return failed
}

// markProjectionFailed records that a projection stopped permanently so the
// assignment loop does not restart it until the daemon restarts.
func (d *Daemon) markProjectionFailed(name string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.failedProjections == nil {
		d.failedProjections = make(map[string]error)
	}

	d.failedProjections[name] = err
}

func (d *Daemon) observeDegraded(ctx context.Context, projectionName string, firstFailureAt time.Time, consecutive int, err error) {
	if d.config.Observer == nil {
		return
	}

	d.config.Observer.OnProjectionDegraded(ctx, DegradedStats{
		ProjectionName:      projectionName,
		FirstFailureAt:      firstFailureAt,
		ConsecutiveFailures: consecutive,
		LastError:           err,
	})
}

func (d *Daemon) observePoisonSkipped(ctx context.Context, projectionName string, failed *batchError) {
	if d.config.Observer == nil {
		return
	}

	d.config.Observer.OnPoisonBatchSkipped(ctx, PoisonBatchStats{
		ProjectionName: projectionName,
		TargetPosition: failed.target,
		EventCount:     len(failed.rows),
		Cause:          failed.cause,
	})
}

// skipPoisonBatch advances the checkpoint past a batch whose Permanent failure a
// poison handler has recorded. The batch is not applied to the read model; the
// handler's dead-letter store is the durable record.
func (d *Daemon) skipPoisonBatch(ctx context.Context, entry registration, failed *batchError) error {
	tx, err := d.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin poison skip transaction for projection %s: %w", entry.name, err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}

		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			d.logger().Error(ctx, "poison skip rollback failed", "instance_id", d.id, "projection", entry.name, "error", rollbackErr)
		}
	}()

	assigned, err := d.ensureProjectionOwnership(ctx, tx, entry.name)
	if err != nil {
		return fmt.Errorf("check ownership for projection %s: %w", entry.name, err)
	}
	if !assigned {
		return errProjectionOwnershipLost
	}

	currentCheckpoint, err := postgres.GetCheckpointForUpdate(ctx, tx, d.projectionCheckpointsTable(), entry.name)
	if err != nil {
		return fmt.Errorf("get checkpoint for projection %s: %w", entry.name, err)
	}
	if currentCheckpoint >= failed.target {
		return nil
	}

	if err := postgres.SaveCheckpoint(ctx, tx, d.projectionCheckpointsTable(), entry.name, failed.target); err != nil {
		return fmt.Errorf("save checkpoint for projection %s: %w", entry.name, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit poison skip transaction for projection %s: %w", entry.name, err)
	}
	committed = true

	return nil
}

func (d *Daemon) processBatch(
	parentCtx context.Context,
	entry registration,
	checkpointOverride ...int64,
) (processedBatch, error) {
	return d.processBatchWithGapState(parentCtx, entry, &gapState{}, checkpointOverride...)
}

func (d *Daemon) processBatchWithGapState(
	parentCtx context.Context,
	entry registration,
	gapTracker *gapState,
	checkpointOverride ...int64,
) (processedBatch, error) {
	start := timeNow()
	ctx := parentCtx
	if d.config.BatchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parentCtx, d.config.BatchTimeout)
		defer cancel()
	}

	probe, err := d.probePlan(ctx, entry.name, gapTracker, checkpointOverride...)
	if err != nil {
		if d.config.Observer != nil {
			d.config.Observer.OnBatchProcessed(ctx, BatchStats{
				ProjectionName: entry.name,
				Duration:       timeNow().Sub(start),
				Detached:       !entry.applier.transactional(),
				Error:          err,
			})
		}
		return processedBatch{}, err
	}

	if probe.targetCheckpoint <= probe.checkpoint {
		if len(probe.rows) == 0 {
			gapTracker.clear()
		}
		return processedBatch{
			blockedByGap: probe.blockedByGap,
			fullWindow:   probe.fullWindow,
			checkpoint:   probe.checkpoint,
		}, nil
	}

	result, err := d.processProbedBatch(parentCtx, ctx, entry, &probe)
	duration := timeNow().Sub(start)
	if err != nil {
		if d.config.Observer != nil {
			lastPos := probe.checkpoint
			lag := probe.highestVisible - lastPos
			if lag < 0 {
				lag = 0
			}
			d.config.Observer.OnBatchProcessed(ctx, BatchStats{
				ProjectionName: entry.name,
				StartPosition:  probe.checkpoint,
				LastPosition:   lastPos,
				HeadPosition:   probe.highestVisible,
				Lag:            lag,
				EventsRead:     len(probe.rows),
				EventsHandled:  0,
				Duration:       duration,
				StaleSkipped:   probe.staleSkipped,
				Detached:       !entry.applier.transactional(),
				Error:          err,
			})
		}
		return processedBatch{}, err
	}

	if result.staleSkipped {
		d.logger().Info(ctx,
			"projection advanced past stale gap",
			"instance_id", d.id,
			"projection", entry.name,
			"gap_position", probe.gapPosition,
			"checkpoint_from", probe.checkpoint,
			"checkpoint_to", probe.targetCheckpoint,
			"stale_for", timeNow().Sub(probe.firstSeenAt),
			"handled_events", result.handledCount,
			"visible_head", probe.highestVisible,
		)
		if d.config.Observer != nil {
			d.config.Observer.OnGapSkipped(ctx, GapStats{
				ProjectionName: entry.name,
				GapPosition:    probe.gapPosition,
				HighestVisible: probe.highestVisible,
				StaleFor:       timeNow().Sub(probe.firstSeenAt),
			})
		}
	}

	if d.config.Observer != nil {
		lastPos := result.checkpoint
		lag := probe.highestVisible - lastPos
		if lag < 0 {
			lag = 0
		}
		d.config.Observer.OnBatchProcessed(ctx, BatchStats{
			ProjectionName: entry.name,
			StartPosition:  probe.checkpoint,
			LastPosition:   lastPos,
			HeadPosition:   probe.highestVisible,
			Lag:            lag,
			EventsRead:     len(probe.rows),
			EventsHandled:  result.handledCount,
			Duration:       duration,
			StaleSkipped:   result.staleSkipped,
			Detached:       !entry.applier.transactional(),
			Error:          nil,
		})
	}

	if result.progressed {
		gapTracker.clear()
	}

	return result, nil
}

func (d *Daemon) probePlan(
	ctx context.Context,
	projectionName string,
	gapTracker *gapState,
	checkpointOverride ...int64,
) (batchPlan, error) {
	tx, err := d.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return batchPlan{}, fmt.Errorf("begin frontier probe transaction for projection %s: %w", projectionName, err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			d.logger().Error(ctx, "frontier probe rollback failed", "instance_id", d.id, "projection", projectionName, "error", rollbackErr)
		}
	}()

	checkpoint := int64(0)
	if len(checkpointOverride) > 0 {
		checkpoint = checkpointOverride[0]
	} else {
		checkpoint, err = postgres.GetCheckpoint(ctx, tx, d.projectionCheckpointsTable(), projectionName)
		if err != nil {
			return batchPlan{}, fmt.Errorf("get checkpoint for projection %s: %w", projectionName, err)
		}
	}

	rows, err := d.store.ReadEvents(ctx, tx, checkpoint, d.config.BatchSize)
	if err != nil {
		return batchPlan{}, fmt.Errorf("probe frontier for projection %s: %w", projectionName, err)
	}

	probe := buildBatchPlan(checkpoint, rows, d.config.BatchSize)
	if len(rows) == 0 || probe.targetCheckpoint > checkpoint {
		return probe, nil
	}
	staleFor := gapTracker.observe(probe.gapPosition, probe.highestVisible, timeNow())
	probe.firstSeenAt = gapTracker.firstSeenAt
	if d.config.Observer != nil {
		d.config.Observer.OnGapDetected(ctx, GapStats{
			ProjectionName: projectionName,
			GapPosition:    probe.gapPosition,
			HighestVisible: probe.highestVisible,
			StaleFor:       staleFor,
		})
	}
	if staleFor < d.staleGapThreshold() {
		return probe, nil
	}

	if !gapTracker.staleLogged {
		d.logger().Error(ctx,
			"projection gap became stale",
			"instance_id", d.id,
			"projection", projectionName,
			"gap_position", probe.gapPosition,
			"stale_for", staleFor,
			"highest_visible_position", probe.highestVisible,
		)
		gapTracker.staleLogged = true
	}

	applyStaleHarbor(&probe, rows, d.staleGapHarborLag())
	return probe, nil
}

// processProbedBatch applies a probed batch either inside the daemon transaction
// or, for detached projections, outside it.
//
// ctx is the batch context, bounded by BatchTimeout. parentCtx is the long-lived
// processing context; the detached checkpoint phase derives its own budget from
// parentCtx so a slow external apply cannot starve it.
func (d *Daemon) processProbedBatch(
	parentCtx, ctx context.Context,
	entry registration,
	probe *batchPlan,
) (processedBatch, error) {
	if !entry.applier.transactional() {
		return d.processDetachedBatch(parentCtx, ctx, entry, probe)
	}

	txOptions := pgx.TxOptions{}
	attempts := 1
	if probe.staleSkipped {
		txOptions = pgx.TxOptions{IsoLevel: pgx.Serializable}
		attempts = staleGapRetryLimit
	}

	originalProbe := *probe
	for attempt := 0; attempt < attempts; attempt++ {
		attemptProbe := originalProbe
		result, err := d.processTxBatchAttempt(ctx, entry, &attemptProbe, &txOptions)
		if err == nil {
			*probe = attemptProbe
			return result, nil
		}
		if !isSerializationFailure(err) || !originalProbe.staleSkipped {
			return processedBatch{}, err
		}
	}

	d.logger().Info(ctx,
		"stale gap advancement hit serializable contention; retrying later",
		"instance_id", d.id,
		"projection", entry.name,
		"gap_position", originalProbe.gapPosition,
		"checkpoint", originalProbe.checkpoint,
		"attempts", attempts,
	)
	return processedBatch{
		blockedByGap: true,
		fullWindow:   originalProbe.fullWindow,
		checkpoint:   originalProbe.checkpoint,
	}, nil
}

// processTxBatchAttempt applies a batch inside one transaction so the read model
// and the checkpoint commit atomically.
func (d *Daemon) processTxBatchAttempt(
	ctx context.Context,
	entry registration,
	probe *batchPlan,
	txOptions *pgx.TxOptions,
) (processedBatch, error) {
	tx, err := d.db.BeginTx(ctx, *txOptions)
	if err != nil {
		return processedBatch{}, fmt.Errorf("begin transaction for projection %s: %w", entry.name, err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}

		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			d.logger().Error(ctx, "projection transaction rollback failed", "instance_id", d.id, "projection", entry.name, "error", rollbackErr)
		}
	}()

	assigned, err := d.ensureProjectionOwnership(ctx, tx, entry.name)
	if err != nil {
		return processedBatch{}, fmt.Errorf("check ownership for projection %s: %w", entry.name, err)
	}
	if !assigned {
		return processedBatch{}, errProjectionOwnershipLost
	}

	prepared, proceed, err := d.prepareProbeForBatch(ctx, tx, entry.name, probe)
	if err != nil {
		return processedBatch{}, err
	}
	if !proceed {
		return prepared, nil
	}

	handledCount, err := entry.applier.apply(ctx, tx, probe.rows)
	if err != nil {
		return processedBatch{}, &batchError{cause: err, rows: probe.rows, target: probe.targetCheckpoint}
	}

	if err := d.recordStaleGapSkip(ctx, tx, entry.name, probe); err != nil {
		return processedBatch{}, err
	}

	if err := postgres.SaveCheckpoint(ctx, tx, d.projectionCheckpointsTable(), entry.name, probe.targetCheckpoint); err != nil {
		return processedBatch{}, fmt.Errorf("save checkpoint for projection %s: %w", entry.name, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return processedBatch{}, fmt.Errorf("commit transaction for projection %s: %w", entry.name, err)
	}
	committed = true

	return processedBatch{
		progressed:   true,
		fullWindow:   probe.fullWindow,
		checkpoint:   probe.targetCheckpoint,
		handledCount: handledCount,
		staleSkipped: probe.staleSkipped,
	}, nil
}

// processDetachedBatch applies the batch with no transaction held and then
// persists the checkpoint in a short transaction. Delivery is at-least-once, so
// handlers must be idempotent.
//
// It runs three phases with independent budgets. parentCtx is the long-lived
// processing context and ctx is the batch context, bounded by BatchTimeout.
//
//   - Stale-gap revalidation and the external apply run on ctx. The apply is
//     further bounded by DetachedApplyTimeout, so it stays under the overall
//     batch cap.
//   - The checkpoint transaction runs on parentCtx bounded by CheckpointTimeout.
//     The budget starts when the checkpoint phase starts, after the apply, and
//     deliberately does not inherit the batch deadline. Otherwise a slow apply
//     would leave the checkpoint write with a canceled context and force a
//     duplicate re-apply of the whole window.
func (d *Daemon) processDetachedBatch(
	parentCtx, ctx context.Context,
	entry registration,
	probe *batchPlan,
) (processedBatch, error) {
	if probe.staleSkipped {
		// Freeze the applied set and the checkpoint target before any external
		// write. Re-deriving the target after the apply could checkpoint past
		// events that were never applied.
		if err := d.revalidateDetachedPlan(ctx, entry.name, probe); err != nil {
			return processedBatch{}, err
		}
	}

	if probe.targetCheckpoint <= probe.checkpoint {
		return processedBatch{
			blockedByGap: probe.blockedByGap,
			fullWindow:   probe.fullWindow,
			checkpoint:   probe.checkpoint,
		}, nil
	}

	applyCtx, cancelApply := d.detachedApplyContext(ctx)

	target := probe.targetCheckpoint
	handledCount, err := entry.applier.apply(applyCtx, nil, probe.rows)
	cancelApply()

	if err != nil {
		return processedBatch{}, &batchError{cause: err, rows: probe.rows, target: target}
	}

	checkpointCtx, cancelCheckpoint := d.checkpointContext(parentCtx)
	defer cancelCheckpoint()

	result, err := d.commitDetachedCheckpoint(checkpointCtx, entry, probe)
	if err != nil {
		return processedBatch{}, err
	}
	if !result.progressed {
		// Either the checkpoint moved under us or a resolved gap invalidated the
		// frozen skip. Both are reported to the loop as-is.
		return result, nil
	}

	result.checkpoint = target
	result.handledCount = handledCount
	result.fullWindow = probe.fullWindow
	result.staleSkipped = probe.staleSkipped

	return result, nil
}

// detachedApplyContext bounds the external apply of a detached projection. It
// derives from the batch context, so the effective budget is
// min(BatchTimeout, DetachedApplyTimeout).
func (d *Daemon) detachedApplyContext(base context.Context) (context.Context, context.CancelFunc) {
	if d.config.DetachedApplyTimeout > 0 {
		return context.WithTimeout(base, d.config.DetachedApplyTimeout)
	}

	return base, func() {}
}

// checkpointContext bounds the post-apply checkpoint phase of a detached
// projection. It derives from the long-lived processing context rather than the
// batch context, and callers must call it after the apply so the budget is
// measured from the start of the checkpoint phase.
func (d *Daemon) checkpointContext(base context.Context) (context.Context, context.CancelFunc) {
	if d.config.CheckpointTimeout > 0 {
		return context.WithTimeout(base, d.config.CheckpointTimeout)
	}

	return base, func() {}
}

// commitDetachedCheckpoint persists the checkpoint, and any stale-gap skip, in a
// short transaction. Only this transaction is retried on a serialization
// failure; the external apply already happened and must not be repeated here.
func (d *Daemon) commitDetachedCheckpoint(
	ctx context.Context,
	entry registration,
	probe *batchPlan,
) (processedBatch, error) {
	// The external apply already happened, so a serialization failure here may be
	// retried freely: only the checkpoint transaction repeats, never the apply.
	txOptions := pgx.TxOptions{}
	attempts := staleGapRetryLimit
	if probe.staleSkipped {
		txOptions = pgx.TxOptions{IsoLevel: pgx.Serializable}
	}

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		result, err := d.commitDetachedCheckpointAttempt(ctx, entry, probe, &txOptions)
		if err == nil {
			return result, nil
		}

		lastErr = err
		if !isSerializationFailure(err) {
			return processedBatch{}, err
		}
	}

	return processedBatch{}, lastErr
}

func (d *Daemon) commitDetachedCheckpointAttempt(
	ctx context.Context,
	entry registration,
	probe *batchPlan,
	txOptions *pgx.TxOptions,
) (processedBatch, error) {
	tx, err := d.db.BeginTx(ctx, *txOptions)
	if err != nil {
		return processedBatch{}, fmt.Errorf("begin checkpoint transaction for projection %s: %w", entry.name, err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}

		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			d.logger().Error(ctx, "detached checkpoint rollback failed", "instance_id", d.id, "projection", entry.name, "error", rollbackErr)
		}
	}()

	assigned, err := d.ensureProjectionOwnership(ctx, tx, entry.name)
	if err != nil {
		return processedBatch{}, fmt.Errorf("check ownership for projection %s: %w", entry.name, err)
	}
	if !assigned {
		return processedBatch{}, errProjectionOwnershipLost
	}

	currentCheckpoint, err := postgres.GetCheckpointForUpdate(ctx, tx, d.projectionCheckpointsTable(), entry.name)
	if err != nil {
		return processedBatch{}, fmt.Errorf("get checkpoint for projection %s: %w", entry.name, err)
	}
	if currentCheckpoint != probe.checkpoint {
		return processedBatch{
			blockedByGap: true,
			fullWindow:   probe.fullWindow,
			checkpoint:   currentCheckpoint,
		}, nil
	}

	if probe.staleSkipped {
		resolved, err := d.detachedGapResolved(ctx, tx, entry.name, probe)
		if err != nil {
			return processedBatch{}, err
		}
		if resolved {
			// The gap closed while the batch was applied externally, so the frozen
			// skip is no longer valid. Discard it and re-probe. The deferred
			// rollback releases the transaction; the external re-apply is covered
			// by handler idempotency.
			return processedBatch{
				retryRequested: true,
				blockedByGap:   true,
				fullWindow:     probe.fullWindow,
				checkpoint:     probe.checkpoint,
			}, nil
		}
	}

	if err := d.recordStaleGapSkip(ctx, tx, entry.name, probe); err != nil {
		return processedBatch{}, err
	}

	if err := postgres.SaveCheckpoint(ctx, tx, d.projectionCheckpointsTable(), entry.name, probe.targetCheckpoint); err != nil {
		return processedBatch{}, fmt.Errorf("save checkpoint for projection %s: %w", entry.name, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return processedBatch{}, fmt.Errorf("commit checkpoint transaction for projection %s: %w", entry.name, err)
	}
	committed = true

	return processedBatch{progressed: true}, nil
}

func (d *Daemon) prepareProbeForBatch(
	ctx context.Context,
	tx pgx.Tx,
	projectionName string,
	probe *batchPlan,
) (processedBatch, bool, error) {
	currentCheckpoint, err := postgres.GetCheckpointForUpdate(ctx, tx, d.projectionCheckpointsTable(), projectionName)
	if err != nil {
		return processedBatch{}, false, err
	}
	if currentCheckpoint != probe.checkpoint {
		return processedBatch{
			blockedByGap: true,
			fullWindow:   probe.fullWindow,
			checkpoint:   currentCheckpoint,
		}, false, nil
	}

	if !probe.staleSkipped {
		return processedBatch{}, true, nil
	}
	if err := d.revalidatePlan(ctx, tx, projectionName, probe); err != nil {
		return processedBatch{}, false, err
	}
	if probe.targetCheckpoint <= probe.checkpoint {
		return processedBatch{
			blockedByGap: probe.blockedByGap,
			fullWindow:   probe.fullWindow,
			checkpoint:   probe.checkpoint,
		}, false, nil
	}

	return processedBatch{}, true, nil
}

func isSerializationFailure(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "40001" {
		return true
	}

	type sqlStateCarrier interface {
		SQLState() string
	}
	var carrier sqlStateCarrier
	if errors.As(err, &carrier) && carrier.SQLState() == "40001" {
		return true
	}

	return false
}

func (d *Daemon) recordStaleGapSkip(
	ctx context.Context,
	tx pgx.Tx,
	projectionName string,
	probe *batchPlan,
) error {
	if !probe.staleSkipped {
		return nil
	}

	record := postgres.ProjectionGapSkipRecord{
		FirstSeenAt:            probe.firstSeenAt,
		ProjectionName:         projectionName,
		InstanceID:             d.id,
		GapPosition:            probe.gapPosition,
		SkipToPosition:         probe.targetCheckpoint,
		HighestVisiblePosition: probe.highestVisible,
	}
	if err := postgres.RecordGapSkip(ctx, tx, d.projectionGapSkipsTable(), &record); err != nil {
		return fmt.Errorf("record gap skip for projection %s: %w", projectionName, err)
	}

	return nil
}

func buildBatchPlan(checkpoint int64, rows []store.PersistedEvent, batchSize int) batchPlan {
	plan := batchPlan{
		checkpoint: checkpoint,
		rows:       rows,
		fullWindow: len(rows) == batchSize,
	}
	if len(rows) == 0 {
		return plan
	}

	plan.highestVisible = rows[len(rows)-1].GlobalPosition
	safeCount, safeFrontier := computeSafeFrontier(checkpoint, rows)
	if safeCount > 0 {
		plan.targetCheckpoint = safeFrontier
		plan.rows = rows[:safeCount]
		return plan
	}

	plan.blockedByGap = true
	plan.gapPosition = checkpoint + 1
	return plan
}

// applyStaleHarbor derives the safe-harbor target for a plan that is blocked on a
// gap and freezes the rows at or below that target, so the applied set and the
// checkpoint target always agree.
func applyStaleHarbor(plan *batchPlan, rows []store.PersistedEvent, lag int) {
	safeHarbor, ok := computeGapSkipTarget(plan.gapPosition, rows, lag)
	if !ok || safeHarbor <= plan.checkpoint {
		return
	}

	plan.targetCheckpoint = safeHarbor
	plan.staleSkipped = true
	plan.rows = rowsUpTo(rows, safeHarbor)
}

func rowsUpTo(rows []store.PersistedEvent, target int64) []store.PersistedEvent {
	for i := range rows {
		if rows[i].GlobalPosition > target {
			return rows[:i]
		}
	}

	return rows
}

func (d *Daemon) revalidatePlan(
	ctx context.Context,
	tx pgx.Tx,
	projectionName string,
	probe *batchPlan,
) error {
	firstSeenAt := probe.firstSeenAt

	rows, err := d.store.ReadEvents(ctx, tx, probe.checkpoint, d.config.BatchSize)
	if err != nil {
		return fmt.Errorf("revalidate stale gap frontier for projection %s: %w", projectionName, err)
	}

	refreshed := buildBatchPlan(probe.checkpoint, rows, d.config.BatchSize)
	refreshed.firstSeenAt = firstSeenAt
	if !refreshed.blockedByGap {
		*probe = refreshed
		return nil
	}

	applyStaleHarbor(&refreshed, rows, d.staleGapHarborLag())
	*probe = refreshed
	return nil
}

// revalidateDetachedPlan runs the stale-gap revalidation in a short read-only
// transaction, before any external apply, and freezes the result on probe.
func (d *Daemon) revalidateDetachedPlan(ctx context.Context, projectionName string, probe *batchPlan) error {
	tx, err := d.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin detached revalidation transaction for projection %s: %w", projectionName, err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			d.logger().Error(ctx, "detached revalidation rollback failed", "instance_id", d.id, "projection", projectionName, "error", rollbackErr)
		}
	}()

	return d.revalidatePlan(ctx, tx, projectionName, probe)
}

// detachedGapResolved reports whether the gap a frozen stale-skip plan was
// advancing past has closed. It must be called inside the checkpoint
// transaction so the decision and the checkpoint write share one view.
func (d *Daemon) detachedGapResolved(ctx context.Context, tx pgx.Tx, projectionName string, probe *batchPlan) (bool, error) {
	rows, err := d.store.ReadEvents(ctx, tx, probe.checkpoint, d.config.BatchSize)
	if err != nil {
		return false, fmt.Errorf("re-check gap for projection %s: %w", projectionName, err)
	}

	refreshed := buildBatchPlan(probe.checkpoint, rows, d.config.BatchSize)
	return !refreshed.blockedByGap, nil
}

func (d *Daemon) waitForProjectionDelay(controlCtx, assignmentCtx context.Context, delay time.Duration) (woken, ok bool) {
	if delay <= 0 {
		return false, true
	}

	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	select {
	case <-controlCtx.Done():
		return false, false
	case <-assignmentCtx.Done():
		return false, false
	case <-d.dispatcher.WakeupChan():
		return true, true
	case <-timer.C:
		return false, true
	}
}

func (d *Daemon) stopAllProjections() {
	d.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(d.runningProjections))
	for name, cancel := range d.runningProjections {
		delete(d.runningProjections, name)
		delete(d.projectionDone, name)
		d.logger().Debug(context.Background(), "canceling projection", "instance_id", d.id, "projection", name)
		cancels = append(cancels, cancel)
	}
	d.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (d *Daemon) ensureLeaderConn(ctx context.Context) (*pgxpool.Conn, error) {
	d.mu.Lock()
	if d.leaderConn != nil {
		conn := d.leaderConn
		d.mu.Unlock()
		return conn, nil
	}
	d.mu.Unlock()

	conn, err := d.db.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("open leader connection: %w", err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.leaderConn != nil {
		existing := d.leaderConn
		existing.Release()
		return existing, nil
	}

	d.leaderConn = conn
	return conn, nil
}

func (d *Daemon) releaseLeaderConnection(ctx context.Context) error {
	d.mu.Lock()
	conn := d.leaderConn
	wasLeader := d.isLeader
	d.leaderConn = nil
	d.isLeader = false
	d.mu.Unlock()

	if d.config.LeaderStrategy == LeaderStrategyLease {
		if wasLeader {
			if err := postgres.ReleaseLease(ctx, d.db, d.config.ProjectorLeaderLeasesTable, d.id); err != nil {
				return fmt.Errorf("release leader lease: %w", err)
			}
			d.logger().Info(ctx, "instance released leadership (lease released)", "instance_id", d.id)
		}
		return nil
	}

	if conn == nil {
		return nil
	}
	defer func() {
		conn.Release()
	}()

	if !wasLeader {
		return nil
	}

	if err := releaseLeaderLock(ctx, conn); err != nil {
		return err
	}

	d.logger().Info(ctx, "instance released leadership", "instance_id", d.id)
	return nil
}

func (d *Daemon) dropLeaderConnection(ctx context.Context) {
	d.mu.Lock()
	conn := d.leaderConn
	wasLeader := d.isLeader
	d.leaderConn = nil
	d.isLeader = false
	d.mu.Unlock()

	if wasLeader {
		d.logger().Info(ctx, "instance lost leadership", "instance_id", d.id)
	}

	if conn != nil {
		conn.Release()
	}
}

// IsRunning returns true if the daemon is currently started and running.
func (d *Daemon) IsRunning() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.started
}

// IsLeader returns true if this instance currently holds the leader role.
func (d *Daemon) IsLeader() bool {
	return d.leaderActive()
}

func (d *Daemon) leaderActive() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.isLeader
}

func (d *Daemon) setLeader(isLeader bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.isLeader = isLeader
}

func (d *Daemon) shutdownTimeout() time.Duration {
	if d.config.ShutdownTimeout <= 0 {
		return defaultShutdownTimeout
	}

	return d.config.ShutdownTimeout
}

func (d *Daemon) assignmentPollInterval() time.Duration {
	interval := d.config.RebalanceInterval
	if interval <= 0 {
		return defaultAssignmentPollInterval
	}

	if interval < time.Second {
		return time.Second
	}

	return interval
}

func (d *Daemon) logger() store.Logger {
	if d.config.Logger == nil {
		return store.NoOpLogger{}
	}

	return d.config.Logger
}

func (d *Daemon) projectionNames() []string {
	names := make([]string, 0, len(d.registrations))
	for _, entry := range d.registrations {
		names = append(names, entry.name)
	}

	return names
}

func (d *Daemon) registrationFor(name string) (registration, bool) {
	entry, ok := d.registrationByName[name]
	return entry, ok
}

func (d *Daemon) projectorInstancesTable() string {
	return resolvedTableName(d.config.ProjectorInstancesTable, postgres.DefaultProjectorInstancesTable)
}

func (d *Daemon) projectionAssignmentsTable() string {
	return resolvedTableName(d.config.ProjectionAssignmentsTable, postgres.DefaultProjectionAssignmentsTable)
}

func (d *Daemon) projectionCheckpointsTable() string {
	return resolvedTableName(d.config.ProjectionCheckpointsTable, postgres.DefaultProjectionCheckpointsTable)
}

func (d *Daemon) projectionGapSkipsTable() string {
	return resolvedTableName(d.config.ProjectionGapSkipsTable, postgres.DefaultProjectionGapSkipsTable)
}

func (d *Daemon) startupInstanceCleanupThreshold() time.Duration {
	timeout := d.config.HeartbeatTimeout
	if timeout <= 0 {
		timeout = DefaultConfig().HeartbeatTimeout
	}

	if timeout > time.Duration((1<<63-1)/startupCleanupMultiplier) {
		return time.Duration(1<<63 - 1)
	}

	return timeout * startupCleanupMultiplier
}

func (d *Daemon) staleGapThreshold() time.Duration {
	if d.config.StaleGapThreshold <= 0 {
		return DefaultConfig().StaleGapThreshold
	}

	return d.config.StaleGapThreshold
}

func (d *Daemon) staleGapHarborLag() int {
	lag := d.config.StaleGapHarborLag
	if lag < 0 {
		lag = DefaultConfig().StaleGapHarborLag
	}

	batchSize := d.config.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultConfig().BatchSize
	}
	maxLag := batchSize - 1
	if maxLag < 0 {
		return 0
	}
	if lag > maxLag {
		return maxLag
	}

	return lag
}

func (d *Daemon) finishProjection(name string, done chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if currentDone, ok := d.projectionDone[name]; ok && currentDone == done {
		delete(d.projectionDone, name)
		delete(d.runningProjections, name)
	}
}

func (d *Daemon) getProcessingCancel() context.CancelFunc {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.processingCancel
}

func applyOptions(opts ...Option) Config { //nolint:gocyclo // sequential validation of independent config fields
	config := DefaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}

	defaults := DefaultConfig()
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.MaxPollInterval < config.PollInterval {
		config.MaxPollInterval = defaults.MaxPollInterval
		if config.MaxPollInterval < config.PollInterval {
			config.MaxPollInterval = config.PollInterval
		}
	}
	if config.DispatcherInterval <= 0 {
		config.DispatcherInterval = defaults.DispatcherInterval
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if config.HeartbeatTimeout <= 0 {
		config.HeartbeatTimeout = defaults.HeartbeatTimeout
	}
	if config.RebalanceInterval <= 0 {
		config.RebalanceInterval = defaults.RebalanceInterval
	}
	if config.BatchPause < 0 {
		config.BatchPause = defaults.BatchPause
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = defaults.BatchTimeout
	}
	if config.DetachedApplyTimeout <= 0 {
		// The effective apply budget is min(BatchTimeout, DetachedApplyTimeout),
		// so the default only has to be generous; BatchTimeout still caps it.
		config.DetachedApplyTimeout = defaults.DetachedApplyTimeout
	}
	if config.CheckpointTimeout <= 0 {
		config.CheckpointTimeout = defaults.CheckpointTimeout
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = defaults.ShutdownTimeout
	}
	if config.StaleGapThreshold <= 0 {
		config.StaleGapThreshold = defaults.StaleGapThreshold
	}
	if config.StaleGapHarborLag < 0 {
		config.StaleGapHarborLag = defaults.StaleGapHarborLag
	}
	if config.BatchSize > 0 && config.StaleGapHarborLag >= config.BatchSize {
		config.StaleGapHarborLag = config.BatchSize - 1
	}
	if config.MaxConsecutiveFailures <= 0 {
		config.MaxConsecutiveFailures = defaults.MaxConsecutiveFailures
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}
	if config.ProjectorInstancesTable == "" {
		config.ProjectorInstancesTable = defaults.ProjectorInstancesTable
	}
	if config.ProjectionAssignmentsTable == "" {
		config.ProjectionAssignmentsTable = defaults.ProjectionAssignmentsTable
	}
	if config.ProjectionCheckpointsTable == "" {
		config.ProjectionCheckpointsTable = defaults.ProjectionCheckpointsTable
	}
	if config.ProjectionGapSkipsTable == "" {
		config.ProjectionGapSkipsTable = defaults.ProjectionGapSkipsTable
	}
	if config.ProjectorLeaderLeasesTable == "" {
		config.ProjectorLeaderLeasesTable = defaults.ProjectorLeaderLeasesTable
	}
	if config.DispatcherStrategy == "" {
		config.DispatcherStrategy = defaults.DispatcherStrategy
	}

	return config
}

func newDispatcher(db PgxPool, eventStore projectorStore, config *Config) dispatcher.Dispatcher {
	if config.DispatcherStrategy == DispatcherStrategyNotify {
		return dispatcher.NewNotifyDispatcher(
			config.NotifyConnectionString,
			config.NotifyChannel,
			db,
			eventStore,
			config.Logger,
		)
	}

	return dispatcher.NewPollDispatcher(db, eventStore, config.DispatcherInterval, config.Logger)
}

func applyAssignments(ctx context.Context, tx pgx.Tx, assignmentsTable string, assignments map[string]uuid.UUID) error {
	if len(assignments) == 0 {
		//nolint:gosec // G201: table name comes from trusted configuration
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s
			SET instance_id = NULL, updated_at = NOW()
			WHERE instance_id IS NOT NULL
		`, assignmentsTable)); err != nil {
			return fmt.Errorf("clear existing assignments: %w", err)
		}
		return nil
	}

	if err := postgres.SetAssignments(ctx, tx, assignmentsTable, assignments); err != nil {
		return fmt.Errorf("set assignments: %w", err)
	}

	return nil
}

func (d *Daemon) ensureProjectionOwnership(ctx context.Context, tx pgx.Tx, projectionName string) (bool, error) {
	var instanceID *uuid.UUID
	//nolint:gosec // G201: table name comes from trusted configuration
	query := fmt.Sprintf(`
		SELECT instance_id
		FROM %s
		WHERE projection_name = $1
		FOR UPDATE
	`, d.projectionAssignmentsTable())
	if err := tx.QueryRow(ctx, query, projectionName).Scan(&instanceID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return instanceID != nil && *instanceID == d.id, nil
}

func nextPollInterval(current, maxInterval, base time.Duration) time.Duration {
	if current <= 0 {
		return base
	}
	if maxInterval <= 0 {
		maxInterval = current
	}

	next := current * 2
	if next < current {
		return maxInterval
	}
	if next > maxInterval {
		return maxInterval
	}

	return next
}

func resolvedTableName(tableName, defaultTableName string) string {
	if strings.TrimSpace(tableName) == "" {
		return defaultTableName
	}

	return tableName
}

func tryAcquireLeaderLock(ctx context.Context, conn *pgxpool.Conn) (bool, error) {
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", postgres.LeaderLockKey).Scan(&acquired); err != nil {
		return false, fmt.Errorf("acquire leader advisory lock: %w", err)
	}

	return acquired, nil
}

func releaseLeaderLock(ctx context.Context, conn *pgxpool.Conn) error {
	var released bool
	if err := conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", postgres.LeaderLockKey).Scan(&released); err != nil {
		return fmt.Errorf("release leader advisory lock: %w", err)
	}
	if !released {
		return fmt.Errorf("release leader advisory lock: lock not held")
	}

	return nil
}
