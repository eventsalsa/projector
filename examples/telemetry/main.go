// Package main demonstrates how to use the Observer interface to instrument
// eventsalsa/projector with structured logging and metrics adapters.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/eventsalsa/projector"
	"github.com/eventsalsa/store"
	storepostgres "github.com/eventsalsa/store/postgres"
)

// LoggingObserver logs batch and lifecycle events using structured logging (slog).
type LoggingObserver struct {
	logger *slog.Logger
}

//nolint:gocritic // hugeParam: matches Observer interface contract
func (l *LoggingObserver) OnBatchProcessed(ctx context.Context, stats projector.BatchStats) {
	if stats.Error != nil {
		l.logger.ErrorContext(ctx, "batch processing failed",
			"projection", stats.ProjectionName,
			"start_pos", stats.StartPosition,
			"last_pos", stats.LastPosition,
			"duration", stats.Duration,
			"error", stats.Error,
		)
		return
	}

	l.logger.InfoContext(ctx, "batch processed successfully",
		"projection", stats.ProjectionName,
		"start_pos", stats.StartPosition,
		"last_pos", stats.LastPosition,
		"head_pos", stats.HeadPosition,
		"lag", stats.Lag,
		"events_read", stats.EventsRead,
		"events_handled", stats.EventsHandled,
		"duration", stats.Duration,
		"stale_skipped", stats.StaleSkipped,
	)
}

func (l *LoggingObserver) OnHeartbeat(ctx context.Context, stats projector.DaemonStats) {
	l.logger.DebugContext(ctx, "heartbeat refreshed",
		"instance_id", stats.InstanceID,
		"is_leader", stats.IsLeader,
	)
}

func (l *LoggingObserver) OnGapDetected(ctx context.Context, stats projector.GapStats) {
	l.logger.WarnContext(ctx, "sequence gap detected",
		"projection", stats.ProjectionName,
		"gap_position", stats.GapPosition,
		"highest_visible", stats.HighestVisible,
		"stale_for", stats.StaleFor,
	)
}

func (l *LoggingObserver) OnGapSkipped(ctx context.Context, stats projector.GapStats) {
	l.logger.WarnContext(ctx, "safe harbor advanced past stale gap",
		"projection", stats.ProjectionName,
		"gap_position", stats.GapPosition,
		"highest_visible", stats.HighestVisible,
		"stale_for", stats.StaleFor,
	)
}

func (l *LoggingObserver) OnRebalance(ctx context.Context, assignments map[string]uuid.UUID) {
	l.logger.InfoContext(ctx, "rebalance assignments updated",
		"assignments_count", len(assignments),
	)
}

// MetricsObserver demonstrates an in-memory / Prometheus adapter embedding NoopObserver
// to implement only the methods it cares about.
type MetricsObserver struct {
	projector.NoopObserver
}

//nolint:gocritic // hugeParam: matches Observer interface contract
func (m *MetricsObserver) OnBatchProcessed(_ context.Context, stats projector.BatchStats) {
	status := "success"
	if stats.Error != nil {
		status = "error"
	}

	// Example metrics emission (e.g. prometheus / otel):
	// batchDurationHistogram.WithLabelValues(stats.ProjectionName, status).Observe(stats.Duration.Seconds())
	// lagGauge.WithLabelValues(stats.ProjectionName).Set(float64(stats.Lag))
	// checkpointPositionGauge.WithLabelValues(stats.ProjectionName).Set(float64(stats.LastPosition))
	// eventsHandledCounter.WithLabelValues(stats.ProjectionName).Add(float64(stats.EventsHandled))
	_ = status
	fmt.Printf("[METRICS] %s batch: lag=%d, handled=%d, duration=%v\n",
		stats.ProjectionName, stats.Lag, stats.EventsHandled, stats.Duration)
}

type ExampleProjection struct{}

func (p *ExampleProjection) Name() string {
	return "orders_summary"
}

func (p *ExampleProjection) Handle(_ context.Context, _ pgx.Tx, _ store.PersistedEvent) error {
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/eventsalsa_test?sslmode=disable"
	}

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	// Combine structured logging and metrics using MultiObserver
	loggingObs := &LoggingObserver{logger: slog.Default()}
	metricsObs := &MetricsObserver{}
	compositeObserver := projector.MultiObserver(loggingObs, metricsObs)

	registry := projector.NewRegistry()
	if err := registry.Add(&ExampleProjection{}, projector.OnStreamTypes("Order")); err != nil {
		slog.Error("failed to register projection", "error", err)
		os.Exit(1)
	}

	daemon := projector.New(
		db,
		eventStore,
		registry,
		projector.WithBatchSize(100),
		projector.WithPollInterval(500*time.Millisecond),
		projector.WithObserver(compositeObserver),
	)

	slog.Info("starting projector daemon with telemetry observer...")
	if err := daemon.Start(ctx); err != nil {
		slog.Error("daemon exited with error", "error", err)
		os.Exit(1)
	}
}
