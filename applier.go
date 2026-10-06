package projector

import (
	"context"
	"fmt"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"
)

// applier is the daemon's normalized view of a projection handler. The four
// public projection shapes are adapted to this single interface so the batch
// loop, the executors, and concern decorators never branch on the handler shape.
type applier interface {
	// apply applies the events to the read model. For transactional appliers the
	// provided tx also owns the checkpoint write. Detached appliers ignore tx and
	// receive nil.
	apply(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) (int, error)

	// transactional reports whether the daemon must hold a transaction open
	// across apply so the read model and the checkpoint commit atomically.
	transactional() bool
}

type txEventApplier struct {
	projection Projection
}

func newTxEventApplier(p Projection) applier {
	return txEventApplier{projection: p}
}

func (a txEventApplier) apply(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) (int, error) {
	for i := range events {
		if err := a.projection.Handle(ctx, tx, events[i]); err != nil {
			return i, fmt.Errorf("handle event %s for projection %s: %w", events[i].EventID, a.projection.Name(), err)
		}
	}

	return len(events), nil
}

func (txEventApplier) transactional() bool { return true }

type detachedEventApplier struct {
	projection DetachedProjection
}

func newDetachedEventApplier(p DetachedProjection) applier {
	return detachedEventApplier{projection: p}
}

func (a detachedEventApplier) apply(ctx context.Context, _ pgx.Tx, events []store.PersistedEvent) (int, error) {
	for i := range events {
		if err := a.projection.Handle(ctx, events[i]); err != nil {
			return i, fmt.Errorf("handle event %s for projection %s: %w", events[i].EventID, a.projection.Name(), err)
		}
	}

	return len(events), nil
}

func (detachedEventApplier) transactional() bool { return false }

type txBatchApplier struct {
	projection BatchProjection
}

func newTxBatchApplier(p BatchProjection) applier {
	return txBatchApplier{projection: p}
}

func (a txBatchApplier) apply(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) (int, error) {
	if err := a.projection.Handle(ctx, tx, events); err != nil {
		return 0, fmt.Errorf("handle batch for projection %s: %w", a.projection.Name(), err)
	}

	return len(events), nil
}

func (txBatchApplier) transactional() bool { return true }

type detachedBatchApplier struct {
	projection DetachedBatchProjection
}

func newDetachedBatchApplier(p DetachedBatchProjection) applier {
	return detachedBatchApplier{projection: p}
}

func (a detachedBatchApplier) apply(ctx context.Context, _ pgx.Tx, events []store.PersistedEvent) (int, error) {
	if err := a.projection.Handle(ctx, events); err != nil {
		return 0, fmt.Errorf("handle batch for projection %s: %w", a.projection.Name(), err)
	}

	return len(events), nil
}

func (detachedBatchApplier) transactional() bool { return false }

// filterSpec is the resolved form of the registration filter options.
type filterSpec struct {
	streamTypes map[string]struct{}
	eventTypes  map[string]struct{}
}

func (s filterSpec) empty() bool {
	return len(s.streamTypes) == 0 && len(s.eventTypes) == 0
}

//nolint:gocritic // hugeParam: matches the store.PersistedEvent value contract
func (s filterSpec) matches(event store.PersistedEvent) bool {
	if len(s.streamTypes) > 0 {
		if _, ok := s.streamTypes[event.StreamType]; !ok {
			return false
		}
	}

	if len(s.eventTypes) > 0 {
		if _, ok := s.eventTypes[event.EventType]; !ok {
			return false
		}
	}

	return true
}

// filteredApplier narrows the batch to events matching the registration filter.
// Filtered-out events are skipped without error, so the executor still advances
// the checkpoint to the unscoped safe frontier.
type filteredApplier struct {
	inner applier
	spec  filterSpec
}

func wrapFilters(inner applier, spec filterSpec) applier {
	if spec.empty() {
		return inner
	}

	return filteredApplier{inner: inner, spec: spec}
}

func (f filteredApplier) apply(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) (int, error) {
	filtered := make([]store.PersistedEvent, 0, len(events))
	for i := range events {
		if f.spec.matches(events[i]) {
			filtered = append(filtered, events[i])
		}
	}

	if len(filtered) == 0 {
		return 0, nil
	}

	return f.inner.apply(ctx, tx, filtered)
}

func (f filteredApplier) transactional() bool { return f.inner.transactional() }
