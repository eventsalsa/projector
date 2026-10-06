package projector

import (
	"context"
	"errors"
	"testing"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"
)

type stubTxProjection struct {
	name    string
	handled []store.PersistedEvent
	err     error
}

func (p *stubTxProjection) Name() string { return p.name }

//nolint:gocritic // hugeParam: implements the Projection contract
func (p *stubTxProjection) Handle(_ context.Context, _ pgx.Tx, event store.PersistedEvent) error {
	if p.err != nil {
		return p.err
	}
	p.handled = append(p.handled, event)
	return nil
}

type stubDetachedProjection struct {
	name    string
	handled []store.PersistedEvent
	err     error
}

func (p *stubDetachedProjection) Name() string { return p.name }

//nolint:gocritic // hugeParam: implements the DetachedProjection contract
func (p *stubDetachedProjection) Handle(_ context.Context, event store.PersistedEvent) error {
	if p.err != nil {
		return p.err
	}
	p.handled = append(p.handled, event)
	return nil
}

type stubBatchProjection struct {
	name  string
	calls int
	err   error
}

func (p *stubBatchProjection) Name() string { return p.name }

func (p *stubBatchProjection) Handle(_ context.Context, _ pgx.Tx, _ []store.PersistedEvent) error {
	p.calls++
	return p.err
}

type stubDetachedBatchProjection struct {
	name  string
	calls int
	err   error
}

func (p *stubDetachedBatchProjection) Name() string { return p.name }

func (p *stubDetachedBatchProjection) Handle(_ context.Context, _ []store.PersistedEvent) error {
	p.calls++
	return p.err
}

func unitTestEvent(position int64, streamType string) store.PersistedEvent {
	return store.PersistedEvent{
		GlobalPosition: position,
		StreamType:     streamType,
		StreamID:       streamType,
		StreamVersion:  position,
	}
}

func TestRegistryAddInfersHandlerShape(t *testing.T) {
	tests := []struct {
		name          string
		projection    any
		transactional bool
	}{
		{"transactional per-event", &stubTxProjection{name: "a"}, true},
		{"detached per-event", &stubDetachedProjection{name: "b"}, false},
		{"transactional batch", &stubBatchProjection{name: "c"}, true},
		{"detached batch", &stubDetachedBatchProjection{name: "d"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewRegistry()
			if err := registry.Add(tc.projection); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
			if len(registry.registrations) != 1 {
				t.Fatalf("registrations = %d, want 1", len(registry.registrations))
			}
			if got := registry.registrations[0].applier.transactional(); got != tc.transactional {
				t.Fatalf("transactional() = %v, want %v", got, tc.transactional)
			}
		})
	}
}

func TestRegistryRejectsInvalidRegistrations(t *testing.T) {
	if err := NewRegistry().Add(nil); !errors.Is(err, ErrNilProjection) {
		t.Fatalf("Add(nil) error = %v, want %v", err, ErrNilProjection)
	}

	var typedNil *stubTxProjection
	registry := NewRegistry()
	if err := registry.Add(typedNil); !errors.Is(err, ErrNilProjection) {
		t.Fatalf("Add(typed nil) error = %v, want %v", err, ErrNilProjection)
	}

	if err := NewRegistry().AddProjection(&stubTxProjection{}); !errors.Is(err, ErrEmptyProjectionName) {
		t.Fatalf("AddProjection(empty name) error = %v, want %v", err, ErrEmptyProjectionName)
	}

	duplicates := NewRegistry()
	if err := duplicates.AddProjection(&stubTxProjection{name: "dup"}); err != nil {
		t.Fatalf("first Add() error = %v", err)
	}
	if err := duplicates.AddProjection(&stubTxProjection{name: "dup"}); !errors.Is(err, ErrDuplicateProjectionName) {
		t.Fatalf("duplicate Add() error = %v, want %v", err, ErrDuplicateProjectionName)
	}

	if err := NewRegistry().Add(struct{}{}); !errors.Is(err, ErrUnknownProjectionShape) {
		t.Fatalf("Add(struct{}{}) error = %v, want %v", err, ErrUnknownProjectionShape)
	}

	if err := NewRegistry().validate(); err != nil {
		t.Fatalf("validate(empty registry) error = %v, want nil", err)
	}
}

func TestRegistryFiltersLimitAppliedEvents(t *testing.T) {
	inner := &stubDetachedProjection{name: "filtered"}
	registry := NewRegistry()
	if err := registry.Add(inner, OnStreamTypes("order"), OnEventTypes("OrderPlaced")); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	events := []store.PersistedEvent{
		{GlobalPosition: 1, StreamType: "order", EventType: "OrderPlaced"},
		{GlobalPosition: 2, StreamType: "order", EventType: "OrderCancelled"},
		{GlobalPosition: 3, StreamType: "user", EventType: "OrderPlaced"},
	}

	handled, err := registry.registrations[0].applier.apply(context.Background(), nil, events)
	if err != nil {
		t.Fatalf("apply() error = %v", err)
	}
	if handled != 1 {
		t.Fatalf("handled = %d, want 1", handled)
	}
	if len(inner.handled) != 1 || inner.handled[0].GlobalPosition != 1 {
		t.Fatalf("inner handled = %v, want only position 1", inner.handled)
	}
}

func TestFilteredApplierForwardsTransactional(t *testing.T) {
	filtered := wrapFilters(newTxEventApplier(&stubTxProjection{name: "a"}), filterSpec{streamTypes: stringSet([]string{"x"})})
	if !filtered.transactional() {
		t.Fatal("filtered transactional applier reports non-transactional")
	}

	detached := wrapFilters(newDetachedEventApplier(&stubDetachedProjection{name: "b"}), filterSpec{eventTypes: stringSet([]string{"y"})})
	if detached.transactional() {
		t.Fatal("filtered detached applier reports transactional")
	}
}

func TestFromProjectionsRegistersClassicShape(t *testing.T) {
	registry := FromProjections(&stubTxProjection{name: "alpha"}, &stubTxProjection{name: "beta"})
	if err := registry.validate(); err != nil {
		t.Fatalf("validate() error = %v", err)
	}
	if len(registry.registrations) != 2 {
		t.Fatalf("registrations = %d, want 2", len(registry.registrations))
	}
	if !registry.registrations[0].applier.transactional() {
		t.Fatal("classic projection should be transactional")
	}
}

func TestBatchApplierCallsHandlerOnce(t *testing.T) {
	projection := &stubBatchProjection{name: "batch"}
	applier := newTxBatchApplier(projection)

	handled, err := applier.apply(context.Background(), nil, []store.PersistedEvent{unitTestEvent(1, "a"), unitTestEvent(2, "a")})
	if err != nil {
		t.Fatalf("apply() error = %v", err)
	}
	if projection.calls != 1 {
		t.Fatalf("handler calls = %d, want 1", projection.calls)
	}
	if handled != 2 {
		t.Fatalf("handled = %d, want 2", handled)
	}
}
