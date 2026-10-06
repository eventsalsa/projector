package projector

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	// ErrNilRegistry indicates that a daemon was created without a projection registry.
	ErrNilRegistry = errors.New("daemon requires a projection registry")

	// ErrNilProjection indicates that a nil projection was registered.
	ErrNilProjection = errors.New("projection is nil")

	// ErrUnknownProjectionShape indicates that a registered value does not
	// implement any of the four projection handler interfaces.
	ErrUnknownProjectionShape = errors.New("projection does not implement a known handler shape")

	// ErrEmptyProjectionName indicates that a projection has an empty name.
	ErrEmptyProjectionName = errors.New("projection name is empty")

	// ErrDuplicateProjectionName indicates that a projection name was registered twice.
	ErrDuplicateProjectionName = errors.New("duplicate projection name")
)

// registration is a projection bound to the applier that normalizes its shape.
type registration struct { //nolint:govet // fieldalignment: readability over marginal memory savings
	name    string
	applier applier
}

// Registry collects the projections for a Daemon and selects the right
// processing path for each handler shape. Build one with NewRegistry, register
// projections with Add or one of the typed Add* methods, then pass it to New.
//
// A concrete type can implement only one shape, because Go does not allow two
// methods with the same name on a type. Registration therefore never has to
// choose between shapes.
type Registry struct { //nolint:govet // fieldalignment: readability over marginal memory savings
	registrations []registration
	names         map[string]struct{}
	err           error
}

// NewRegistry returns an empty projection registry.
func NewRegistry() *Registry {
	return &Registry{names: make(map[string]struct{})}
}

// FromProjections builds a registry from transactional per-event projections.
// It is a convenience for daemons that only use the classic Projection shape.
// Registration errors are recorded and surfaced by Daemon.Start.
func FromProjections(projections ...Projection) *Registry {
	registry := NewRegistry()
	for _, projection := range projections {
		//nolint:errcheck // registration errors are recorded on the registry and surfaced by Daemon.Start
		_ = registry.AddProjection(projection)
	}

	return registry
}

// Add registers a projection and infers its handler shape. It returns an error
// for nil values, empty or duplicate names, and values that implement none of
// the four projection interfaces.
func (r *Registry) Add(projection any, opts ...RegistrationOption) error {
	if isNilValue(projection) {
		return r.record(ErrNilProjection)
	}

	switch typed := projection.(type) {
	case Projection:
		return r.AddProjection(typed, opts...)
	case DetachedProjection:
		return r.AddDetached(typed, opts...)
	case BatchProjection:
		return r.AddBatch(typed, opts...)
	case DetachedBatchProjection:
		return r.AddDetachedBatch(typed, opts...)
	default:
		return r.record(fmt.Errorf("%w: %T", ErrUnknownProjectionShape, projection))
	}
}

// AddProjection registers a transactional per-event projection.
func (r *Registry) AddProjection(p Projection, opts ...RegistrationOption) error {
	if isNilValue(p) {
		return r.record(ErrNilProjection)
	}

	return r.add(p.Name(), newTxEventApplier(p), opts...)
}

// AddDetached registers a projection whose read model is outside the daemon's
// PostgreSQL database. The daemon applies it with no transaction held.
func (r *Registry) AddDetached(p DetachedProjection, opts ...RegistrationOption) error {
	if isNilValue(p) {
		return r.record(ErrNilProjection)
	}

	return r.add(p.Name(), newDetachedEventApplier(p), opts...)
}

// AddBatch registers a transactional projection that applies a whole batch in one call.
func (r *Registry) AddBatch(p BatchProjection, opts ...RegistrationOption) error {
	if isNilValue(p) {
		return r.record(ErrNilProjection)
	}

	return r.add(p.Name(), newTxBatchApplier(p), opts...)
}

// AddDetachedBatch registers a batch projection whose read model is outside the
// daemon's PostgreSQL database.
func (r *Registry) AddDetachedBatch(p DetachedBatchProjection, opts ...RegistrationOption) error {
	if isNilValue(p) {
		return r.record(ErrNilProjection)
	}

	return r.add(p.Name(), newDetachedBatchApplier(p), opts...)
}

func (r *Registry) add(name string, a applier, opts ...RegistrationOption) error {
	if strings.TrimSpace(name) == "" {
		return r.record(ErrEmptyProjectionName)
	}

	if _, exists := r.names[name]; exists {
		return r.record(fmt.Errorf("%w: %q", ErrDuplicateProjectionName, name))
	}

	var spec filterSpec
	for _, opt := range opts {
		if opt != nil {
			opt(&spec)
		}
	}

	r.names[name] = struct{}{}
	r.registrations = append(r.registrations, registration{
		name:    name,
		applier: wrapFilters(a, spec),
	})

	return nil
}

func (r *Registry) record(err error) error {
	if r.err == nil {
		r.err = err
	}

	return err
}

func (r *Registry) validate() error {
	if r == nil {
		return ErrNilRegistry
	}

	return r.err
}

// RegistrationOption configures how a projection is registered.
type RegistrationOption func(*filterSpec)

// OnStreamTypes limits a projection to the given stream types. Events from other
// stream types are skipped while checkpoints still advance to the unscoped
// safe frontier.
func OnStreamTypes(streamTypes ...string) RegistrationOption {
	return func(spec *filterSpec) {
		spec.streamTypes = stringSet(streamTypes)
	}
}

// OnEventTypes limits a projection to the given event types. Events with other
// event types are skipped while checkpoints still advance to the unscoped
// safe frontier.
func OnEventTypes(eventTypes ...string) RegistrationOption {
	return func(spec *filterSpec) {
		spec.eventTypes = stringSet(eventTypes)
	}
}

func stringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}

	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}

	return set
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
