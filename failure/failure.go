// Package failure classifies projection errors so the daemon can decide whether
// to back off and retry or to stop the affected projection.
//
// A handler that hits a transient read-model failure (a network error, a rate
// limit, a 5xx response) should return failure.Retryable(err). A handler that
// knows retrying cannot help (a schema mismatch, a malformed document) should
// return failure.Permanent(err). Any other error is unclassified and follows the
// projection's configured failure policy.
//
// Use errors.Is or errors.As to inspect classification; both unwrap through
// fmt.Errorf("%w") wrapping, so the daemon and tests do not need to know how the
// handler annotated the error.
package failure

import "errors"

type class uint8

const (
	classRetryable class = iota
	classPermanent
)

// classifiedError carries the classification alongside the original error.
type classifiedError struct {
	err   error
	class class
}

func (e *classifiedError) Error() string { return e.err.Error() }

func (e *classifiedError) Unwrap() error { return e.err }

// Retryable marks err as a transient failure of the read model. The daemon backs
// off and retries the batch; a retryable failure is never treated as fatal.
// Retryable(nil) returns nil.
func Retryable(err error) error {
	if err == nil {
		return nil
	}

	return &classifiedError{err: err, class: classRetryable}
}

// Permanent marks err as a failure that retrying cannot fix. By default the
// daemon stops the affected projection; if a poison handler is registered, it can
// record the batch and let the projection advance past it. Permanent(nil)
// returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}

	return &classifiedError{err: err, class: classPermanent}
}

// IsRetryable reports whether err, or any error in its chain, was marked
// Retryable. When both classifications appear in one chain the outermost one
// wins, so a caller that re-wraps a Permanent error with Retryable changes the
// classification.
func IsRetryable(err error) bool {
	var classified *classifiedError
	if !errors.As(err, &classified) {
		return false
	}

	return classified.class == classRetryable
}

// IsPermanent reports whether err, or any error in its chain, was marked
// Permanent, using the same outermost-wins rule as IsRetryable.
func IsPermanent(err error) bool {
	var classified *classifiedError
	if !errors.As(err, &classified) {
		return false
	}

	return classified.class == classPermanent
}
