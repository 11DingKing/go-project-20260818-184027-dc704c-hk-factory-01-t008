package errorsx

import (
	"errors"
	"fmt"
)

// Sentinel error definitions. These are wrapped with %w at call sites so
// that errors.Is can match them across the entire call chain.
var (
	ErrNotFound           = errors.New("entity not found")
	ErrAlreadyExists      = errors.New("entity already exists")
	ErrInvalidTransition  = errors.New("invalid state transition")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrConcurrentUpdate   = errors.New("concurrent version conflict")
	ErrDispatchTimeout    = errors.New("dispatch acknowledgment timed out")
	ErrPartialFailure     = errors.New("partial dispatch failure")
	ErrCompensationFailed = errors.New("compensation failed")
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
	ErrCircuitOpen        = errors.New("upstream circuit breaker open")
	ErrAllUpstreamsDown   = errors.New("all upstreams unavailable")
)

// BusinessError wraps an underlying error with a human-readable message and
// a category suitable for mapping to HTTP status codes.
type BusinessError struct {
	Cause    error
	Category string
	Message  string
}

func (be *BusinessError) Error() string {
	if be.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", be.Category, be.Message, be.Cause)
	}
	return fmt.Sprintf("%s: %s", be.Category, be.Message)
}

func (be *BusinessError) Unwrap() error { return be.Cause }

func Wrap(category string, message string, cause error) *BusinessError {
	return &BusinessError{Cause: cause, Category: category, Message: message}
}

// NotFound creates a not-found error wrapping ErrNotFound.
func NotFound(entity, id string) *BusinessError {
	return Wrap("not_found", fmt.Sprintf("%s %s", entity, id), ErrNotFound)
}

// InvalidTransition wraps ErrInvalidTransition with transition context.
func InvalidTransition(from, to, entity string) *BusinessError {
	return Wrap(
		"invalid_transition",
		fmt.Sprintf("%s cannot transition from %s to %s", entity, from, to),
		ErrInvalidTransition,
	)
}

// PartialFailure wraps ErrPartialFailure with department failure details.
func PartialFailure(changeID string, failed []string) *BusinessError {
	return Wrap(
		"partial_failure",
		fmt.Sprintf("change %s failed for departments %v", changeID, failed),
		ErrPartialFailure,
	)
}

// IsNotFound returns true if err chain contains ErrNotFound.
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }

// IsInvalidTransition returns true if err chain contains ErrInvalidTransition.
func IsInvalidTransition(err error) bool { return errors.Is(err, ErrInvalidTransition) }

// IsPartialFailure returns true if err chain contains ErrPartialFailure.
func IsPartialFailure(err error) bool { return errors.Is(err, ErrPartialFailure) }

// IsConcurrentUpdate returns true if err chain contains ErrConcurrentUpdate.
func IsConcurrentUpdate(err error) bool { return errors.Is(err, ErrConcurrentUpdate) }

// IsCircuitOpen returns true if err chain contains ErrCircuitOpen.
func IsCircuitOpen(err error) bool { return errors.Is(err, ErrCircuitOpen) }
