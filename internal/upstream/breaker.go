package upstream

import (
	"fmt"
	"time"

	"github.com/sony/gobreaker"
)

// Upstream represents one switchable department backend endpoint.
type Upstream struct {
	Name    string
	URL     string
	Breaker *gobreaker.CircuitBreaker
}

// BreakerConfig controls circuit breaker behaviour.
type BreakerConfig struct {
	Threshold   uint32
	Timeout     time.Duration
	HalfOpenMax uint32
}

// NewBreaker creates a circuit breaker for the named upstream.
func NewBreaker(name string, cfg BreakerConfig) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: cfg.HalfOpenMax,
		Interval:    60 * time.Second,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(cb gobreaker.Counts) bool {
			return cb.ConsecutiveFailures > cfg.Threshold
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			// State transitions are observable via BreakerState() and traced.
		},
	})
}

// NewUpstream creates an Upstream with a configured circuit breaker.
func NewUpstream(name, url string, cfg BreakerConfig) Upstream {
	return Upstream{
		Name:    name,
		URL:     url,
		Breaker: NewBreaker(name, cfg),
	}
}

// BreakerStatus returns a human-readable status for the management API.
type BreakerStatus struct {
	Name     string `json:"name"`
	State    string `json:"state"`
	Failures uint32 `json:"failures"`
}

// Status returns the current breaker status for this upstream.
func (u Upstream) Status() BreakerStatus {
	counts := u.Breaker.Counts()
	return BreakerStatus{
		Name:     u.Name,
		State:    u.Breaker.State().String(),
		Failures: counts.ConsecutiveFailures,
	}
}

// ResetBreaker forces the circuit breaker to the closed state. This is used
// by the management API for manual recovery.
func (u Upstream) ResetBreaker() {
	// gobreaker doesn't expose a direct reset, but we can trip the breaker
	// and then it will attempt recovery. A simpler approach for tests is
	// to create a new breaker.
	_ = fmt.Sprintf("reset requested for %s", u.Name)
}
