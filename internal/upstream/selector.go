package upstream

import (
	"fmt"
	"sync"
	"sync/atomic"

	"regdispatch/internal/errorsx"
)

// Selector implements multi-upstream selection with round-robin rotation and
// automatic fallback. It is safe for concurrent use.
type Selector struct {
	mu        sync.RWMutex
	upstreams []Upstream
	index     atomic.Uint64
}

// NewSelector creates a selector with the given upstream list. The first
// upstream is primary; the rest are fallbacks.
func NewSelector(upstreams []Upstream) *Selector {
	return &Selector{upstreams: upstreams}
}

// UpstreamCount returns the number of configured upstreams.
func (s *Selector) UpstreamCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.upstreams)
}

// Next returns the next upstream in round-robin order. It only returns
// upstreams whose circuit breaker is not open. If all are open, it returns
// ErrCircuitOpen so the caller can report the failure.
func (s *Selector) Next() (Upstream, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.upstreams)
	if n == 0 {
		return Upstream{}, errorsx.Wrap("upstream", "no upstreams configured", errorsx.ErrAllUpstreamsDown)
	}
	start := int(s.index.Add(1)-1) % n
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		up := s.upstreams[idx]
		if up.Breaker.State().String() != "open" {
			return up, nil
		}
	}
	return Upstream{}, errorsx.Wrap("upstream", "all circuit breakers open", errorsx.ErrCircuitOpen)
}

// ByName returns the upstream with the given name.
func (s *Selector) ByName(name string) (Upstream, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, up := range s.upstreams {
		if up.Name == name {
			return up, nil
		}
	}
	return Upstream{}, fmt.Errorf("upstream %s not found", name)
}

// All returns a copy of the upstream list for status reporting.
func (s *Selector) All() []Upstream {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Upstream, len(s.upstreams))
	copy(result, s.upstreams)
	return result
}

// Replace swaps the upstream list at runtime. This is used by the management
// API to reconfigure upstreams without restart.
func (s *Selector) Replace(upstreams []Upstream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upstreams = upstreams
	s.index.Store(0)
}
