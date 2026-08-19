package clock

import (
	"context"
	"sync"
	"time"
)

// Clock provides an injectable time source so tests can control wall-clock
// without relying on real sleeps or network timing.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) Ticker
}

// Ticker is the injectable interface for time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock delegates to the standard library time package.
type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (RealClock) NewTicker(d time.Duration) Ticker {
	return &realTicker{t: time.NewTicker(d)}
}

type realTicker struct{ t *time.Ticker }

func (rt *realTicker) C() <-chan time.Time { return rt.t.C }
func (rt *realTicker) Stop()               { rt.t.Stop() }

// FakeClock is a manually-controlled clock for deterministic tests. It tracks
// all created tickers and fires them when Advance is called.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (fc *FakeClock) Now() time.Time {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.now
}

func (fc *FakeClock) Advance(d time.Duration) {
	fc.mu.Lock()
	fc.now = fc.now.Add(d)
	tickers := make([]*fakeTicker, len(fc.tickers))
	copy(tickers, fc.tickers)
	fc.mu.Unlock()
	for _, t := range tickers {
		t.fire(fc.now)
	}
}

func (fc *FakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	fc.mu.Lock()
	target := fc.now.Add(d)
	fc.mu.Unlock()
	ch <- target
	return ch
}

func (fc *FakeClock) NewTicker(d time.Duration) Ticker {
	ft := &fakeTicker{
		ch:   make(chan time.Time, 1),
		stop: make(chan struct{}),
		clk:  fc,
	}
	fc.mu.Lock()
	fc.tickers = append(fc.tickers, ft)
	fc.mu.Unlock()
	return ft
}

type fakeTicker struct {
	ch   chan time.Time
	stop chan struct{}
	clk  *FakeClock
}

func (ft *fakeTicker) C() <-chan time.Time { return ft.ch }
func (ft *fakeTicker) Stop() {
	select {
	case <-ft.stop:
	default:
		close(ft.stop)
		ft.clk.mu.Lock()
		defer ft.clk.mu.Unlock()
		for i, t := range ft.clk.tickers {
			if t == ft {
				ft.clk.tickers = append(ft.clk.tickers[:i], ft.clk.tickers[i+1:]...)
				return
			}
		}
	}
}

func (ft *fakeTicker) fire(now time.Time) {
	select {
	case <-ft.stop:
		return
	default:
	}
	select {
	case ft.ch <- now:
	default:
	}
}

// WaitForTick blocks until the context is cancelled or a tick is received.
func WaitForTick(ctx context.Context, t Ticker) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C():
		return nil
	}
}
