package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"regdispatch/internal/clock"
	"regdispatch/internal/store"
)

// Task is a periodic background job with graceful shutdown support.
type Task struct {
	Name       string
	Interval   time.Duration
	MaxRetries int
	Run        func(ctx context.Context) error
}

// Scheduler runs periodic background tasks using Ticker. Each task has
// independent retry and backoff. The scheduler can be stopped gracefully.
type Scheduler struct {
	tasks   []*Task
	clk     clock.Clock
	log     zerolog.Logger
	store   *store.Store
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	running bool
}

// New creates a scheduler with the given clock, logger, and store.
func New(clk clock.Clock, log zerolog.Logger, st *store.Store) *Scheduler {
	return &Scheduler{clk: clk, log: log, store: st}
}

// Register adds a periodic task to the scheduler.
func (s *Scheduler) Register(task *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, task)
}

// Start begins executing all registered tasks. Each task runs in its own
// goroutine with a ticker. Returns an error if already running.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("scheduler already running")
	}
	s.running = true
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	tasks := make([]*Task, len(s.tasks))
	copy(tasks, s.tasks)
	s.mu.Unlock()

	for _, task := range tasks {
		s.wg.Add(1)
		go s.runTask(ctx, task)
	}
	return nil
}

// runTask executes a task on a ticker interval with retry and exponential
// backoff. The goroutine exits when the context is cancelled.
func (s *Scheduler) runTask(ctx context.Context, task *Task) {
	defer s.wg.Done()
	ticker := s.clk.NewTicker(task.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			s.executeWithRetry(ctx, task)
		}
	}
}

// executeWithRetry runs a task with up to MaxRetries retries and exponential
// backoff. After exhausting retries, the error is logged as a permanent
// failure.
func (s *Scheduler) executeWithRetry(ctx context.Context, task *Task) {
	var lastErr error
	for attempt := 0; attempt <= task.MaxRetries; attempt++ {
		if err := task.Run(ctx); err != nil {
			lastErr = err
			s.log.Warn().Err(err).Str("task", task.Name).Int("attempt", attempt+1).
				Msg("task failed, will retry")
			if attempt < task.MaxRetries {
				backoff := time.Duration(1<<attempt) * time.Second
				select {
				case <-ctx.Done():
					return
				case <-s.clk.After(backoff):
					continue
				}
			}
		} else {
			return
		}
	}
	if lastErr != nil {
		s.log.Error().Err(lastErr).Str("task", task.Name).
			Msg("task permanently failed after all retries")
	}
}

// Stop gracefully shuts down the scheduler, waiting for in-flight tasks
// to complete or the shutdown timeout to elapse.
func (s *Scheduler) Stop(timeout time.Duration) {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		s.log.Warn().Dur("timeout", timeout).Msg("scheduler stop timed out")
	}
}

// IsRunning returns true if the scheduler has been started and not stopped.
func (s *Scheduler) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// Tasks returns a copy of registered task names.
func (s *Scheduler) Tasks() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, len(s.tasks))
	for i, t := range s.tasks {
		names[i] = t.Name
	}
	return names
}
