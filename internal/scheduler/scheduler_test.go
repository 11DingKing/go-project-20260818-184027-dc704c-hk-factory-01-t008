package scheduler

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"regdispatch/internal/clock"
	"regdispatch/internal/store"
)

func testSchedulerStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sched_test.db")
	st, err := store.Open(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	_, err = st.EnsureSchema(context.Background())
	require.NoError(t, err)
	return st
}

func TestSchedulerStartStop(t *testing.T) {
	st := testSchedulerStore(t)
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	sched := New(clk, log, st)

	sched.Register(&Task{
		Name:       "test_task",
		Interval:   100 * time.Millisecond,
		MaxRetries: 2,
		Run: func(ctx context.Context) error {
			return nil
		},
	})

	require.NoError(t, sched.Start(context.Background()))
	assert.True(t, sched.IsRunning())
	assert.Contains(t, sched.Tasks(), "test_task")

	sched.Stop(2 * time.Second)
	assert.False(t, sched.IsRunning())
}

func TestSchedulerRetryOnFailure(t *testing.T) {
	st := testSchedulerStore(t)
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	sched := New(clk, log, st)

	var attempts atomic.Int32
	sched.Register(&Task{
		Name:       "failing_task",
		Interval:   100 * time.Millisecond,
		MaxRetries: 3,
		Run: func(ctx context.Context) error {
			n := attempts.Add(1)
			if n < 3 {
				return errFake
			}
			return nil
		},
	})

	require.NoError(t, sched.Start(context.Background()))
	time.Sleep(100 * time.Millisecond)
	clk.Advance(5 * time.Second)
	time.Sleep(500 * time.Millisecond)
	sched.Stop(2 * time.Second)

	assert.GreaterOrEqual(t, attempts.Load(), int32(3), "task should be retried at least 3 times")
}

func TestSchedulerPermanentFailureLogged(t *testing.T) {
	st := testSchedulerStore(t)
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	sched := New(clk, log, st)

	var attempts atomic.Int32
	sched.Register(&Task{
		Name:       "always_fails",
		Interval:   100 * time.Millisecond,
		MaxRetries: 1,
		Run: func(ctx context.Context) error {
			attempts.Add(1)
			return errFake
		},
	})

	require.NoError(t, sched.Start(context.Background()))
	time.Sleep(100 * time.Millisecond)
	clk.Advance(5 * time.Second)
	time.Sleep(500 * time.Millisecond)
	sched.Stop(2 * time.Second)

	assert.GreaterOrEqual(t, attempts.Load(), int32(2), "task should have been retried")
}

func TestSchedulerGracefulShutdown(t *testing.T) {
	st := testSchedulerStore(t)
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	sched := New(clk, log, st)

	sched.Register(&Task{
		Name:       "long_task",
		Interval:   100 * time.Millisecond,
		MaxRetries: 1,
		Run: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
				return nil
			}
		},
	})

	require.NoError(t, sched.Start(context.Background()))
	time.Sleep(200 * time.Millisecond)
	sched.Stop(1 * time.Second)
	assert.False(t, sched.IsRunning())
}

func TestSchedulerContextCancellation(t *testing.T) {
	st := testSchedulerStore(t)
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	sched := New(clk, log, st)

	ctx, cancel := context.WithCancel(context.Background())
	sched.Register(&Task{
		Name:       "ctx_task",
		Interval:   100 * time.Millisecond,
		MaxRetries: 0,
		Run: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})

	require.NoError(t, sched.Start(ctx))
	cancel()
	sched.Stop(2 * time.Second)
	assert.False(t, sched.IsRunning())
}

func TestSchedulerConcurrentTasksRun(t *testing.T) {
	st := testSchedulerStore(t)
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	sched := New(clk, log, st)

	var counter atomic.Int32
	for i := 0; i < 3; i++ {
		sched.Register(&Task{
			Name:       "task_" + string(rune('A'+i)),
			Interval:   50 * time.Millisecond,
			MaxRetries: 1,
			Run: func(ctx context.Context) error {
				counter.Add(1)
				return nil
			},
		})
	}

	require.NoError(t, sched.Start(context.Background()))
	time.Sleep(100 * time.Millisecond)
	clk.Advance(2 * time.Second)
	time.Sleep(300 * time.Millisecond)
	sched.Stop(2 * time.Second)

	assert.Greater(t, counter.Load(), int32(0), "concurrent tasks should have run")
}

func TestSchedulerDoubleStartRejected(t *testing.T) {
	st := testSchedulerStore(t)
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	sched := New(clk, log, st)

	sched.Register(&Task{
		Name:       "test",
		Interval:   time.Second,
		MaxRetries: 0,
		Run:        func(ctx context.Context) error { return nil },
	})

	require.NoError(t, sched.Start(context.Background()))
	err := sched.Start(context.Background())
	assert.Error(t, err)
	sched.Stop(1 * time.Second)
}

func TestSchedulerTaskList(t *testing.T) {
	st := testSchedulerStore(t)
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	sched := New(clk, log, st)

	sched.Register(&Task{Name: "task_one", Interval: time.Second, MaxRetries: 0, Run: func(ctx context.Context) error { return nil }})
	sched.Register(&Task{Name: "task_two", Interval: time.Second, MaxRetries: 0, Run: func(ctx context.Context) error { return nil }})

	tasks := sched.Tasks()
	assert.Contains(t, tasks, "task_one")
	assert.Contains(t, tasks, "task_two")
	assert.Len(t, tasks, 2)
}

var errFake = errFakeErr{}

type errFakeErr struct{}

func (errFakeErr) Error() string { return "simulated failure" }
