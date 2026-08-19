package scheduler

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"regdispatch/internal/clock"
	"regdispatch/internal/orchestrator"
	"regdispatch/internal/store"
)

// RegisterDefaultTasks adds the standard background tasks: dispatch expiry
// processing, event log compaction, and subscriber stale check.
func RegisterDefaultTasks(sched *Scheduler, orch *orchestrator.Orchestrator, repos *store.Repositories, clk clock.Clock, log zerolog.Logger, compactInterval time.Duration, retainCount int) {
	sched.Register(&Task{
		Name:       "dispatch_expiry_processor",
		Interval:   10 * time.Second,
		MaxRetries: 3,
		Run: func(ctx context.Context) error {
			n, err := orch.ProcessExpired(ctx)
			if err != nil {
				return err
			}
			if n > 0 {
				log.Debug().Int("processed", n).Msg("dispatch expiry task ran")
			}
			return nil
		},
	})

	sched.Register(&Task{
		Name:       "event_log_compaction",
		Interval:   compactInterval,
		MaxRetries: 2,
		Run: func(ctx context.Context) error {
			eventLog, ok := repos.EventLog.(interface {
				Compact(ctx context.Context, retainCount int) (int64, error)
			})
			if !ok {
				return nil
			}
			n, err := eventLog.Compact(ctx, retainCount)
			if err != nil {
				return err
			}
			if n > 0 {
				log.Info().Int64("deleted", n).Msg("event log compacted")
			}
			return nil
		},
	})

	sched.Register(&Task{
		Name:       "subscriber_stale_check",
		Interval:   30 * time.Second,
		MaxRetries: 3,
		Run: func(ctx context.Context) error {
			stale, err := repos.Subscribers.ListStale(ctx, clk.Now().Add(-5*time.Minute))
			if err != nil {
				return err
			}
			for _, so := range stale {
				log.Debug().Str("subscriber", so.SubscriberID).Str("topic", so.Topic).
					Msg("stale subscriber detected, will recover offset on reconnect")
			}
			return nil
		},
	})

	sched.Register(&Task{
		Name:       "compensation_retry",
		Interval:   15 * time.Second,
		MaxRetries: 3,
		Run: func(ctx context.Context) error {
			pending, err := repos.Compensation.ListPending(ctx)
			if err != nil {
				return err
			}
			for _, cr := range pending {
				log.Warn().Str("compensation_id", cr.ID).Str("change_id", cr.ChangeID).
					Msg("pending compensation found, manual intervention may be needed")
			}
			return nil
		},
	})
}
