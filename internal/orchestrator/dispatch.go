package orchestrator

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"regdispatch/internal/clock"
	"regdispatch/internal/domain/change"
	"regdispatch/internal/domain/department"
	"regdispatch/internal/domain/event"
	"regdispatch/internal/errorsx"
	"regdispatch/internal/store"
	"regdispatch/internal/upstream"
)

// Dispatcher handles multi-department batch dispatch, upstream forwarding,
// and dispatch task lifecycle management.
type Dispatcher struct {
	repos      *store.Repositories
	upstream   *upstream.Client
	clk        clock.Clock
	log        zerolog.Logger
	maxRetries int
	retryBase  time.Duration
	retryMax   time.Duration
}

// Dispatch sends a change to all relevant departments, appending events to
// the log and forwarding each via the upstream client. It is idempotent:
// re-dispatching an already-dispatched change is rejected.
func (d *Dispatcher) Dispatch(ctx context.Context, changeID, operator string) error {
	chg, err := d.repos.Changes.GetByID(ctx, changeID)
	if err != nil {
		return fmt.Errorf("get change for dispatch: %w", err)
	}
	newStatus, err := change.Transition(chg.Status, change.ActionDispatch)
	if err != nil {
		return fmt.Errorf("dispatch transition: %w", err)
	}
	depts := department.DepartmentsForChange(chg.ChangeType)
	if len(depts) == 0 {
		return errorsx.Wrap("dispatch", "no departments for change type "+chg.ChangeType, errorsx.ErrInvalidArgument)
	}
	tasks := make([]*change.DispatchTask, 0, len(depts))
	for _, dept := range depts {
		payload := event.Payload{
			ChangeID:       chg.ID,
			EnterpriseID:   chg.EnterpriseID,
			ChangeType:     chg.ChangeType,
			DepartmentCode: string(dept.Code),
			Before:         chg.BeforeSnapshot,
			After:          chg.AfterSnapshot,
			Operator:       operator,
		}
		payloadJSON, _ := event.EncodePayload(payload)
		entry := &event.Entry{
			Topic:          dept.Topic,
			ChangeID:       chg.ID,
			DepartmentCode: string(dept.Code),
			EventType:      event.TypeChangeDispatched,
			Payload:        payloadJSON,
			EventTime:      chg.EventTime,
			CreatedAt:      d.clk.Now(),
		}
		offset, err := d.repos.EventLog.Append(ctx, entry)
		if err != nil {
			return fmt.Errorf("append event log for %s: %w", dept.Code, err)
		}
		task := &change.DispatchTask{
			ID:             "disp-" + uuid.NewString()[:12],
			ChangeID:       chg.ID,
			DepartmentCode: string(dept.Code),
			Topic:          dept.Topic,
			Status:         change.DispatchPending,
			LogOffset:      offset,
			AttemptCount:   0,
			MaxAttempts:    d.maxRetries,
			CreatedAt:      d.clk.Now(),
			UpdatedAt:      d.clk.Now(),
		}
		tasks = append(tasks, task)
	}
	if err := d.repos.Dispatch.CreateBatch(ctx, tasks); err != nil {
		return fmt.Errorf("create dispatch tasks: %w", err)
	}
	if err := d.repos.Changes.UpdateStatus(ctx, chg.ID, newStatus); err != nil {
		return fmt.Errorf("update change status to dispatching: %w", err)
	}
	for _, task := range tasks {
		d.forwardToUpstream(ctx, task, chg)
	}
	d.log.Info().Str("change_id", changeID).Int("departments", len(tasks)).Msg("change dispatched")
	return nil
}

// forwardToUpstream sends a dispatch request to the upstream department backend.
// It updates the task status based on the response. Failures do not block
// the dispatch; the task enters timed_out for retry by the scheduler.
func (d *Dispatcher) forwardToUpstream(ctx context.Context, task *change.DispatchTask, chg *change.Change) {
	beforeSnap, _ := chg.Before()
	afterSnap, _ := chg.After()
	req := upstream.DispatchRequest{
		ChangeID:       chg.ID,
		EnterpriseID:   chg.EnterpriseID,
		ChangeType:     chg.ChangeType,
		DepartmentCode: task.DepartmentCode,
		BeforeValue:    beforeSnap.LegalRepresentative,
		AfterValue:     afterSnap.LegalRepresentative,
		Attempt:        task.AttemptCount + 1,
	}
	if d.upstream == nil {
		return
	}
	resp, err := d.upstream.Forward(ctx, req)
	if err != nil {
		d.log.Warn().Err(err).Str("task_id", task.ID).Str("dept", task.DepartmentCode).
			Msg("upstream forward failed, will retry")
		nextRetry := d.clk.Now().Add(d.exponentialBackoff(task.AttemptCount + 1))
		_ = d.repos.Dispatch.IncrementAttempt(ctx, task.ID, nextRetry)
		return
	}
	if resp.Success {
		_ = d.repos.Dispatch.UpdateStatus(ctx, task.ID, change.DispatchSucceeded, resp.Detail, "")
		d.evaluateChangeCompletion(ctx, chg.ID)
	} else {
		_ = d.repos.Dispatch.UpdateStatus(ctx, task.ID, change.DispatchFailed, "", resp.Error)
		d.evaluateChangeCompletion(ctx, chg.ID)
	}
}

// CompleteDispatch marks a task as succeeded and checks if the change is complete.
func (d *Dispatcher) CompleteDispatch(ctx context.Context, taskID, operator, result string) error {
	task, err := d.repos.Dispatch.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("get dispatch task: %w", err)
	}
	newStatus, err := change.DispatchTransition(task.Status, "succeed")
	if err != nil {
		return fmt.Errorf("complete dispatch transition: %w", err)
	}
	if err := d.repos.Dispatch.UpdateStatus(ctx, taskID, newStatus, result, ""); err != nil {
		return fmt.Errorf("update dispatch status: %w", err)
	}
	d.logAudit(ctx, operator, "dispatch.complete", taskID, task.DepartmentCode, task.ChangeID)
	d.evaluateChangeCompletion(ctx, task.ChangeID)
	return nil
}

// FailDispatch marks a task as failed and triggers compensation if needed.
func (d *Dispatcher) FailDispatch(ctx context.Context, taskID, operator, errMsg string) error {
	task, err := d.repos.Dispatch.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("get dispatch task: %w", err)
	}
	newStatus, err := change.DispatchTransition(task.Status, "fail")
	if err != nil {
		return fmt.Errorf("fail dispatch transition: %w", err)
	}
	if err := d.repos.Dispatch.UpdateStatus(ctx, taskID, newStatus, "", errMsg); err != nil {
		return fmt.Errorf("update dispatch status: %w", err)
	}
	d.logAudit(ctx, operator, "dispatch.fail", taskID, task.DepartmentCode, task.ChangeID)
	d.evaluateChangeCompletion(ctx, task.ChangeID)
	return nil
}

// evaluateChangeCompletion checks all dispatch tasks for a change and
// transitions the change status based on the aggregate result.
func (d *Dispatcher) evaluateChangeCompletion(ctx context.Context, changeID string) {
	tasks, err := d.repos.Dispatch.ListByChange(ctx, changeID)
	if err != nil {
		d.log.Error().Err(err).Str("change_id", changeID).Msg("list dispatch tasks")
		return
	}
	if len(tasks) == 0 {
		return
	}
	allDone := true
	succeeded := 0
	failed := 0
	for _, t := range tasks {
		if t.Status != change.DispatchSucceeded && t.Status != change.DispatchFailed &&
			t.Status != change.DispatchDeadLetter {
			allDone = false
			break
		}
		if t.Status == change.DispatchSucceeded {
			succeeded++
		} else if t.Status == change.DispatchFailed || t.Status == change.DispatchDeadLetter {
			failed++
		}
	}
	if !allDone {
		return
	}
	chg, err := d.repos.Changes.GetByID(ctx, changeID)
	if err != nil {
		return
	}
	if chg.Status != change.StatusDispatching && chg.Status != change.StatusPartialSuccess {
		return
	}
	var action change.Action
	if failed == 0 {
		action = change.ActionAckAll
	} else if succeeded > 0 {
		action = change.ActionFailPartial
	} else {
		action = change.ActionFailAll
	}
	newStatus, err := change.Transition(chg.Status, action)
	if err != nil {
		d.log.Error().Err(err).Str("change_id", changeID).Msg("transition failed")
		return
	}
	if err := d.repos.Changes.UpdateStatus(ctx, changeID, newStatus); err != nil {
		d.log.Error().Err(err).Str("change_id", changeID).Msg("update change status")
		return
	}
	if action == change.ActionAckAll {
		d.applyChangeToEnterprise(ctx, chg)
	}
	d.log.Info().Str("change_id", changeID).Str("status", string(newStatus)).
		Int("succeeded", succeeded).Int("failed", failed).Msg("change evaluated")
}

// applyChangeToEnterprise updates the enterprise record to reflect a completed
// change, applying the after-snapshot to the current enterprise state.
func (d *Dispatcher) applyChangeToEnterprise(ctx context.Context, chg *change.Change) {
	afterSnap, err := chg.After()
	if err != nil {
		d.log.Error().Err(err).Str("change_id", chg.ID).Msg("parse after snapshot")
		return
	}
	ent, err := d.repos.Enterprises.GetByID(ctx, chg.EnterpriseID)
	if err != nil {
		d.log.Error().Err(err).Str("change_id", chg.ID).Msg("get enterprise for apply")
		return
	}
	if err := d.repos.Enterprises.UpdateAfterChange(ctx, ent.ID, afterSnap, ent.Version); err != nil {
		d.log.Error().Err(err).Str("change_id", chg.ID).Msg("apply change to enterprise")
		return
	}
	d.logAudit(ctx, "system", "enterprise.apply_change", ent.ID, chg.ChangeType, chg.ID)
}

// ProcessExpired finds dispatch tasks past their retry deadline and either
// retries them (if attempts remain) or moves them to dead letter.
func (d *Dispatcher) ProcessExpired(ctx context.Context) (int, error) {
	now := d.clk.Now()
	expired, err := d.repos.Dispatch.ListExpired(ctx, now)
	if err != nil {
		return 0, fmt.Errorf("list expired dispatch: %w", err)
	}
	processed := 0
	for _, task := range expired {
		processed++
		newStatus, err := change.DispatchTransition(task.Status, "timeout")
		if err != nil {
			continue
		}
		_ = d.repos.Dispatch.UpdateStatus(ctx, task.ID, newStatus, "", "ack timeout")
		if task.AttemptCount+1 >= task.MaxAttempts {
			d.moveToDeadLetter(ctx, task, "max retries exceeded")
		} else {
			nextRetry := d.clk.Now().Add(d.exponentialBackoff(task.AttemptCount + 1))
			_ = d.repos.Dispatch.IncrementAttempt(ctx, task.ID, nextRetry)
			d.redeliverToUpstream(ctx, task)
		}
	}
	if processed > 0 {
		d.log.Info().Int("expired", processed).Msg("processed expired dispatches")
	}
	return processed, nil
}

func (d *Dispatcher) redeliverToUpstream(ctx context.Context, task *change.DispatchTask) {
	chg, err := d.repos.Changes.GetByID(ctx, task.ChangeID)
	if err != nil {
		return
	}
	d.forwardToUpstream(ctx, task, chg)
}

func (d *Dispatcher) moveToDeadLetter(ctx context.Context, task *change.DispatchTask, reason string) {
	_ = d.repos.Dispatch.MoveToDeadLetter(ctx, task.ID, reason, task.AttemptCount)
	dl := &change.DeadLetter{
		ID:             "dl-" + uuid.NewString()[:12],
		DispatchTaskID: task.ID,
		ChangeID:       task.ChangeID,
		DepartmentCode: task.DepartmentCode,
		LastError:      reason,
		AttemptCount:   task.AttemptCount,
		Status:         "pending",
		CreatedAt:      d.clk.Now(),
	}
	if err := d.repos.DeadLetters.Create(ctx, dl); err != nil {
		d.log.Error().Err(err).Str("task_id", task.ID).Msg("create dead letter")
	}
	d.logAudit(ctx, "system", "dispatch.dead_letter", task.ID, task.DepartmentCode, task.ChangeID)
}

// exponentialBackoff computes a retry delay with jitter based on the attempt
// number, capped at retryMax.
func (d *Dispatcher) exponentialBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	delay := float64(d.retryBase) * math.Pow(2, float64(attempt-1))
	if delay > float64(d.retryMax) {
		delay = float64(d.retryMax)
	}
	return time.Duration(delay)
}

func (d *Dispatcher) logAudit(ctx context.Context, actor, action, entityID, deptCode, changeID string) {
	rec := &change.AuditRecord{
		Actor:      actor,
		Action:     action,
		EntityType: "dispatch_task",
		EntityID:   entityID,
		Details:    fmt.Sprintf("department=%s change=%s", deptCode, changeID),
		CreatedAt:  d.clk.Now(),
	}
	_ = d.repos.Audit.Record(ctx, rec)
}
