package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"regdispatch/internal/clock"
	"regdispatch/internal/domain/change"
	"regdispatch/internal/domain/enterprise"
	"regdispatch/internal/domain/event"
	"regdispatch/internal/errorsx"
	"regdispatch/internal/store"
	"regdispatch/internal/upstream"
)

// Compensator handles rollback of partially-failed changes. Compensation is
// idempotent: it checks current state before applying and can be retried
// safely without double-rolling-back.
type Compensator struct {
	repos    *store.Repositories
	upstream *upstream.Client
	clk      clock.Clock
	log      zerolog.Logger
}

// Compensate rolls back successfully-applied department changes for a
// partially-failed change. It records compensation actions, sends rollback
// notifications to affected departments, and restores the enterprise snapshot.
func (c *Compensator) Compensate(ctx context.Context, changeID, operator string) error {
	chg, err := c.repos.Changes.GetByID(ctx, changeID)
	if err != nil {
		return fmt.Errorf("get change for compensation: %w", err)
	}
	if chg.Status != change.StatusPartialFailed && chg.Status != change.StatusPartialSuccess {
		return errorsx.InvalidTransition(string(chg.Status), "start_compensation", "change")
	}
	newStatus, err := change.Transition(chg.Status, change.ActionStartCompensation)
	if err != nil {
		return fmt.Errorf("compensation transition: %w", err)
	}
	if err := c.repos.Changes.CompareAndSetStatus(ctx, changeID, chg.Status, newStatus); err != nil {
		return fmt.Errorf("set compensating status: %w", err)
	}

	tasks, err := c.repos.Dispatch.ListByChange(ctx, changeID)
	if err != nil {
		return fmt.Errorf("list dispatch tasks: %w", err)
	}

	existingComps, _ := c.repos.Compensation.ListByChange(ctx, changeID)
	compedDepts := make(map[string]bool)
	for _, ec := range existingComps {
		if ec.Status == change.CompensationCompleted {
			compedDepts[ec.DepartmentCode] = true
		}
	}

	compensated := 0
	for _, task := range tasks {
		if task.Status != change.DispatchSucceeded {
			continue
		}
		if compedDepts[task.DepartmentCode] {
			continue // idempotent: already compensated
		}
		if err := c.compensateOne(ctx, task, chg, operator); err != nil {
			c.log.Error().Err(err).Str("dept", task.DepartmentCode).Msg("compensation failed")
			continue
		}
		compensated++
	}

	beforeSnap, err := chg.Before()
	if err != nil {
		return fmt.Errorf("parse before snapshot: %w", err)
	}
	if err := c.restoreEnterprise(ctx, chg, beforeSnap); err != nil {
		c.log.Error().Err(err).Str("change_id", changeID).Msg("restore enterprise failed")
	}

	finalStatus := change.StatusRolledBack
	if compensated == 0 && len(compedDepts) > 0 {
		finalStatus = change.StatusRolledBack
	}
	_, err = change.Transition(newStatus, change.ActionCompensationDone)
	if err != nil {
		return fmt.Errorf("compensation done transition: %w", err)
	}
	if err := c.repos.Changes.UpdateStatus(ctx, changeID, finalStatus); err != nil {
		return fmt.Errorf("set rolled back status: %w", err)
	}

	c.appendCompensationEvent(ctx, chg)
	c.logAudit(ctx, operator, "change.compensate", changeID,
		fmt.Sprintf("compensated=%d", compensated))
	c.log.Info().Str("change_id", changeID).Int("compensated", compensated).
		Msg("compensation completed")
	return nil
}

// compensateOne rolls back a single department's applied change. It creates
// a compensation record and sends a rollback notification via the upstream.
func (c *Compensator) compensateOne(ctx context.Context, task *change.DispatchTask, chg *change.Change, operator string) error {
	compRec := &change.CompensationRecord{
		ID:             "comp-" + uuid.NewString()[:12],
		ChangeID:       chg.ID,
		DepartmentCode: task.DepartmentCode,
		Action:         "rollback_change",
		Status:         change.CompensationPending,
		CreatedAt:      c.clk.Now(),
	}
	if err := c.repos.Compensation.Create(ctx, compRec); err != nil {
		return fmt.Errorf("create compensation record: %w", err)
	}

	rollbackErr := c.sendRollbackToUpstream(ctx, task, chg)
	if rollbackErr != nil {
		_ = c.repos.Compensation.UpdateStatus(ctx, compRec.ID, change.CompensationFailed, rollbackErr.Error())
		return rollbackErr
	}

	if err := c.repos.Compensation.UpdateStatus(ctx, compRec.ID, change.CompensationCompleted, ""); err != nil {
		return fmt.Errorf("update compensation status: %w", err)
	}
	if err := c.repos.Dispatch.UpdateStatus(ctx, task.ID, change.DispatchFailed, "compensated", "rolled back by compensation"); err != nil {
		c.log.Warn().Err(err).Str("task_id", task.ID).Msg("update dispatched task after compensation")
	}
	return nil
}

// sendRollbackToUpstream notifies the department backend to undo the change.
// This is best-effort; if the upstream is unavailable, the compensation record
// remains in 'failed' state for manual retry.
func (c *Compensator) sendRollbackToUpstream(ctx context.Context, task *change.DispatchTask, chg *change.Change) error {
	if c.upstream == nil {
		return nil
	}
	beforeSnap, _ := chg.Before()
	req := upstream.DispatchRequest{
		ChangeID:       chg.ID,
		EnterpriseID:   chg.EnterpriseID,
		ChangeType:     "rollback_" + chg.ChangeType,
		DepartmentCode: task.DepartmentCode,
		BeforeValue:    chg.AfterSnapshot,
		AfterValue:     chg.BeforeSnapshot,
		Attempt:        1,
	}
	_, beforeLegal := beforeSnap.LegalRepresentative, ""
	_ = beforeLegal
	resp, err := c.upstream.Forward(ctx, req)
	if err != nil {
		return fmt.Errorf("rollback upstream forward: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("rollback rejected by upstream %s: %s", task.DepartmentCode, resp.Error)
	}
	return nil
}

// restoreEnterprise restores the enterprise to its pre-change snapshot.
// This is idempotent: if the enterprise is already in the before state, it
// succeeds without error.
func (c *Compensator) restoreEnterprise(ctx context.Context, chg *change.Change, beforeSnap enterprise.Snapshot) error {
	ent, err := c.repos.Enterprises.GetByID(ctx, chg.EnterpriseID)
	if err != nil {
		return fmt.Errorf("get enterprise for restore: %w", err)
	}
	if err := c.repos.Enterprises.UpdateAfterChange(ctx, ent.ID, beforeSnap, ent.Version); err != nil {
		if errorsx.IsConcurrentUpdate(err) {
			c.log.Warn().Str("enterprise_id", ent.ID).Msg("concurrent update during restore, skipping")
			return nil
		}
		return fmt.Errorf("restore enterprise: %w", err)
	}
	return nil
}

func (c *Compensator) appendCompensationEvent(ctx context.Context, chg *change.Change) {
	payload := event.Payload{
		ChangeID:     chg.ID,
		EnterpriseID: chg.EnterpriseID,
		ChangeType:   chg.ChangeType,
		Reason:       "compensation_completed",
	}
	payloadJSON, _ := event.EncodePayload(payload)
	entry := &event.Entry{
		Topic:     "topic.compensation",
		ChangeID:  chg.ID,
		EventType: event.TypeCompensationCompleted,
		Payload:   payloadJSON,
		EventTime: chg.EventTime,
		CreatedAt: c.clk.Now(),
	}
	if _, err := c.repos.EventLog.Append(ctx, entry); err != nil {
		c.log.Error().Err(err).Msg("append compensation event")
	}
}

func (c *Compensator) logAudit(ctx context.Context, actor, action, entityID, details string) {
	rec := &change.AuditRecord{
		Actor:      actor,
		Action:     action,
		EntityType: "change",
		EntityID:   entityID,
		Details:    details,
		CreatedAt:  c.clk.Now(),
	}
	_ = c.repos.Audit.Record(ctx, rec)
}
