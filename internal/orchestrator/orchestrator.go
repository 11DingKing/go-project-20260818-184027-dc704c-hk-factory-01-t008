package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"regdispatch/internal/clock"
	"regdispatch/internal/domain/change"
	"regdispatch/internal/domain/department"
	"regdispatch/internal/domain/enterprise"
	"regdispatch/internal/domain/event"
	"regdispatch/internal/errorsx"
	"regdispatch/internal/store"
	"regdispatch/internal/upstream"
)

// Orchestrator coordinates the full registration-change lifecycle: enterprise
// registration, change submission, multi-department dispatch, out-of-order
// resolution, partial-failure compensation, and revocation.
type Orchestrator struct {
	repos       *store.Repositories
	upstream    *upstream.Client
	clk         clock.Clock
	log         zerolog.Logger
	dispatcher  *Dispatcher
	resolver    *Resolver
	compensator *Compensator
	maxRetries  int
	retryBase   time.Duration
	retryMax    time.Duration
}

// New creates an Orchestrator wired to the given repositories, upstream client,
// and configuration.
func New(
	repos *store.Repositories,
	up *upstream.Client,
	clk clock.Clock,
	log zerolog.Logger,
	maxRetries int,
	retryBase, retryMax time.Duration,
) *Orchestrator {
	o := &Orchestrator{
		repos:      repos,
		upstream:   up,
		clk:        clk,
		log:        log,
		maxRetries: maxRetries,
		retryBase:  retryBase,
		retryMax:   retryMax,
	}
	o.dispatcher = &Dispatcher{
		repos:      repos,
		upstream:   up,
		clk:        clk,
		log:        log,
		maxRetries: maxRetries,
		retryBase:  retryBase,
		retryMax:   retryMax,
	}
	o.resolver = &Resolver{repos: repos, clk: clk, log: log}
	o.compensator = &Compensator{repos: repos, upstream: up, clk: clk, log: log}
	return o
}

// Audit writes an audit record for the given actor, action, and entity.
func (o *Orchestrator) audit(ctx context.Context, actor, action, entityType, entityID, details string) {
	rec := &change.AuditRecord{
		Actor:      actor,
		Action:     action,
		EntityType: entityType,
		EntityID:   entityID,
		Details:    details,
		CreatedAt:  o.clk.Now(),
	}
	if err := o.repos.Audit.Record(ctx, rec); err != nil {
		o.log.Error().Err(err).Str("action", action).Msg("audit record failed")
	}
}

// RegisterEnterprise creates a new enterprise in the registry.
func (o *Orchestrator) RegisterEnterprise(ctx context.Context, e *enterprise.Enterprise) error {
	if err := e.Validate(); err != nil {
		return errorsx.Wrap("validation", err.Error(), err)
	}
	e.ID = "ent-" + uuid.NewString()[:12]
	e.Status = enterprise.StatusActive
	e.Version = 1
	e.CreatedAt = o.clk.Now()
	e.UpdatedAt = e.CreatedAt
	if err := o.repos.Enterprises.Create(ctx, e); err != nil {
		return fmt.Errorf("create enterprise: %w", err)
	}
	o.audit(ctx, "system", "enterprise.register", "enterprise", e.ID, e.Name)
	return nil
}

// ListEnterprises returns a paginated list of enterprises.
func (o *Orchestrator) ListEnterprises(ctx context.Context, offset, limit int) ([]*enterprise.Enterprise, int, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	return o.repos.Enterprises.List(ctx, store.ListFilter{Offset: offset, Limit: limit})
}

// GetEnterprise returns a single enterprise by ID.
func (o *Orchestrator) GetEnterprise(ctx context.Context, id string) (*enterprise.Enterprise, error) {
	return o.repos.Enterprises.GetByID(ctx, id)
}

// SubmitChange records a new registration change submitted by an intake clerk.
// The change is stored in draft status pending dispatch.
func (o *Orchestrator) SubmitChange(ctx context.Context, c *change.Change) error {
	c.ID = "chg-" + uuid.NewString()[:12]
	c.Status = change.StatusSubmitted
	c.EventTime = o.clk.Now()
	c.CreatedAt = c.EventTime
	c.UpdatedAt = c.EventTime
	if err := c.Validate(); err != nil {
		return errorsx.Wrap("validation", err.Error(), err)
	}
	ent, err := o.repos.Enterprises.GetByID(ctx, c.EnterpriseID)
	if err != nil {
		return fmt.Errorf("lookup enterprise for change: %w", err)
	}
	beforeSnap := ent.ToSnapshot()
	beforeJSON, err := enterprise.SnapshotJSON(beforeSnap)
	if err != nil {
		return fmt.Errorf("encode before snapshot: %w", err)
	}
	c.BeforeSnapshot = beforeJSON
	afterSnap := beforeSnap
	if err := applyChangeToSnapshot(&afterSnap, c.ChangeType, c.NewValue); err != nil {
		return fmt.Errorf("apply change to snapshot: %w", err)
	}
	afterJSON, err := enterprise.SnapshotJSON(afterSnap)
	if err != nil {
		return fmt.Errorf("encode after snapshot: %w", err)
	}
	c.AfterSnapshot = afterJSON

	if err := o.repos.Changes.Create(ctx, c); err != nil {
		return fmt.Errorf("create change: %w", err)
	}
	o.audit(ctx, c.SubmittedBy, "change.submit", "change", c.ID,
		fmt.Sprintf("type=%s enterprise=%s", c.ChangeType, c.EnterpriseID))
	return nil
}

// ListChanges returns changes matching the given filters with pagination.
func (o *Orchestrator) ListChanges(ctx context.Context, f store.ChangeFilter) ([]*change.Change, int, error) {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}
	return o.repos.Changes.List(ctx, f)
}

// GetChange returns a single change by ID.
func (o *Orchestrator) GetChange(ctx context.Context, id string) (*change.Change, error) {
	return o.repos.Changes.GetByID(ctx, id)
}

// DispatchChange triggers batch dispatch to all relevant departments and
// forwards each via the upstream client.
func (o *Orchestrator) DispatchChange(ctx context.Context, changeID, operator string) error {
	return o.dispatcher.Dispatch(ctx, changeID, operator)
}

// AckDispatch records a department's acknowledgment of receipt.
func (o *Orchestrator) AckDispatch(ctx context.Context, taskID, ackedBy string) error {
	task, err := o.repos.Dispatch.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("get dispatch task: %w", err)
	}
	if !change.CanDispatchTransition(task.Status, "ack") {
		return errorsx.InvalidTransition(string(task.Status), "ack", "dispatch_task")
	}
	now := o.clk.Now()
	if err := o.repos.Dispatch.SetAcked(ctx, taskID, ackedBy, now); err != nil {
		return fmt.Errorf("ack dispatch: %w", err)
	}
	o.audit(ctx, ackedBy, "dispatch.ack", "dispatch_task", taskID,
		fmt.Sprintf("department=%s change=%s", task.DepartmentCode, task.ChangeID))
	return nil
}

// CompleteDispatch marks a department's processing as succeeded.
func (o *Orchestrator) CompleteDispatch(ctx context.Context, taskID, operator, result string) error {
	return o.dispatcher.CompleteDispatch(ctx, taskID, operator, result)
}

// FailDispatch marks a department's processing as failed, triggering
// compensation if appropriate.
func (o *Orchestrator) FailDispatch(ctx context.Context, taskID, operator, errMsg string) error {
	return o.dispatcher.FailDispatch(ctx, taskID, operator, errMsg)
}

// ProcessExpired finds dispatch tasks past their retry deadline and either
// retries them or moves them to dead letter.
func (o *Orchestrator) ProcessExpired(ctx context.Context) (int, error) {
	return o.dispatcher.ProcessExpired(ctx)
}

// ResolveOrder reorders changes for an enterprise by event time so that late
// arrivals are backfilled to their correct chronological position.
func (o *Orchestrator) ResolveOrder(ctx context.Context, enterpriseID string) error {
	return o.resolver.Resolve(ctx, enterpriseID)
}

// CompensateChange triggers compensation for a partially failed change,
// rolling back successfully-applied department changes.
func (o *Orchestrator) CompensateChange(ctx context.Context, changeID, operator string) error {
	return o.compensator.Compensate(ctx, changeID, operator)
}

// RevokeChange revokes an erroneous change and notifies affected departments.
func (o *Orchestrator) RevokeChange(ctx context.Context, changeID, operator, reason string) error {
	chg, err := o.repos.Changes.GetByID(ctx, changeID)
	if err != nil {
		return fmt.Errorf("get change for revocation: %w", err)
	}
	if change.IsTerminal(chg.Status) {
		return errorsx.InvalidTransition(string(chg.Status), "revoke", "change")
	}
	newStatus, err := change.Transition(chg.Status, change.ActionRevoke)
	if err != nil {
		return fmt.Errorf("revoke transition: %w", err)
	}
	if err := o.repos.Changes.SetRevoked(ctx, changeID, reason); err != nil {
		return fmt.Errorf("set revoked: %w", err)
	}
	tasks, _ := o.repos.Dispatch.ListByChange(ctx, changeID)
	for _, t := range tasks {
		if t.Status == change.DispatchSucceeded {
			_ = o.repos.Dispatch.UpdateStatus(ctx, t.ID, change.DispatchFailed,
				"revoked", "change revoked")
		}
	}
	compRec := &change.CompensationRecord{
		ID:             "comp-" + uuid.NewString()[:12],
		ChangeID:       changeID,
		DepartmentCode: "all",
		Action:         "revoke_notify",
		Status:         change.CompensationCompleted,
		CreatedAt:      o.clk.Now(),
	}
	_ = o.repos.Compensation.Create(ctx, compRec)
	o.audit(ctx, operator, "change.revoke", "change", changeID, reason)
	o.log.Info().Str("change_id", changeID).Str("new_status", string(newStatus)).Msg("change revoked")
	return nil
}

// ListDispatchesByDepartment returns pending and in-progress dispatch tasks
// for a department with pagination.
func (o *Orchestrator) ListDispatchesByDepartment(ctx context.Context, deptCode string, f store.DispatchFilter) ([]*change.DispatchTask, int, error) {
	if !department.IsValidCode(deptCode) {
		return nil, 0, errorsx.Wrap("validation", "invalid department code", errorsx.ErrInvalidArgument)
	}
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}
	return o.repos.Dispatch.ListByDepartment(ctx, deptCode, f)
}

// ListAuditRecords returns paginated audit records matching the filter.
func (o *Orchestrator) ListAuditRecords(ctx context.Context, f store.AuditFilter) ([]*change.AuditRecord, int, error) {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}
	return o.repos.Audit.List(ctx, f)
}

// ListDeadLetters returns paginated dead letter records.
func (o *Orchestrator) ListDeadLetters(ctx context.Context, f store.DeadLetterFilter) ([]*change.DeadLetter, int, error) {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}
	return o.repos.DeadLetters.List(ctx, f)
}

// RedeliverDeadLetter manually re-dispatches a dead-lettered task.
func (o *Orchestrator) RedeliverDeadLetter(ctx context.Context, dlID, operator string) error {
	dl, err := o.repos.DeadLetters.GetByID(ctx, dlID)
	if err != nil {
		return fmt.Errorf("get dead letter: %w", err)
	}
	if err := o.repos.DeadLetters.UpdateStatus(ctx, dlID, "redelivered"); err != nil {
		return fmt.Errorf("update dead letter: %w", err)
	}
	task := &change.DispatchTask{
		ID:             "disp-" + uuid.NewString()[:12],
		ChangeID:       dl.ChangeID,
		DepartmentCode: dl.DepartmentCode,
		Topic:          "topic." + dl.DepartmentCode,
		Status:         change.DispatchPending,
		AttemptCount:   0,
		MaxAttempts:    o.maxRetries,
		CreatedAt:      o.clk.Now(),
		UpdatedAt:      o.clk.Now(),
	}
	if err := o.repos.Dispatch.Create(ctx, task); err != nil {
		return fmt.Errorf("recreate dispatch task: %w", err)
	}
	o.audit(ctx, operator, "dead_letter.redeliver", "dead_letter", dlID,
		fmt.Sprintf("new_task=%s", task.ID))
	return nil
}

func applyChangeToSnapshot(snap *enterprise.Snapshot, changeType, newValue string) error {
	switch changeType {
	case change.TypeLegalRepresentative:
		snap.LegalRepresentative = newValue
	case change.TypeName:
		snap.Name = newValue
	case change.TypeRegisteredCapital:
		snap.RegisteredCapital = newValue
	case change.TypeBusinessScope:
		snap.BusinessScope = newValue
	default:
		return fmt.Errorf("unsupported change type: %s", changeType)
	}
	return nil
}

// GetSubscriberOffset returns the last committed offset for a subscriber.
func (o *Orchestrator) GetSubscriberOffset(ctx context.Context, subscriberID, topic string) (int64, error) {
	return o.repos.Subscribers.GetOffset(ctx, subscriberID, topic)
}

// ReadEventLog returns events from a topic starting after the given offset.
func (o *Orchestrator) ReadEventLog(ctx context.Context, topic string, offset int64, limit int) ([]*event.Entry, error) {
	return o.repos.EventLog.ReadFrom(ctx, topic, offset, limit)
}

// CommitSubscriberOffset persists the consumed offset for a subscriber.
func (o *Orchestrator) CommitSubscriberOffset(ctx context.Context, subscriberID, topic string, offset int64) error {
	return o.repos.Subscribers.CommitOffset(ctx, subscriberID, topic, offset)
}

// MaxEventOffset returns the highest offset in the event log.
func (o *Orchestrator) MaxEventOffset(ctx context.Context) (int64, error) {
	return o.repos.EventLog.MaxOffset(ctx)
}
