package orchestrator

import (
	"context"
	"fmt"
	"sort"

	"github.com/rs/zerolog"

	"regdispatch/internal/clock"
	"regdispatch/internal/domain/change"
	"regdispatch/internal/domain/enterprise"
	"regdispatch/internal/store"
)

// Resolver handles out-of-order and late-arriving changes by sorting them
// by event time (not arrival time) and assigning a stable resolution order.
// Late arrivals are backfilled to their correct chronological position.
type Resolver struct {
	repos *store.Repositories
	clk   clock.Clock
	log   zerolog.Logger
}

// lateArrivalThreshold is how far behind the latest event time a change can
// be before it is flagged as "expired late data". Changes older than this
// are still applied but logged as late.
const lateArrivalThreshold = 0 // all late arrivals are flagged

// Resolve sorts all changes for an enterprise by event time and assigns
// resolution_order values. It identifies late arrivals and ensures the
// final enterprise state is deterministic regardless of arrival order.
func (r *Resolver) Resolve(ctx context.Context, enterpriseID string) error {
	changes, err := r.repos.Changes.ListByEnterprise(ctx, enterpriseID)
	if err != nil {
		return fmt.Errorf("list changes for resolution: %w", err)
	}
	if len(changes) == 0 {
		return nil
	}

	sorted := make([]*change.Change, len(changes))
	copy(sorted, changes)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].EventTime.Equal(sorted[j].EventTime) {
			return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
		}
		return sorted[i].EventTime.Before(sorted[j].EventTime)
	})

	lateArrivals := r.identifyLateArrivals(sorted)
	for i, c := range sorted {
		if c.ResolutionOrder != i+1 {
			if err := r.repos.Changes.SetResolutionOrder(ctx, c.ID, i+1); err != nil {
				return fmt.Errorf("set resolution order: %w", err)
			}
		}
	}

	r.replayEnterpriseState(ctx, enterpriseID, sorted)

	if len(lateArrivals) > 0 {
		r.log.Info().Str("enterprise_id", enterpriseID).
			Int("late_arrivals", len(lateArrivals)).
			Msg("resolved late-arriving changes")
	}
	return nil
}

// identifyLateArrivals finds changes whose event_time is earlier than the
// maximum event_time seen so far in arrival order (sorted by created_at).
// These are changes that arrived after a later-dated change was already
// recorded, meaning they are "late" relative to the arrival sequence.
func (r *Resolver) identifyLateArrivals(sorted []*change.Change) []*change.Change {
	if len(sorted) < 2 {
		return nil
	}
	byArrival := make([]*change.Change, len(sorted))
	copy(byArrival, sorted)
	sort.SliceStable(byArrival, func(i, j int) bool {
		return byArrival[i].CreatedAt.Before(byArrival[j].CreatedAt)
	})

	var late []*change.Change
	maxEventTime := byArrival[0].EventTime
	for i := 1; i < len(byArrival); i++ {
		if byArrival[i].EventTime.Before(maxEventTime) {
			late = append(late, byArrival[i])
		}
		if byArrival[i].EventTime.After(maxEventTime) {
			maxEventTime = byArrival[i].EventTime
		}
	}
	return late
}

// replayEnterpriseState recomputes the enterprise snapshot by replaying all
// completed changes in event-time order. This ensures the final state is
// deterministic regardless of arrival order.
func (r *Resolver) replayEnterpriseState(ctx context.Context, enterpriseID string, sorted []*change.Change) {
	ent, err := r.repos.Enterprises.GetByID(ctx, enterpriseID)
	if err != nil {
		r.log.Error().Err(err).Str("enterprise_id", enterpriseID).Msg("get enterprise for replay")
		return
	}
	snap := ent.ToSnapshot()
	for _, c := range sorted {
		if c.Status != change.StatusCompleted && c.Status != change.StatusDispatching {
			continue
		}
		afterSnap, err := c.After()
		if err != nil {
			r.log.Error().Err(err).Str("change_id", c.ID).Msg("parse after snapshot")
			continue
		}
		snap = mergeSnapshots(snap, afterSnap)
	}
	if err := r.repos.Enterprises.UpdateAfterChange(ctx, enterpriseID, snap, ent.Version); err != nil {
		r.log.Error().Err(err).Str("enterprise_id", enterpriseID).Msg("replay update enterprise")
	}
}

// mergeSnapshots applies the fields from the 'after' snapshot onto the
// current snapshot, field by field. Only non-empty fields are applied,
// ensuring partial changes don't wipe unrelated fields.
func mergeSnapshots(current, after enterprise.Snapshot) enterprise.Snapshot {
	result := current
	if after.Name != "" {
		result.Name = after.Name
	}
	if after.LegalRepresentative != "" {
		result.LegalRepresentative = after.LegalRepresentative
	}
	if after.RegisteredCapital != "" {
		result.RegisteredCapital = after.RegisteredCapital
	}
	if after.BusinessScope != "" {
		result.BusinessScope = after.BusinessScope
	}
	return result
}

// ReconciliationEntry is one row in a reconciliation export.
type ReconciliationEntry struct {
	ChangeID        string `json:"change_id"`
	EnterpriseID    string `json:"enterprise_id"`
	ChangeType      string `json:"change_type"`
	DepartmentCode  string `json:"department_code"`
	DispatchStatus  string `json:"dispatch_status"`
	EventTime       string `json:"event_time"`
	ResolutionOrder int    `json:"resolution_order"`
	AttemptCount    int    `json:"attempt_count"`
	AckedBy         string `json:"acked_by,omitempty"`
}

// ExportReconciliation produces a reconciliation statement for a department
// or enterprise, listing all dispatch tasks with their current status.
func (o *Orchestrator) ExportReconciliation(ctx context.Context, enterpriseID, departmentCode string) ([]ReconciliationEntry, error) {
	if enterpriseID == "" && departmentCode == "" {
		return nil, fmt.Errorf("enterprise_id or department_code is required")
	}
	var tasks []*change.DispatchTask
	if departmentCode != "" {
		t, _, err := o.repos.Dispatch.ListByDepartment(ctx, departmentCode, store.DispatchFilter{ListFilter: store.ListFilter{Limit: 1000}})
		if err != nil {
			return nil, fmt.Errorf("list dispatch by department: %w", err)
		}
		tasks = t
	} else {
		changes, err := o.repos.Changes.ListByEnterprise(ctx, enterpriseID)
		if err != nil {
			return nil, fmt.Errorf("list changes: %w", err)
		}
		for _, c := range changes {
			ct, err := o.repos.Dispatch.ListByChange(ctx, c.ID)
			if err != nil {
				continue
			}
			tasks = append(tasks, ct...)
		}
	}

	entries := make([]ReconciliationEntry, 0, len(tasks))
	for _, t := range tasks {
		chg, err := o.repos.Changes.GetByID(ctx, t.ChangeID)
		if err != nil {
			continue
		}
		entry := ReconciliationEntry{
			ChangeID:        t.ChangeID,
			EnterpriseID:    chg.EnterpriseID,
			ChangeType:      chg.ChangeType,
			DepartmentCode:  t.DepartmentCode,
			DispatchStatus:  string(t.Status),
			EventTime:       chg.EventTime.Format("2006-01-02T15:04:05Z"),
			ResolutionOrder: chg.ResolutionOrder,
			AttemptCount:    t.AttemptCount,
			AckedBy:         t.AckedBy,
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// ViewBacklog returns the count of pending dispatch tasks per department.
func (o *Orchestrator) ViewBacklog(ctx context.Context) (map[string]int, error) {
	result := make(map[string]int)
	for _, dept := range []string{"tax", "social_security", "provident_fund", "industry_supervisor", "market_regulator"} {
		tasks, _, err := o.repos.Dispatch.ListByDepartment(ctx, dept, store.DispatchFilter{
			ListFilter: store.ListFilter{Limit: 10000},
			Status:     string(change.DispatchPending),
		})
		if err != nil {
			return nil, fmt.Errorf("list backlog for %s: %w", dept, err)
		}
		result[dept] = len(tasks)
	}
	return result, nil
}
