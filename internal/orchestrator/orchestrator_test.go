package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"regdispatch/internal/clock"
	"regdispatch/internal/domain/change"
	"regdispatch/internal/domain/enterprise"
	"regdispatch/internal/store"
)

func testOrchestrator(t *testing.T) (*Orchestrator, *store.Repositories, clock.Clock) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := store.Open(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	_, err = st.EnsureSchema(context.Background())
	require.NoError(t, err)
	repos := st.AllRepositories()
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	orch := New(repos, nil, clk, log, 3, 1*time.Second, 10*time.Second)
	return orch, repos, clk
}

func mustRegisterEnterprise(t *testing.T, orch *Orchestrator) *enterprise.Enterprise {
	t.Helper()
	ent := &enterprise.Enterprise{
		Name:                "测试企业有限公司",
		LegalRepresentative: "原法定代表人",
		UnifiedCreditCode:   fmt.Sprintf("91110000MA0%07d", time.Now().UnixNano()%10000000),
		RegisteredCapital:   "500万元",
		BusinessScope:       "软件开发及技术服务",
		IndustryCode:        "I6510",
	}
	require.NoError(t, orch.RegisterEnterprise(context.Background(), ent))
	return ent
}

func mustSubmitChange(t *testing.T, orch *Orchestrator, ent *enterprise.Enterprise) *change.Change {
	t.Helper()
	c := &change.Change{
		EnterpriseID: ent.ID,
		ChangeType:   change.TypeLegalRepresentative,
		NewValue:     "新法定代表人",
		SubmittedBy:  "clerk1",
	}
	require.NoError(t, orch.SubmitChange(context.Background(), c))
	return c
}

func TestRegisterEnterprise(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	assert.NotEmpty(t, ent.ID)
	assert.Equal(t, enterprise.StatusActive, ent.Status)
	assert.Equal(t, 1, ent.Version)
}

func TestRegisterEnterpriseValidation(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	ent := &enterprise.Enterprise{Name: ""}
	err := orch.RegisterEnterprise(context.Background(), ent)
	assert.Error(t, err)
}

func TestSubmitChange(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	assert.Equal(t, change.StatusSubmitted, c.Status)
	assert.NotEmpty(t, c.BeforeSnapshot)
	assert.NotEmpty(t, c.AfterSnapshot)
}

func TestSubmitChangeInvalidEnterprise(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	c := &change.Change{
		EnterpriseID: "nonexistent",
		ChangeType:   change.TypeLegalRepresentative,
		NewValue:     "新法人",
		SubmittedBy:  "clerk1",
	}
	err := orch.SubmitChange(context.Background(), c)
	assert.Error(t, err)
}

func TestDispatchChangeCreatesTasks(t *testing.T) {
	orch, repos, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)

	err := orch.DispatchChange(context.Background(), c.ID, "operator1")
	require.NoError(t, err)

	tasks, err := repos.Dispatch.ListByChange(context.Background(), c.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, tasks)
	for _, task := range tasks {
		assert.Equal(t, change.DispatchPending, task.Status)
	}

	chg, _ := repos.Changes.GetByID(context.Background(), c.ID)
	assert.Equal(t, change.StatusDispatching, chg.Status)
}

func TestDispatchChangeIdempotentDuplicateRejected(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)

	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))
	err := orch.DispatchChange(context.Background(), c.ID, "op2")
	assert.Error(t, err, "duplicate dispatch should be rejected")
}

func TestDispatchInvalidTransitionReject(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)

	// Move to completed first by direct repo update
	require.NoError(t, orch.repos.Changes.UpdateStatus(context.Background(), c.ID, change.StatusCompleted))

	err := orch.DispatchChange(context.Background(), c.ID, "op1")
	assert.Error(t, err, "dispatch from completed should be rejected")
}

func TestAckDispatchInvalidTransition(t *testing.T) {
	orch, repos, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	tasks, _ := repos.Dispatch.ListByChange(context.Background(), c.ID)
	require.NotEmpty(t, tasks)
	task := tasks[0]

	// Ack from pending should fail (need to be delivered first)
	err := orch.AckDispatch(context.Background(), task.ID, "dept_clerk")
	assert.Error(t, err)

	// Move to delivered, then ack should succeed
	require.NoError(t, repos.Dispatch.UpdateStatus(context.Background(), task.ID, change.DispatchDelivered, "", ""))
	err = orch.AckDispatch(context.Background(), task.ID, "dept_clerk")
	require.NoError(t, err)

	// Double ack should fail
	err = orch.AckDispatch(context.Background(), task.ID, "dept_clerk2")
	assert.Error(t, err)
}

func TestCompleteDispatchTransitions(t *testing.T) {
	orch, repos, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	tasks, _ := repos.Dispatch.ListByChange(context.Background(), c.ID)
	require.NotEmpty(t, tasks)
	for _, task := range tasks {
		require.NoError(t, repos.Dispatch.UpdateStatus(context.Background(), task.ID, change.DispatchAcked, "", ""))
		err := orch.CompleteDispatch(context.Background(), task.ID, "dept_clerk", "processed")
		require.NoError(t, err)
	}

	chg, _ := repos.Changes.GetByID(context.Background(), c.ID)
	assert.Equal(t, change.StatusCompleted, chg.Status)
}

func TestRevokeChange(t *testing.T) {
	orch, repos, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)

	err := orch.RevokeChange(context.Background(), c.ID, "admin", "误操作")
	require.NoError(t, err)

	chg, _ := repos.Changes.GetByID(context.Background(), c.ID)
	assert.Equal(t, change.StatusRevoked, chg.Status)
	assert.Equal(t, "误操作", chg.RevokedReason)
}

func TestRevokeCompletedInvalidTransition(t *testing.T) {
	orch, repos, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, repos.Changes.UpdateStatus(context.Background(), c.ID, change.StatusCompleted))

	err := orch.RevokeChange(context.Background(), c.ID, "admin", "误操作")
	assert.Error(t, err)
}

func TestCompensateInvalidTransition(t *testing.T) {
	orch, repos, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, repos.Changes.UpdateStatus(context.Background(), c.ID, change.StatusDispatching))

	err := orch.CompensateChange(context.Background(), c.ID, "admin")
	assert.Error(t, err, "compensating from dispatching should be rejected")
}

func TestConcurrentDispatchRace(t *testing.T) {
	orch, repos, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)

	numChanges := 20
	changes := make([]*change.Change, numChanges)
	for i := 0; i < numChanges; i++ {
		c := &change.Change{
			EnterpriseID: ent.ID,
			ChangeType:   change.TypeLegalRepresentative,
			NewValue:     fmt.Sprintf("法人%d", i),
			SubmittedBy:  "clerk1",
		}
		require.NoError(t, orch.SubmitChange(context.Background(), c))
		changes[i] = c
	}

	var wg sync.WaitGroup
	errs := make([]error, numChanges)
	for i := 0; i < numChanges; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = orch.DispatchChange(context.Background(), changes[idx].ID, "op")
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		}
	}
	assert.Equal(t, numChanges, successCount, "all concurrent dispatches should succeed")

	tasks, _, err := repos.Dispatch.ListByDepartment(context.Background(), "tax", store.DispatchFilter{
		ListFilter: store.ListFilter{Limit: 1000},
	})
	require.NoError(t, err)
	assert.Equal(t, numChanges, len(tasks))
}

func TestConcurrentAckRace(t *testing.T) {
	orch, repos, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	tasks, _ := repos.Dispatch.ListByChange(context.Background(), c.ID)
	require.NotEmpty(t, tasks)
	task := tasks[0]
	require.NoError(t, repos.Dispatch.UpdateStatus(context.Background(), task.ID, change.DispatchDelivered, "", ""))

	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = orch.AckDispatch(context.Background(), task.ID, fmt.Sprintf("clerk%d", idx))
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		}
	}
	assert.Equal(t, 1, successCount, "only one concurrent ack should succeed")

	task2, _ := repos.Dispatch.GetByID(context.Background(), task.ID)
	assert.Equal(t, change.DispatchAcked, task2.Status)
}

func TestListEnterprisesPagination(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	for i := 0; i < 5; i++ {
		ent := &enterprise.Enterprise{
			Name:                fmt.Sprintf("企业%d", i),
			LegalRepresentative: "法人",
			UnifiedCreditCode:   fmt.Sprintf("91110000MA0%07d", i),
			RegisteredCapital:   "100万",
			BusinessScope:       "服务",
			IndustryCode:        "I6510",
		}
		require.NoError(t, orch.RegisterEnterprise(context.Background(), ent))
	}

	items, total, err := orch.ListEnterprises(context.Background(), 0, 3)
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	assert.Len(t, items, 3)

	items2, total2, err := orch.ListEnterprises(context.Background(), 3, 3)
	require.NoError(t, err)
	assert.Equal(t, 5, total2)
	assert.Len(t, items2, 2)
}

func TestListChangesWithFilters(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	for i := 0; i < 3; i++ {
		c := &change.Change{
			EnterpriseID: ent.ID,
			ChangeType:   change.TypeLegalRepresentative,
			NewValue:     fmt.Sprintf("法人%d", i),
			SubmittedBy:  "clerk1",
		}
		require.NoError(t, orch.SubmitChange(context.Background(), c))
	}

	items, total, err := orch.ListChanges(context.Background(), store.ChangeFilter{
		ListFilter:   store.ListFilter{Limit: 10},
		EnterpriseID: ent.ID,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, items, 3)
}

func TestAuditRecordPersisted(t *testing.T) {
	orch, repos, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)

	items, _, err := repos.Audit.List(context.Background(), store.AuditFilter{
		ListFilter: store.ListFilter{Limit: 10},
		EntityType: "enterprise",
		EntityID:   ent.ID,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, items)
	assert.Equal(t, "enterprise.register", items[0].Action)
}

func TestExportReconciliation(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	entries, err := orch.ExportReconciliation(context.Background(), ent.ID, "")
	require.NoError(t, err)
	assert.NotEmpty(t, entries)
	for _, e := range entries {
		assert.Equal(t, c.ID, e.ChangeID)
	}
}

func TestViewBacklog(t *testing.T) {
	orch, _, _ := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	backlog, err := orch.ViewBacklog(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, backlog)
	totalPending := 0
	for _, count := range backlog {
		totalPending += count
	}
	assert.Greater(t, totalPending, 0)
}

func TestResolveOrderAssignsChronologicalOrder(t *testing.T) {
	orch, repos, clk := testOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)

	changes := make([]*change.Change, 5)
	for i := 0; i < 5; i++ {
		c := &change.Change{
			EnterpriseID: ent.ID,
			ChangeType:   change.TypeLegalRepresentative,
			NewValue:     fmt.Sprintf("法人%d", i),
			SubmittedBy:  "clerk1",
		}
		require.NoError(t, orch.SubmitChange(context.Background(), c))
		changes[i] = c
		clk.(*clock.FakeClock).Advance(1 * time.Minute)
	}

	err := orch.ResolveOrder(context.Background(), ent.ID)
	require.NoError(t, err)

	all, err := repos.Changes.ListByEnterprise(context.Background(), ent.ID)
	require.NoError(t, err)
	for i, c := range all {
		assert.Equal(t, i+1, c.ResolutionOrder, "change %d should have order %d", i, i+1)
	}
}

func TestRestartRecoverUnfinishedDispatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "restart.db")
	st, err := store.Open(context.Background(), dbPath)
	require.NoError(t, err)
	_, err = st.EnsureSchema(context.Background())
	require.NoError(t, err)
	repos := st.AllRepositories()
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	orch := New(repos, nil, clk, log, 3, 1*time.Second, 10*time.Second)

	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	require.NoError(t, st.Close())

	st2, err := store.Open(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { st2.Close() })
	_, err = st2.EnsureSchema(context.Background())
	require.NoError(t, err)
	repos2 := st2.AllRepositories()
	orch2 := New(repos2, nil, clk, log, 3, 1*time.Second, 10*time.Second)
	_ = orch2

	tasks, err := repos2.Dispatch.ListByChange(context.Background(), c.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, tasks)
	for _, task := range tasks {
		assert.Equal(t, change.DispatchPending, task.Status, "dispatch should still be pending after restart")
	}

	chg, err := repos2.Changes.GetByID(context.Background(), c.ID)
	require.NoError(t, err)
	assert.Equal(t, change.StatusDispatching, chg.Status)
}
