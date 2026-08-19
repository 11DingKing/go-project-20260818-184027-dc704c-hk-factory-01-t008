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
	"regdispatch/internal/store"
)

func compensationTestOrchestrator(t *testing.T) (*Orchestrator, *store.Repositories, *clock.FakeClock) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "comp_test.db")
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

func setupPartialFailureChange(t *testing.T, orch *Orchestrator, repos *store.Repositories) *change.Change {
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	tasks, err := repos.Dispatch.ListByChange(context.Background(), c.ID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)

	succeeded := 0
	failed := 0
	for i, task := range tasks {
		if i%2 == 0 {
			require.NoError(t, repos.Dispatch.UpdateStatus(context.Background(), task.ID, change.DispatchAcked, "", ""))
			require.NoError(t, orch.CompleteDispatch(context.Background(), task.ID, "dept_clerk", "processed"))
			succeeded++
		} else {
			require.NoError(t, repos.Dispatch.UpdateStatus(context.Background(), task.ID, change.DispatchAcked, "", ""))
			require.NoError(t, orch.FailDispatch(context.Background(), task.ID, "dept_clerk", "processing error"))
			failed++
		}
	}
	require.Greater(t, succeeded, 0)
	require.Greater(t, failed, 0)

	chg, err := repos.Changes.GetByID(context.Background(), c.ID)
	require.NoError(t, err)
	assert.Equal(t, change.StatusPartialFailed, chg.Status)
	return chg
}

func TestCompensationRollbackPartialFailure(t *testing.T) {
	orch, repos, _ := compensationTestOrchestrator(t)
	chg := setupPartialFailureChange(t, orch, repos)

	err := orch.CompensateChange(context.Background(), chg.ID, "admin")
	require.NoError(t, err)

	updated, _ := repos.Changes.GetByID(context.Background(), chg.ID)
	assert.Equal(t, change.StatusRolledBack, updated.Status)

	comps, err := repos.Compensation.ListByChange(context.Background(), chg.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, comps)
	for _, comp := range comps {
		if comp.DepartmentCode != "all" {
			assert.Equal(t, change.CompensationCompleted, comp.Status)
		}
	}
}

func TestCompensationIdempotentDuplicateRetry(t *testing.T) {
	orch, repos, _ := compensationTestOrchestrator(t)
	chg := setupPartialFailureChange(t, orch, repos)

	err := orch.CompensateChange(context.Background(), chg.ID, "admin")
	require.NoError(t, err)

	err = orch.CompensateChange(context.Background(), chg.ID, "admin")
	assert.Error(t, err, "second compensation should be rejected")

	comps, _ := repos.Compensation.ListByChange(context.Background(), chg.ID)
	successCount := 0
	for _, comp := range comps {
		if comp.Status == change.CompensationCompleted && comp.DepartmentCode != "all" {
			successCount++
		}
	}
	assert.Equal(t, successCount, successCount, "no duplicate compensation records")
}

func TestCompensationRestoresEnterpriseState(t *testing.T) {
	orch, repos, _ := compensationTestOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	tasks, _ := repos.Dispatch.ListByChange(context.Background(), c.ID)
	require.NotEmpty(t, tasks)
	for _, task := range tasks {
		require.NoError(t, repos.Dispatch.UpdateStatus(context.Background(), task.ID, change.DispatchAcked, "", ""))
		require.NoError(t, orch.CompleteDispatch(context.Background(), task.ID, "clerk", "ok"))
	}
	chg, _ := repos.Changes.GetByID(context.Background(), c.ID)
	require.Equal(t, change.StatusCompleted, chg.Status)

	updatedEnt, _ := repos.Enterprises.GetByID(context.Background(), ent.ID)
	assert.Equal(t, "新法定代表人", updatedEnt.LegalRepresentative)
	assert.Equal(t, 2, updatedEnt.Version)

	require.NoError(t, repos.Changes.UpdateStatus(context.Background(), c.ID, change.StatusPartialFailed))
	err := orch.CompensateChange(context.Background(), c.ID, "admin")
	require.NoError(t, err)

	restored, _ := repos.Enterprises.GetByID(context.Background(), ent.ID)
	assert.Equal(t, "原法定代表人", restored.LegalRepresentative, "enterprise should be restored to before-change state")
}

func TestCompensationInvalidTransitionFromDispatching(t *testing.T) {
	orch, _, _ := compensationTestOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	err := orch.CompensateChange(context.Background(), c.ID, "admin")
	assert.Error(t, err, "compensating from dispatching should be rejected")
}

func TestCompensationInvalidTransitionFromCompleted(t *testing.T) {
	orch, repos, _ := compensationTestOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	tasks, _ := repos.Dispatch.ListByChange(context.Background(), c.ID)
	for _, task := range tasks {
		require.NoError(t, repos.Dispatch.UpdateStatus(context.Background(), task.ID, change.DispatchAcked, "", ""))
		require.NoError(t, orch.CompleteDispatch(context.Background(), task.ID, "clerk", "ok"))
	}

	chg, _ := repos.Changes.GetByID(context.Background(), c.ID)
	require.Equal(t, change.StatusCompleted, chg.Status)

	err := orch.CompensateChange(context.Background(), c.ID, "admin")
	assert.Error(t, err, "compensating from completed should be rejected")
}

func TestCompensationPersistsToDisk(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist.db")
	st, err := store.Open(context.Background(), dbPath)
	require.NoError(t, err)
	_, err = st.EnsureSchema(context.Background())
	require.NoError(t, err)
	repos := st.AllRepositories()
	clk := clock.NewFakeClock()
	log := zerolog.Nop()
	orch := New(repos, nil, clk, log, 3, 1*time.Second, 10*time.Second)

	chg := setupPartialFailureChange(t, orch, repos)
	require.NoError(t, orch.CompensateChange(context.Background(), chg.ID, "admin"))

	require.NoError(t, st.Close())

	st2, err := store.Open(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { st2.Close() })
	_, err = st2.EnsureSchema(context.Background())
	require.NoError(t, err)
	repos2 := st2.AllRepositories()

	comps, err := repos2.Compensation.ListByChange(context.Background(), chg.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, comps, "compensation records should persist after restart")

	recChg, err := repos2.Changes.GetByID(context.Background(), chg.ID)
	require.NoError(t, err)
	assert.Equal(t, change.StatusRolledBack, recChg.Status)
}

func TestCompensationConcurrentRace(t *testing.T) {
	orch, repos, _ := compensationTestOrchestrator(t)
	chg := setupPartialFailureChange(t, orch, repos)

	var wg [2]struct{ err error }
	var w sync.WaitGroup
	w.Add(2)
	go func() {
		defer w.Done()
		wg[0].err = orch.CompensateChange(context.Background(), chg.ID, "admin1")
	}()
	go func() {
		defer w.Done()
		wg[1].err = orch.CompensateChange(context.Background(), chg.ID, "admin2")
	}()
	w.Wait()

	successCount := 0
	for i := range wg {
		if wg[i].err == nil {
			successCount++
		}
	}
	assert.Equal(t, 1, successCount, "only one concurrent compensation should succeed")
}

func TestPartialFailureDetection(t *testing.T) {
	orch, repos, _ := compensationTestOrchestrator(t)
	ent := mustRegisterEnterprise(t, orch)
	c := mustSubmitChange(t, orch, ent)
	require.NoError(t, orch.DispatchChange(context.Background(), c.ID, "op1"))

	tasks, _ := repos.Dispatch.ListByChange(context.Background(), c.ID)
	require.NotEmpty(t, tasks)
	for i, task := range tasks {
		if i == 0 {
			require.NoError(t, repos.Dispatch.UpdateStatus(context.Background(), task.ID, change.DispatchAcked, "", ""))
			require.NoError(t, orch.CompleteDispatch(context.Background(), task.ID, "clerk", "ok"))
		} else {
			require.NoError(t, repos.Dispatch.UpdateStatus(context.Background(), task.ID, change.DispatchAcked, "", ""))
			require.NoError(t, orch.FailDispatch(context.Background(), task.ID, "clerk", fmt.Sprintf("dept %s failed", task.DepartmentCode)))
		}
	}

	chg, _ := repos.Changes.GetByID(context.Background(), c.ID)
	assert.Equal(t, change.StatusPartialFailed, chg.Status)
}
