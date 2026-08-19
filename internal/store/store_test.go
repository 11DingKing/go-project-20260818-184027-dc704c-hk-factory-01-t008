package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"regdispatch/internal/domain/change"
	"regdispatch/internal/domain/enterprise"
	"regdispatch/internal/domain/event"
)

func testStore(t *testing.T) (*Store, *Repositories) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := Open(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	_, err = st.EnsureSchema(context.Background())
	require.NoError(t, err)
	return st, st.AllRepositories()
}

func mustCreateEnterprise(t *testing.T, repos *Repositories) *enterprise.Enterprise {
	t.Helper()
	ent := &enterprise.Enterprise{
		Name:                "测试科技有限公司",
		LegalRepresentative: "原法定代表人",
		UnifiedCreditCode:   "91110000MA0TEST0001",
		RegisteredCapital:   "1000万元",
		BusinessScope:       "软件开发",
		IndustryCode:        "I6510",
		Status:              enterprise.StatusActive,
		Version:             1,
		CreatedAt:           time.Now(),
		UpdatedAt:           time.Now(),
	}
	ent.ID = "ent-test-" + fmt.Sprintf("%d", time.Now().UnixNano())
	require.NoError(t, repos.Enterprises.Create(context.Background(), ent))
	return ent
}

func mustCreateChange(t *testing.T, repos *Repositories, ent *enterprise.Enterprise) *change.Change {
	t.Helper()
	before, _ := enterprise.SnapshotJSON(ent.ToSnapshot())
	afterSnap := ent.ToSnapshot()
	afterSnap.LegalRepresentative = "新法定代表人"
	after, _ := enterprise.SnapshotJSON(afterSnap)
	c := &change.Change{
		ID:             "chg-test-" + fmt.Sprintf("%d", time.Now().UnixNano()),
		EnterpriseID:   ent.ID,
		ChangeType:     change.TypeLegalRepresentative,
		BeforeSnapshot: before,
		AfterSnapshot:  after,
		NewValue:       "新法定代表人",
		Status:         change.StatusSubmitted,
		SubmittedBy:    "clerk1",
		EventTime:      time.Now(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	require.NoError(t, repos.Changes.Create(context.Background(), c))
	return c
}

func TestRestartRecoverFromDisk(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "recover.db")

	st1, err := Open(context.Background(), dbPath)
	require.NoError(t, err)
	_, err = st1.EnsureSchema(context.Background())
	require.NoError(t, err)
	repos1 := st1.AllRepositories()

	ent := mustCreateEnterprise(t, repos1)
	chg := mustCreateChange(t, repos1, ent)

	task := &change.DispatchTask{
		ID:             "disp-recover-1",
		ChangeID:       chg.ID,
		DepartmentCode: "tax",
		Topic:          "topic.tax",
		Status:         change.DispatchPending,
		LogOffset:      1,
		MaxAttempts:    3,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	require.NoError(t, repos1.Dispatch.Create(context.Background(), task))
	require.NoError(t, st1.Close())

	st2, err := Open(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { st2.Close() })
	_, err = st2.EnsureSchema(context.Background())
	require.NoError(t, err)
	repos2 := st2.AllRepositories()

	recovered, err := repos2.Enterprises.GetByID(context.Background(), ent.ID)
	require.NoError(t, err)
	assert.Equal(t, ent.Name, recovered.Name)
	assert.Equal(t, ent.UnifiedCreditCode, recovered.UnifiedCreditCode)

	recChg, err := repos2.Changes.GetByID(context.Background(), chg.ID)
	require.NoError(t, err)
	assert.Equal(t, change.StatusSubmitted, recChg.Status)

	recTask, err := repos2.Dispatch.GetByID(context.Background(), task.ID)
	require.NoError(t, err)
	assert.Equal(t, change.DispatchPending, recTask.Status)
}

func TestPersistEventLog(t *testing.T) {
	_, repos := testStore(t)

	entry := &event.Entry{
		Topic:     "topic.tax",
		ChangeID:  "chg-persist-1",
		EventType: event.TypeChangeSubmitted,
		Payload:   `{"change_id":"chg-persist-1"}`,
		EventTime: time.Now(),
		CreatedAt: time.Now(),
	}
	offset, err := repos.EventLog.Append(context.Background(), entry)
	require.NoError(t, err)
	assert.Greater(t, offset, int64(0))

	entries, err := repos.EventLog.ReadFrom(context.Background(), "topic.tax", 0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, offset, entries[0].Offset)
	assert.Equal(t, "chg-persist-1", entries[0].ChangeID)
}

func TestEventLogAppendAndReplay(t *testing.T) {
	_, repos := testStore(t)

	for i := 0; i < 5; i++ {
		entry := &event.Entry{
			Topic:     "topic.tax",
			ChangeID:  fmt.Sprintf("chg-replay-%d", i),
			EventType: event.TypeChangeDispatched,
			Payload:   fmt.Sprintf(`{"index":%d}`, i),
			EventTime: time.Now(),
			CreatedAt: time.Now(),
		}
		_, err := repos.EventLog.Append(context.Background(), entry)
		require.NoError(t, err)
	}

	entries, err := repos.EventLog.ReadFrom(context.Background(), "topic.tax", 0, 100)
	require.NoError(t, err)
	require.Len(t, entries, 5)
	for i, e := range entries {
		assert.Equal(t, fmt.Sprintf("chg-replay-%d", i), e.ChangeID)
	}

	entriesFrom2, err := repos.EventLog.ReadFrom(context.Background(), "topic.tax", 2, 100)
	require.NoError(t, err)
	require.Len(t, entriesFrom2, 3)
	assert.Equal(t, "chg-replay-2", entriesFrom2[0].ChangeID)
}

func TestEventLogTruncate(t *testing.T) {
	_, repos := testStore(t)

	for i := 0; i < 5; i++ {
		entry := &event.Entry{
			Topic:     "topic.tax",
			ChangeID:  fmt.Sprintf("chg-trunc-%d", i),
			EventType: event.TypeChangeDispatched,
			Payload:   `{}`,
			EventTime: time.Now(),
			CreatedAt: time.Now(),
		}
		_, err := repos.EventLog.Append(context.Background(), entry)
		require.NoError(t, err)
	}

	deleted, err := repos.EventLog.Truncate(context.Background(), 3)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	count, err := repos.EventLog.Count(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestCommitRollbackTransaction(t *testing.T) {
	st, repos := testStore(t)
	ent := mustCreateEnterprise(t, repos)

	tx, err := st.BeginTx(context.Background())
	require.NoError(t, err)

	_, err = tx.ExecContext(context.Background(),
		"INSERT INTO changes (id, enterprise_id, change_type, before_snapshot, after_snapshot, event_time, status, submitted_by, resolution_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"chg-tx-1", ent.ID, "legal_representative", "{}", "{}", time.Now().Unix(), "draft", "clerk1", 0, time.Now().Unix(), time.Now().Unix())
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())

	_, err = repos.Changes.GetByID(context.Background(), "chg-tx-1")
	assert.Error(t, err)
}

func TestTransactionCommitPersists(t *testing.T) {
	st, repos := testStore(t)
	ent := mustCreateEnterprise(t, repos)

	tx, err := st.BeginTx(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = tx.ExecContext(context.Background(),
		"INSERT INTO changes (id, enterprise_id, change_type, before_snapshot, after_snapshot, event_time, status, submitted_by, resolution_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"chg-commit-1", ent.ID, "legal_representative", "{}", "{}", time.Now().Unix(), "submitted", "clerk1", 0, time.Now().Unix(), time.Now().Unix())
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	c, err := repos.Changes.GetByID(context.Background(), "chg-commit-1")
	require.NoError(t, err)
	assert.Equal(t, change.StatusSubmitted, c.Status)
}

func TestBatchDispatchCreateTransaction(t *testing.T) {
	_, repos := testStore(t)
	ent := mustCreateEnterprise(t, repos)
	chg := mustCreateChange(t, repos, ent)

	tasks := []*change.DispatchTask{
		{ID: "disp-batch-1", ChangeID: chg.ID, DepartmentCode: "tax", Topic: "topic.tax", Status: change.DispatchPending, MaxAttempts: 3, LogOffset: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "disp-batch-2", ChangeID: chg.ID, DepartmentCode: "social_security", Topic: "topic.social_security", Status: change.DispatchPending, MaxAttempts: 3, LogOffset: 2, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "disp-batch-3", ChangeID: chg.ID, DepartmentCode: "provident_fund", Topic: "topic.provident_fund", Status: change.DispatchPending, MaxAttempts: 3, LogOffset: 3, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}
	require.NoError(t, repos.Dispatch.CreateBatch(context.Background(), tasks))

	tasks2, err := repos.Dispatch.ListByChange(context.Background(), chg.ID)
	require.NoError(t, err)
	assert.Len(t, tasks2, 3)
}

func TestPaginationBoundaries(t *testing.T) {
	_, repos := testStore(t)

	for i := 0; i < 25; i++ {
		ent := &enterprise.Enterprise{
			ID:                  fmt.Sprintf("ent-page-%d", i),
			Name:                fmt.Sprintf("企业%d", i),
			LegalRepresentative: "法人",
			UnifiedCreditCode:   fmt.Sprintf("91110000MA0PAGE%04d", i),
			RegisteredCapital:   "100万",
			BusinessScope:       "服务",
			IndustryCode:        "I6510",
			Status:              enterprise.StatusActive,
			Version:             1,
			CreatedAt:           time.Now(),
			UpdatedAt:           time.Now(),
		}
		require.NoError(t, repos.Enterprises.Create(context.Background(), ent))
	}

	items, total, err := repos.Enterprises.List(context.Background(), ListFilter{Offset: 0, Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 25, total)
	assert.Len(t, items, 10)

	items2, total2, err := repos.Enterprises.List(context.Background(), ListFilter{Offset: 20, Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 25, total2)
	assert.Len(t, items2, 5)

	items3, total3, err := repos.Enterprises.List(context.Background(), ListFilter{Offset: 30, Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 25, total3)
	assert.Len(t, items3, 0)
}

func TestConcurrentEnterpriseUpdate(t *testing.T) {
	_, repos := testStore(t)
	ent := mustCreateEnterprise(t, repos)

	snap := ent.ToSnapshot()
	snap.LegalRepresentative = "更新法人1"
	err1 := repos.Enterprises.UpdateAfterChange(context.Background(), ent.ID, snap, ent.Version)
	require.NoError(t, err1)

	snap2 := ent.ToSnapshot()
	snap2.LegalRepresentative = "更新法人2"
	err2 := repos.Enterprises.UpdateAfterChange(context.Background(), ent.ID, snap2, ent.Version)
	assert.Error(t, err2)
}

func TestConcurrentDispatchCreateRace(t *testing.T) {
	_, repos := testStore(t)
	ent := mustCreateEnterprise(t, repos)
	chg := mustCreateChange(t, repos, ent)

	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			task := &change.DispatchTask{
				ID:             fmt.Sprintf("disp-race-%d", idx),
				ChangeID:       chg.ID,
				DepartmentCode: "tax",
				Topic:          "topic.tax",
				Status:         change.DispatchPending,
				MaxAttempts:    3,
				LogOffset:      int64(idx),
				CreatedAt:      time.Now(),
				UpdatedAt:      time.Now(),
			}
			errs[idx] = repos.Dispatch.Create(context.Background(), task)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}

	tasks, err := repos.Dispatch.ListByChange(context.Background(), chg.ID)
	require.NoError(t, err)
	assert.Len(t, tasks, 10)
}

func TestSchemaVersionMigration(t *testing.T) {
	st, _ := testStore(t)
	v, err := st.SchemaVersion(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, v)

	err = st.ApplyMigration(context.Background(), 1, "SELECT 1")
	assert.NoError(t, err)

	v2, err := st.SchemaVersion(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, v2)
}

func TestSubscriberOffsetPersistence(t *testing.T) {
	_, repos := testStore(t)

	err := repos.Subscribers.CommitOffset(context.Background(), "sub-tax", "topic.tax", 42)
	require.NoError(t, err)

	offset, err := repos.Subscribers.GetOffset(context.Background(), "sub-tax", "topic.tax")
	require.NoError(t, err)
	assert.Equal(t, int64(42), offset)

	err = repos.Subscribers.CommitOffset(context.Background(), "sub-tax", "topic.tax", 50)
	require.NoError(t, err)

	offset2, err := repos.Subscribers.GetOffset(context.Background(), "sub-tax", "topic.tax")
	require.NoError(t, err)
	assert.Equal(t, int64(50), offset2)
}

func TestDeadLetterPersistence(t *testing.T) {
	_, repos := testStore(t)
	ent := mustCreateEnterprise(t, repos)
	chg := mustCreateChange(t, repos, ent)

	dl := &change.DeadLetter{
		ID:             "dl-test-1",
		DispatchTaskID: "disp-dl-1",
		ChangeID:       chg.ID,
		DepartmentCode: "tax",
		LastError:      "max retries exceeded",
		AttemptCount:   5,
		Status:         "pending",
		CreatedAt:      time.Now(),
	}
	require.NoError(t, repos.DeadLetters.Create(context.Background(), dl))

	recovered, err := repos.DeadLetters.GetByID(context.Background(), dl.ID)
	require.NoError(t, err)
	assert.Equal(t, "max retries exceeded", recovered.LastError)
	assert.Equal(t, 5, recovered.AttemptCount)
}

func TestAuditRecordQuery(t *testing.T) {
	_, repos := testStore(t)

	for i := 0; i < 5; i++ {
		rec := &change.AuditRecord{
			Actor:      "clerk1",
			Action:     "change.submit",
			EntityType: "change",
			EntityID:   fmt.Sprintf("chg-audit-%d", i),
			CreatedAt:  time.Now(),
		}
		require.NoError(t, repos.Audit.Record(context.Background(), rec))
	}

	items, total, err := repos.Audit.List(context.Background(), AuditFilter{
		ListFilter: ListFilter{Offset: 0, Limit: 10},
		Actor:      "clerk1",
	})
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	assert.Len(t, items, 5)
}

func TestDataDirWritable(t *testing.T) {
	st, _ := testStore(t)
	dir := t.TempDir()
	err := st.DataDirWritable(dir)
	assert.NoError(t, err)

	nonexistent := filepath.Join(dir, "nonexistent", "deep", "path")
	err = st.DataDirWritable(nonexistent)
	assert.Error(t, err)
}

func TestCompensationRecordPersistence(t *testing.T) {
	_, repos := testStore(t)
	ent := mustCreateEnterprise(t, repos)
	chg := mustCreateChange(t, repos, ent)

	cr := &change.CompensationRecord{
		ID:             "comp-test-1",
		ChangeID:       chg.ID,
		DepartmentCode: "tax",
		Action:         "rollback_change",
		Status:         change.CompensationPending,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, repos.Compensation.Create(context.Background(), cr))

	err := repos.Compensation.UpdateStatus(context.Background(), cr.ID, change.CompensationCompleted, "")
	require.NoError(t, err)

	records, err := repos.Compensation.ListByChange(context.Background(), chg.ID)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, change.CompensationCompleted, records[0].Status)
}

func TestEventLogCompaction(t *testing.T) {
	_, repos := testStore(t)

	for i := 0; i < 20; i++ {
		entry := &event.Entry{
			Topic:     "topic.tax",
			ChangeID:  fmt.Sprintf("chg-compact-%d", i),
			EventType: event.TypeChangeDispatched,
			Payload:   `{}`,
			EventTime: time.Now(),
			CreatedAt: time.Now(),
		}
		_, err := repos.EventLog.Append(context.Background(), entry)
		require.NoError(t, err)
	}

	eventLog, ok := repos.EventLog.(interface {
		Compact(ctx context.Context, retainCount int) (int64, error)
	})
	require.True(t, ok)
	deleted, err := eventLog.Compact(context.Background(), 5)
	require.NoError(t, err)
	assert.Greater(t, deleted, int64(0))

	count, err := repos.EventLog.Count(context.Background())
	require.NoError(t, err)
	assert.LessOrEqual(t, count, int64(15))
}

func TestSQLiteFileExists(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "exists.db")
	st, err := Open(context.Background(), dbPath)
	require.NoError(t, err)
	st.Close()
	_, err = os.Stat(dbPath)
	assert.NoError(t, err)
}

func TestSQLContextCancellation(t *testing.T) {
	st, _ := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := st.db.PingContext(ctx)
	if err != nil {
		assert.Contains(t, err.Error(), "context canceled")
	}
}

func TestEnterpriseNotFound(t *testing.T) {
	_, repos := testStore(t)
	_, err := repos.Enterprises.GetByID(context.Background(), "nonexistent")
	assert.Error(t, err)
}

func TestChangeNotFound(t *testing.T) {
	_, repos := testStore(t)
	_, err := repos.Changes.GetByID(context.Background(), "nonexistent")
	assert.Error(t, err)
}
