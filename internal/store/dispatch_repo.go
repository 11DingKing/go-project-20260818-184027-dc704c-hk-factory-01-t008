package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"regdispatch/internal/domain/change"
	"regdispatch/internal/errorsx"
)

type dispatchRepo struct{ store *Store }

func scanDispatch(scanner interface{ Scan(...any) error }) (*change.DispatchTask, error) {
	var t change.DispatchTask
	var logOffset int64
	var createdAt, updatedAt int64
	var nextRetryAt, ackedAt, completedAt sql.NullInt64
	var lastError, ackedBy, result, resultError sql.NullString
	err := scanner.Scan(
		&t.ID, &t.ChangeID, &t.DepartmentCode, &t.Topic, &t.Status,
		&logOffset, &t.AttemptCount, &t.MaxAttempts,
		&nextRetryAt, &lastError, &ackedBy, &ackedAt,
		&result, &resultError, &completedAt, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	t.LogOffset = logOffset
	if nextRetryAt.Valid {
		tm := time.Unix(nextRetryAt.Int64, 0)
		t.NextRetryAt = &tm
	}
	t.LastError = lastError.String
	t.AckedBy = ackedBy.String
	if ackedAt.Valid {
		tm := time.Unix(ackedAt.Int64, 0)
		t.AckedAt = &tm
	}
	t.Result = result.String
	t.ResultError = resultError.String
	if completedAt.Valid {
		tm := time.Unix(completedAt.Int64, 0)
		t.CompletedAt = &tm
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	t.UpdatedAt = time.Unix(updatedAt, 0)
	return &t, nil
}

func (r *dispatchRepo) Create(ctx context.Context, t *change.DispatchTask) error {
	return r.createTasks(ctx, []*change.DispatchTask{t})
}

func (r *dispatchRepo) CreateBatch(ctx context.Context, tasks []*change.DispatchTask) error {
	return r.createTasks(ctx, tasks)
}

func (r *dispatchRepo) createTasks(ctx context.Context, tasks []*change.DispatchTask) error {
	tx, err := r.store.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin batch dispatch tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO dispatch_tasks
			(id, change_id, department_code, topic, status, log_offset,
			 attempt_count, max_attempts, next_retry_at, last_error,
			 acked_by, acked_at, result, result_error, completed_at,
			 created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare dispatch insert: %w", err)
	}
	defer stmt.Close()
	for _, t := range tasks {
		var nextRetry, ackedAt, completedAt any
		if t.NextRetryAt != nil {
			nextRetry = t.NextRetryAt.Unix()
		}
		if t.AckedAt != nil {
			ackedAt = t.AckedAt.Unix()
		}
		if t.CompletedAt != nil {
			completedAt = t.CompletedAt.Unix()
		}
		if _, err := stmt.ExecContext(ctx,
			t.ID, t.ChangeID, t.DepartmentCode, t.Topic, string(t.Status),
			t.LogOffset, t.AttemptCount, t.MaxAttempts, nextRetry, t.LastError,
			t.AckedBy, ackedAt, t.Result, t.ResultError, completedAt,
			t.CreatedAt.Unix(), t.UpdatedAt.Unix()); err != nil {
			return fmt.Errorf("insert dispatch task %s: %w", t.ID, err)
		}
	}
	return tx.Commit()
}

func (r *dispatchRepo) GetByID(ctx context.Context, id string) (*change.DispatchTask, error) {
	row := r.store.db.QueryRowContext(ctx, `
		SELECT id, change_id, department_code, topic, status, log_offset,
		       attempt_count, max_attempts, next_retry_at, last_error,
		       acked_by, acked_at, result, result_error, completed_at,
		       created_at, updated_at
		FROM dispatch_tasks WHERE id = ?`, id)
	t, err := scanDispatch(row)
	if err == sql.ErrNoRows {
		return nil, errorsx.NotFound("dispatch_task", id)
	}
	if err != nil {
		return nil, fmt.Errorf("get dispatch task %s: %w", id, err)
	}
	return t, nil
}

func (r *dispatchRepo) ListByChange(ctx context.Context, changeID string) ([]*change.DispatchTask, error) {
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, change_id, department_code, topic, status, log_offset,
		       attempt_count, max_attempts, next_retry_at, last_error,
		       acked_by, acked_at, result, result_error, completed_at,
		       created_at, updated_at
		FROM dispatch_tasks WHERE change_id = ? ORDER BY created_at ASC`, changeID)
	if err != nil {
		return nil, fmt.Errorf("list dispatch by change: %w", err)
	}
	defer rows.Close()
	var result []*change.DispatchTask
	for rows.Next() {
		t, err := scanDispatch(rows)
		if err != nil {
			return nil, fmt.Errorf("scan dispatch: %w", err)
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (r *dispatchRepo) ListByDepartment(ctx context.Context, deptCode string, f DispatchFilter) ([]*change.DispatchTask, int, error) {
	if f.Limit <= 0 {
		f.Limit = 20
	}
	conditions := []string{"department_code = ?"}
	args := []any{deptCode}
	if f.ChangeID != "" {
		conditions = append(conditions, "change_id = ?")
		args = append(args, f.ChangeID)
	}
	if f.Status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, f.Status)
	}
	where := " WHERE " + joinStrings(conditions, " AND ")

	var total int
	countArgs := make([]any, len(args))
	copy(countArgs, args)
	if err := r.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dispatch_tasks"+where, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count dispatch tasks: %w", err)
	}
	queryArgs := append(args, f.Limit, f.Offset)
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, change_id, department_code, topic, status, log_offset,
		       attempt_count, max_attempts, next_retry_at, last_error,
		       acked_by, acked_at, result, result_error, completed_at,
		       created_at, updated_at
		FROM dispatch_tasks`+where+`
		ORDER BY created_at DESC LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list dispatch by department: %w", err)
	}
	defer rows.Close()
	var result []*change.DispatchTask
	for rows.Next() {
		t, err := scanDispatch(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan dispatch: %w", err)
		}
		result = append(result, t)
	}
	return result, total, rows.Err()
}

func (r *dispatchRepo) ListExpired(ctx context.Context, before time.Time) ([]*change.DispatchTask, error) {
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, change_id, department_code, topic, status, log_offset,
		       attempt_count, max_attempts, next_retry_at, last_error,
		       acked_by, acked_at, result, result_error, completed_at,
		       created_at, updated_at
		FROM dispatch_tasks
		WHERE status IN ('delivered', 'acknowledged') AND next_retry_at IS NOT NULL AND next_retry_at < ?
		ORDER BY next_retry_at ASC`, before.Unix())
	if err != nil {
		return nil, fmt.Errorf("list expired dispatch: %w", err)
	}
	defer rows.Close()
	var result []*change.DispatchTask
	for rows.Next() {
		t, err := scanDispatch(rows)
		if err != nil {
			return nil, fmt.Errorf("scan dispatch: %w", err)
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (r *dispatchRepo) ListPendingRetry(ctx context.Context, before time.Time) ([]*change.DispatchTask, error) {
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, change_id, department_code, topic, status, log_offset,
		       attempt_count, max_attempts, next_retry_at, last_error,
		       acked_by, acked_at, result, result_error, completed_at,
		       created_at, updated_at
		FROM dispatch_tasks
		WHERE status = 'timed_out' AND next_retry_at IS NOT NULL AND next_retry_at < ?
		ORDER BY next_retry_at ASC`, before.Unix())
	if err != nil {
		return nil, fmt.Errorf("list pending retry: %w", err)
	}
	defer rows.Close()
	var result []*change.DispatchTask
	for rows.Next() {
		t, err := scanDispatch(rows)
		if err != nil {
			return nil, fmt.Errorf("scan dispatch: %w", err)
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (r *dispatchRepo) UpdateStatus(ctx context.Context, id string, status change.DispatchStatus, result, errMsg string) error {
	now := time.Now().Unix()
	var completedAt any
	if status == change.DispatchSucceeded || status == change.DispatchFailed {
		completedAt = now
	}
	_, err := r.store.db.ExecContext(ctx, `
		UPDATE dispatch_tasks SET status = ?, result = ?, result_error = ?,
		       completed_at = ?, updated_at = ? WHERE id = ?`,
		string(status), result, errMsg, completedAt, now, id)
	if err != nil {
		return fmt.Errorf("update dispatch status: %w", err)
	}
	return nil
}

func (r *dispatchRepo) SetAcked(ctx context.Context, id, ackedBy string, ackedAt time.Time) error {
	res, err := r.store.db.ExecContext(ctx, `
		UPDATE dispatch_tasks SET status = ?, acked_by = ?, acked_at = ?, updated_at = ?
		WHERE id = ? AND status IN ('delivered', 'timed_out')`,
		string(change.DispatchAcked), ackedBy, ackedAt.Unix(), ackedAt.Unix(), id)
	if err != nil {
		return fmt.Errorf("ack dispatch: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errorsx.InvalidTransition("current", "ack", "dispatch_task")
	}
	return nil
}

func (r *dispatchRepo) IncrementAttempt(ctx context.Context, id string, nextRetry time.Time) error {
	_, err := r.store.db.ExecContext(ctx, `
		UPDATE dispatch_tasks SET status = ?, attempt_count = attempt_count + 1,
		       next_retry_at = ?, updated_at = ? WHERE id = ?`,
		string(change.DispatchPending), nextRetry.Unix(), time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("increment attempt: %w", err)
	}
	return nil
}

func (r *dispatchRepo) MoveToDeadLetter(ctx context.Context, id string, errMsg string, attempts int) error {
	_, err := r.store.db.ExecContext(ctx, `
		UPDATE dispatch_tasks SET status = ?, last_error = ?, updated_at = ? WHERE id = ?`,
		string(change.DispatchDeadLetter), errMsg, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("move to dead letter: %w", err)
	}
	return nil
}
