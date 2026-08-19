package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"regdispatch/internal/domain/change"
	"regdispatch/internal/errorsx"
)

type deadLetterRepo struct{ store *Store }

func (r *deadLetterRepo) Create(ctx context.Context, dl *change.DeadLetter) error {
	_, err := r.store.db.ExecContext(ctx, `
		INSERT INTO dead_letters (id, dispatch_task_id, change_id, department_code, last_error, attempt_count, created_at, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		dl.ID, dl.DispatchTaskID, dl.ChangeID, dl.DepartmentCode,
		dl.LastError, dl.AttemptCount, dl.CreatedAt.Unix(), dl.Status)
	if err != nil {
		return fmt.Errorf("insert dead letter: %w", err)
	}
	return nil
}

func (r *deadLetterRepo) GetByID(ctx context.Context, id string) (*change.DeadLetter, error) {
	row := r.store.db.QueryRowContext(ctx, `
		SELECT id, dispatch_task_id, change_id, department_code, last_error, attempt_count, created_at, status
		FROM dead_letters WHERE id = ?`, id)
	var dl change.DeadLetter
	var createdAt int64
	err := row.Scan(&dl.ID, &dl.DispatchTaskID, &dl.ChangeID, &dl.DepartmentCode,
		&dl.LastError, &dl.AttemptCount, &createdAt, &dl.Status)
	if err == sql.ErrNoRows {
		return nil, errorsx.NotFound("dead_letter", id)
	}
	if err != nil {
		return nil, fmt.Errorf("get dead letter %s: %w", id, err)
	}
	dl.CreatedAt = time.Unix(createdAt, 0)
	return &dl, nil
}

func (r *deadLetterRepo) List(ctx context.Context, f DeadLetterFilter) ([]*change.DeadLetter, int, error) {
	if f.Limit <= 0 {
		f.Limit = 20
	}
	conditions := []string{}
	args := []any{}
	if f.DepartmentCode != "" {
		conditions = append(conditions, "department_code = ?")
		args = append(args, f.DepartmentCode)
	}
	if f.Status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, f.Status)
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + joinStrings(conditions, " AND ")
	}

	var total int
	countArgs := make([]any, len(args))
	copy(countArgs, args)
	if err := r.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dead_letters"+where, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count dead letters: %w", err)
	}

	queryArgs := append(args, f.Limit, f.Offset)
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, dispatch_task_id, change_id, department_code, last_error, attempt_count, created_at, status
		FROM dead_letters`+where+`
		ORDER BY created_at DESC LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()
	var result []*change.DeadLetter
	for rows.Next() {
		var dl change.DeadLetter
		var createdAt int64
		if err := rows.Scan(&dl.ID, &dl.DispatchTaskID, &dl.ChangeID, &dl.DepartmentCode,
			&dl.LastError, &dl.AttemptCount, &createdAt, &dl.Status); err != nil {
			return nil, 0, fmt.Errorf("scan dead letter: %w", err)
		}
		dl.CreatedAt = time.Unix(createdAt, 0)
		result = append(result, &dl)
	}
	return result, total, rows.Err()
}

func (r *deadLetterRepo) UpdateStatus(ctx context.Context, id string, status string) error {
	res, err := r.store.db.ExecContext(ctx,
		"UPDATE dead_letters SET status = ? WHERE id = ?", status, id)
	if err != nil {
		return fmt.Errorf("update dead letter status: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errorsx.NotFound("dead_letter", id)
	}
	return nil
}
