package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"regdispatch/internal/domain/change"
)

type compensationRepo struct{ store *Store }

func (r *compensationRepo) Create(ctx context.Context, cr *change.CompensationRecord) error {
	_, err := r.store.db.ExecContext(ctx, `
		INSERT INTO compensation_records (id, change_id, department_code, action, status, attempt_count, last_error, created_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cr.ID, cr.ChangeID, cr.DepartmentCode, cr.Action, cr.Status,
		cr.AttemptCount, cr.LastError, cr.CreatedAt.Unix(), nullTime(cr.CompletedAt))
	if err != nil {
		return fmt.Errorf("insert compensation record: %w", err)
	}
	return nil
}

func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.Unix()
}

func (r *compensationRepo) UpdateStatus(ctx context.Context, id string, status string, errMsg string) error {
	now := time.Now().Unix()
	var completedAt any
	if status == change.CompensationCompleted {
		completedAt = now
	}
	_, err := r.store.db.ExecContext(ctx, `
		UPDATE compensation_records SET status = ?, last_error = ?, attempt_count = attempt_count + 1, completed_at = COALESCE(?, completed_at)
		WHERE id = ?`, status, errMsg, completedAt, id)
	if err != nil {
		return fmt.Errorf("update compensation status: %w", err)
	}
	return nil
}

func (r *compensationRepo) ListByChange(ctx context.Context, changeID string) ([]*change.CompensationRecord, error) {
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, change_id, department_code, action, status, attempt_count, last_error, created_at, completed_at
		FROM compensation_records WHERE change_id = ? ORDER BY created_at ASC`, changeID)
	if err != nil {
		return nil, fmt.Errorf("list compensation by change: %w", err)
	}
	defer rows.Close()
	var result []*change.CompensationRecord
	for rows.Next() {
		cr, err := scanCompensation(rows)
		if err != nil {
			return nil, fmt.Errorf("scan compensation: %w", err)
		}
		result = append(result, cr)
	}
	return result, rows.Err()
}

func (r *compensationRepo) ListPending(ctx context.Context) ([]*change.CompensationRecord, error) {
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, change_id, department_code, action, status, attempt_count, last_error, created_at, completed_at
		FROM compensation_records WHERE status = 'pending' ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("list pending compensation: %w", err)
	}
	defer rows.Close()
	var result []*change.CompensationRecord
	for rows.Next() {
		cr, err := scanCompensation(rows)
		if err != nil {
			return nil, fmt.Errorf("scan compensation: %w", err)
		}
		result = append(result, cr)
	}
	return result, rows.Err()
}

func scanCompensation(scanner interface{ Scan(...any) error }) (*change.CompensationRecord, error) {
	var cr change.CompensationRecord
	var createdAt int64
	var lastError sql.NullString
	var completedAt sql.NullInt64
	err := scanner.Scan(&cr.ID, &cr.ChangeID, &cr.DepartmentCode, &cr.Action,
		&cr.Status, &cr.AttemptCount, &lastError, &createdAt, &completedAt)
	if err != nil {
		return nil, err
	}
	cr.LastError = lastError.String
	cr.CreatedAt = time.Unix(createdAt, 0)
	if completedAt.Valid {
		tm := time.Unix(completedAt.Int64, 0)
		cr.CompletedAt = &tm
	}
	return &cr, nil
}
