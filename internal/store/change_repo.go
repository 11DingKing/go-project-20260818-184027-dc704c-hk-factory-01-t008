package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"regdispatch/internal/domain/change"
	"regdispatch/internal/errorsx"
)

type changeRepo struct{ store *Store }

func (r *changeRepo) Create(ctx context.Context, c *change.Change) error {
	_, err := r.store.db.ExecContext(ctx, `
		INSERT INTO changes
			(id, enterprise_id, change_type, before_snapshot, after_snapshot,
			 evidence_materials, event_time, status, submitted_by, revoked_reason,
			 resolution_order, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.ID, c.EnterpriseID, c.ChangeType, c.BeforeSnapshot, c.AfterSnapshot,
		c.EvidenceMaterials, c.EventTime.Unix(), string(c.Status), c.SubmittedBy,
		c.RevokedReason, c.ResolutionOrder, c.CreatedAt.Unix(), c.UpdatedAt.Unix())
	if err != nil {
		return fmt.Errorf("insert change: %w", err)
	}
	return nil
}

func scanChange(scanner interface{ Scan(...any) error }) (*change.Change, error) {
	var c change.Change
	var eventTime, createdAt, updatedAt int64
	var evidence, revokedReason sql.NullString
	err := scanner.Scan(
		&c.ID, &c.EnterpriseID, &c.ChangeType, &c.BeforeSnapshot, &c.AfterSnapshot,
		&evidence, &eventTime, &c.Status, &c.SubmittedBy, &revokedReason,
		&c.ResolutionOrder, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	c.EvidenceMaterials = evidence.String
	c.RevokedReason = revokedReason.String
	c.EventTime = time.Unix(eventTime, 0)
	c.CreatedAt = time.Unix(createdAt, 0)
	c.UpdatedAt = time.Unix(updatedAt, 0)
	return &c, nil
}

func (r *changeRepo) GetByID(ctx context.Context, id string) (*change.Change, error) {
	row := r.store.db.QueryRowContext(ctx, `
		SELECT id, enterprise_id, change_type, before_snapshot, after_snapshot,
		       evidence_materials, event_time, status, submitted_by, revoked_reason,
		       resolution_order, created_at, updated_at
		FROM changes WHERE id = ?`, id)
	c, err := scanChange(row)
	if err == sql.ErrNoRows {
		return nil, errorsx.NotFound("change", id)
	}
	if err != nil {
		return nil, fmt.Errorf("get change %s: %w", id, err)
	}
	return c, nil
}

func (r *changeRepo) List(ctx context.Context, f ChangeFilter) ([]*change.Change, int, error) {
	if f.Limit <= 0 {
		f.Limit = 20
	}
	var conditions []string
	var args []any
	if f.EnterpriseID != "" {
		conditions = append(conditions, "enterprise_id = ?")
		args = append(args, f.EnterpriseID)
	}
	if f.DepartmentCode != "" {
		conditions = append(conditions, "id IN (SELECT change_id FROM dispatch_tasks WHERE department_code = ?)")
		args = append(args, f.DepartmentCode)
	}
	if f.Status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, f.Status)
	}
	if f.FromTime != nil {
		conditions = append(conditions, "event_time >= ?")
		args = append(args, f.FromTime.Unix())
	}
	if f.ToTime != nil {
		conditions = append(conditions, "event_time <= ?")
		args = append(args, f.ToTime.Unix())
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + joinStrings(conditions, " AND ")
	}

	var total int
	countArgs := make([]any, len(args))
	copy(countArgs, args)
	if err := r.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM changes"+where, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count changes: %w", err)
	}

	queryArgs := append(args, f.Limit, f.Offset)
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, enterprise_id, change_type, before_snapshot, after_snapshot,
		       evidence_materials, event_time, status, submitted_by, revoked_reason,
		       resolution_order, created_at, updated_at
		FROM changes`+where+`
		ORDER BY event_time ASC, created_at ASC LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list changes: %w", err)
	}
	defer rows.Close()
	var result []*change.Change
	for rows.Next() {
		c, err := scanChange(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan change: %w", err)
		}
		result = append(result, c)
	}
	return result, total, rows.Err()
}

func joinStrings(parts []string, sep string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += sep
		}
		result += p
	}
	return result
}

func (r *changeRepo) ListByEnterprise(ctx context.Context, enterpriseID string) ([]*change.Change, error) {
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, enterprise_id, change_type, before_snapshot, after_snapshot,
		       evidence_materials, event_time, status, submitted_by, revoked_reason,
		       resolution_order, created_at, updated_at
		FROM changes WHERE enterprise_id = ?
		ORDER BY event_time ASC`, enterpriseID)
	if err != nil {
		return nil, fmt.Errorf("list changes by enterprise: %w", err)
	}
	defer rows.Close()
	var result []*change.Change
	for rows.Next() {
		c, err := scanChange(rows)
		if err != nil {
			return nil, fmt.Errorf("scan change: %w", err)
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *changeRepo) UpdateStatus(ctx context.Context, id string, status change.Status) error {
	res, err := r.store.db.ExecContext(ctx,
		"UPDATE changes SET status = ?, updated_at = ? WHERE id = ?",
		string(status), time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("update change status: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errorsx.NotFound("change", id)
	}
	return nil
}

func (r *changeRepo) SetRevoked(ctx context.Context, id string, reason string) error {
	_, err := r.store.db.ExecContext(ctx,
		"UPDATE changes SET status = ?, revoked_reason = ?, updated_at = ? WHERE id = ?",
		string(change.StatusRevoked), reason, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("revoke change: %w", err)
	}
	return nil
}

func (r *changeRepo) SetResolutionOrder(ctx context.Context, id string, order int) error {
	_, err := r.store.db.ExecContext(ctx,
		"UPDATE changes SET resolution_order = ?, updated_at = ? WHERE id = ?",
		order, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("set resolution order: %w", err)
	}
	return nil
}

func (r *changeRepo) CountByEnterprise(ctx context.Context, enterpriseID string) (int, error) {
	var count int
	err := r.store.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM changes WHERE enterprise_id = ?", enterpriseID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count changes by enterprise: %w", err)
	}
	return count, nil
}

// CompareAndSetStatus atomically transitions a change's status only if the
// current status matches expectedFrom. Returns ErrConcurrentUpdate if the
// current status does not match.
func (r *changeRepo) CompareAndSetStatus(ctx context.Context, id string, expectedFrom, newTo change.Status) error {
	res, err := r.store.db.ExecContext(ctx,
		"UPDATE changes SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
		string(newTo), time.Now().Unix(), id, string(expectedFrom))
	if err != nil {
		return fmt.Errorf("compare and set change status: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errorsx.Wrap("conflict", "status was changed by another transaction", errorsx.ErrConcurrentUpdate)
	}
	return nil
}
