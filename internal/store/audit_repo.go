package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"regdispatch/internal/domain/change"
)

type auditRepo struct{ store *Store }

func (r *auditRepo) Record(ctx context.Context, rec *change.AuditRecord) error {
	_, err := r.store.db.ExecContext(ctx, `
		INSERT INTO audit_records (actor, action, entity_type, entity_id, details, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		rec.Actor, rec.Action, rec.EntityType, rec.EntityID, rec.Details, rec.CreatedAt.Unix())
	if err != nil {
		return fmt.Errorf("insert audit record: %w", err)
	}
	return nil
}

func (r *auditRepo) List(ctx context.Context, f AuditFilter) ([]*change.AuditRecord, int, error) {
	if f.Limit <= 0 {
		f.Limit = 20
	}
	conditions := []string{}
	args := []any{}
	if f.Actor != "" {
		conditions = append(conditions, "actor = ?")
		args = append(args, f.Actor)
	}
	if f.EntityType != "" {
		conditions = append(conditions, "entity_type = ?")
		args = append(args, f.EntityType)
	}
	if f.EntityID != "" {
		conditions = append(conditions, "entity_id = ?")
		args = append(args, f.EntityID)
	}
	if f.FromTime != nil {
		conditions = append(conditions, "created_at >= ?")
		args = append(args, f.FromTime.Unix())
	}
	if f.ToTime != nil {
		conditions = append(conditions, "created_at <= ?")
		args = append(args, f.ToTime.Unix())
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + joinStrings(conditions, " AND ")
	}

	var total int
	countArgs := make([]any, len(args))
	copy(countArgs, args)
	if err := r.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit_records"+where, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count audit records: %w", err)
	}

	queryArgs := append(args, f.Limit, f.Offset)
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, actor, action, entity_type, entity_id, details, created_at
		FROM audit_records`+where+`
		ORDER BY created_at DESC LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list audit records: %w", err)
	}
	defer rows.Close()
	var result []*change.AuditRecord
	for rows.Next() {
		var rec change.AuditRecord
		var details sql.NullString
		var createdAt int64
		if err := rows.Scan(&rec.ID, &rec.Actor, &rec.Action, &rec.EntityType,
			&rec.EntityID, &details, &createdAt); err != nil {
			return nil, 0, fmt.Errorf("scan audit record: %w", err)
		}
		rec.Details = details.String
		rec.CreatedAt = time.Unix(createdAt, 0)
		result = append(result, &rec)
	}
	return result, total, rows.Err()
}
