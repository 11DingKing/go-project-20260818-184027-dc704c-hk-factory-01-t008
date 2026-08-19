package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"regdispatch/internal/domain/enterprise"
	"regdispatch/internal/errorsx"
)

type enterpriseRepo struct{ store *Store }

func (r *enterpriseRepo) Create(ctx context.Context, e *enterprise.Enterprise) error {
	_, err := r.store.db.ExecContext(ctx, `
		INSERT INTO enterprises
			(id, name, legal_representative, unified_credit_code, registered_capital,
			 business_scope, industry_code, status, version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.Name, e.LegalRepresentative, e.UnifiedCreditCode,
		e.RegisteredCapital, e.BusinessScope, e.IndustryCode,
		e.Status, e.Version, e.CreatedAt.Unix(), e.UpdatedAt.Unix(),
	)
	if err != nil {
		return fmt.Errorf("insert enterprise: %w", err)
	}
	return nil
}

func (r *enterpriseRepo) scanEnterprise(row interface{}) (*enterprise.Enterprise, error) {
	var e enterprise.Enterprise
	var createdAt, updatedAt int64
	var err error
	switch v := row.(type) {
	case *sql.Row:
		err = v.Scan(&e.ID, &e.Name, &e.LegalRepresentative, &e.UnifiedCreditCode,
			&e.RegisteredCapital, &e.BusinessScope, &e.IndustryCode,
			&e.Status, &e.Version, &createdAt, &updatedAt)
	case *sql.Rows:
		err = v.Scan(&e.ID, &e.Name, &e.LegalRepresentative, &e.UnifiedCreditCode,
			&e.RegisteredCapital, &e.BusinessScope, &e.IndustryCode,
			&e.Status, &e.Version, &createdAt, &updatedAt)
	default:
		return nil, fmt.Errorf("unsupported scanner type")
	}
	if err != nil {
		return nil, err
	}
	e.CreatedAt = time.Unix(createdAt, 0)
	e.UpdatedAt = time.Unix(updatedAt, 0)
	return &e, nil
}

func (r *enterpriseRepo) GetByID(ctx context.Context, id string) (*enterprise.Enterprise, error) {
	row := r.store.db.QueryRowContext(ctx, `
		SELECT id, name, legal_representative, unified_credit_code, registered_capital,
		       business_scope, industry_code, status, version, created_at, updated_at
		FROM enterprises WHERE id = ?`, id)
	e, err := r.scanEnterprise(row)
	if err == sql.ErrNoRows {
		return nil, errorsx.NotFound("enterprise", id)
	}
	if err != nil {
		return nil, fmt.Errorf("get enterprise %s: %w", id, err)
	}
	return e, nil
}

func (r *enterpriseRepo) GetByCreditCode(ctx context.Context, code string) (*enterprise.Enterprise, error) {
	row := r.store.db.QueryRowContext(ctx, `
		SELECT id, name, legal_representative, unified_credit_code, registered_capital,
		       business_scope, industry_code, status, version, created_at, updated_at
		FROM enterprises WHERE unified_credit_code = ?`, code)
	e, err := r.scanEnterprise(row)
	if err == sql.ErrNoRows {
		return nil, errorsx.NotFound("enterprise", code)
	}
	if err != nil {
		return nil, fmt.Errorf("get enterprise by credit code %s: %w", code, err)
	}
	return e, nil
}

func (r *enterpriseRepo) List(ctx context.Context, f ListFilter) ([]*enterprise.Enterprise, int, error) {
	if f.Limit <= 0 {
		f.Limit = 20
	}
	var total int
	if err := r.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM enterprises").Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count enterprises: %w", err)
	}
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, name, legal_representative, unified_credit_code, registered_capital,
		       business_scope, industry_code, status, version, created_at, updated_at
		FROM enterprises ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		f.Limit, f.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list enterprises: %w", err)
	}
	defer rows.Close()
	var result []*enterprise.Enterprise
	for rows.Next() {
		e, err := r.scanEnterprise(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan enterprise: %w", err)
		}
		result = append(result, e)
	}
	return result, total, rows.Err()
}

func (r *enterpriseRepo) UpdateAfterChange(ctx context.Context, id string, snap enterprise.Snapshot, version int) error {
	res, err := r.store.db.ExecContext(ctx, `
		UPDATE enterprises SET name = ?, legal_representative = ?, registered_capital = ?,
		       business_scope = ?, version = version + 1, updated_at = ?
		WHERE id = ? AND version = ?`,
		snap.Name, snap.LegalRepresentative, snap.RegisteredCapital,
		snap.BusinessScope, time.Now().Unix(), id, version)
	if err != nil {
		return fmt.Errorf("update enterprise after change: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errorsx.Wrap("conflict", "enterprise version mismatch", errorsx.ErrConcurrentUpdate)
	}
	return nil
}

func (r *enterpriseRepo) UpdateStatus(ctx context.Context, id string, status string) error {
	_, err := r.store.db.ExecContext(ctx,
		"UPDATE enterprises SET status = ?, updated_at = ? WHERE id = ?",
		status, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("update enterprise status: %w", err)
	}
	return nil
}
