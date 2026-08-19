package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"regdispatch/internal/domain/event"
)

type eventLogRepo struct{ store *Store }

func scanEvent(scanner interface{ Scan(...any) error }) (*event.Entry, error) {
	var e event.Entry
	var eventTime, createdAt int64
	var deptCode sql.NullString
	err := scanner.Scan(&e.Offset, &e.Topic, &e.ChangeID, &deptCode, &e.EventType,
		&e.Payload, &eventTime, &e.Sequence, &createdAt)
	if err != nil {
		return nil, err
	}
	e.DepartmentCode = deptCode.String
	e.EventTime = time.Unix(eventTime, 0)
	e.CreatedAt = time.Unix(createdAt, 0)
	return &e, nil
}

func (r *eventLogRepo) Append(ctx context.Context, entry *event.Entry) (int64, error) {
	tx, err := r.store.BeginTx(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin event log append tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
		INSERT INTO event_log (topic, change_id, department_code, event_type, payload, event_time, sequence, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.Topic, entry.ChangeID, nullString(entry.DepartmentCode),
		string(entry.EventType), entry.Payload, entry.EventTime.Unix(),
		entry.Sequence, entry.CreatedAt.Unix())
	if err != nil {
		return 0, fmt.Errorf("insert event log: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get last insert id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit event log append: %w", err)
	}
	return id, nil
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (r *eventLogRepo) ReadFrom(ctx context.Context, topic string, offset int64, limit int) ([]*event.Entry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT offset, topic, change_id, department_code, event_type, payload, event_time, sequence, created_at
		FROM event_log WHERE topic = ? AND offset > ? ORDER BY offset ASC LIMIT ?`,
		topic, offset, limit)
	if err != nil {
		return nil, fmt.Errorf("read event log from %d: %w", offset, err)
	}
	defer rows.Close()
	var result []*event.Entry
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (r *eventLogRepo) ReadByChange(ctx context.Context, changeID string) ([]*event.Entry, error) {
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT offset, topic, change_id, department_code, event_type, payload, event_time, sequence, created_at
		FROM event_log WHERE change_id = ? ORDER BY offset ASC`, changeID)
	if err != nil {
		return nil, fmt.Errorf("read event log by change: %w", err)
	}
	defer rows.Close()
	var result []*event.Entry
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (r *eventLogRepo) ReadAll(ctx context.Context, offset int64, limit int) ([]*event.Entry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT offset, topic, change_id, department_code, event_type, payload, event_time, sequence, created_at
		FROM event_log WHERE offset > ? ORDER BY offset ASC LIMIT ?`, offset, limit)
	if err != nil {
		return nil, fmt.Errorf("read all event log: %w", err)
	}
	defer rows.Close()
	var result []*event.Entry
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (r *eventLogRepo) Truncate(ctx context.Context, beforeOffset int64) (int64, error) {
	res, err := r.store.db.ExecContext(ctx,
		"DELETE FROM event_log WHERE offset < ?", beforeOffset)
	if err != nil {
		return 0, fmt.Errorf("truncate event log: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return n, nil
}

func (r *eventLogRepo) MaxOffset(ctx context.Context) (int64, error) {
	var max sql.NullInt64
	err := r.store.db.QueryRowContext(ctx, "SELECT MAX(offset) FROM event_log").Scan(&max)
	if err != nil {
		return 0, fmt.Errorf("max offset: %w", err)
	}
	if !max.Valid {
		return 0, nil
	}
	return max.Int64, nil
}

func (r *eventLogRepo) Count(ctx context.Context) (int64, error) {
	var count int64
	err := r.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_log").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count event log: %w", err)
	}
	return count, nil
}

// Compact removes event log entries that have been fully processed by all
// subscribers for their respective topics, keeping only the last retainCount
// entries per topic for replay safety.
func (r *eventLogRepo) Compact(ctx context.Context, retainCount int) (int64, error) {
	tx, err := r.store.BeginTx(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin compact tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var totalDeleted int64
	for _, topic := range event.AllTopics() {
		var maxOffset int64
		err := tx.QueryRowContext(ctx,
			"SELECT COALESCE(MAX(offset), 0) FROM event_log WHERE topic = ?", topic).Scan(&maxOffset)
		if err != nil {
			return 0, fmt.Errorf("max offset for topic %s: %w", topic, err)
		}
		cutoff := maxOffset - int64(retainCount)
		if cutoff <= 0 {
			continue
		}
		res, err := tx.ExecContext(ctx,
			"DELETE FROM event_log WHERE topic = ? AND offset < ?", topic, cutoff)
		if err != nil {
			return 0, fmt.Errorf("delete old events for topic %s: %w", topic, err)
		}
		n, _ := res.RowsAffected()
		totalDeleted += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit compact: %w", err)
	}
	return totalDeleted, nil
}
