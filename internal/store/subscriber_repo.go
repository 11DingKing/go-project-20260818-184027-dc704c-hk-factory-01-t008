package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"regdispatch/internal/domain/change"
)

type subscriberRepo struct{ store *Store }

func (r *subscriberRepo) GetOffset(ctx context.Context, subscriberID, topic string) (int64, error) {
	var offset int64
	err := r.store.db.QueryRowContext(ctx,
		"SELECT committed_offset FROM subscriber_offsets WHERE subscriber_id = ? AND topic = ?",
		subscriberID, topic).Scan(&offset)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get subscriber offset: %w", err)
	}
	return offset, nil
}

func (r *subscriberRepo) CommitOffset(ctx context.Context, subscriberID, topic string, offset int64) error {
	_, err := r.store.db.ExecContext(ctx, `
		INSERT INTO subscriber_offsets (subscriber_id, topic, committed_offset, last_seen_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(subscriber_id, topic) DO UPDATE SET
			committed_offset = excluded.committed_offset,
			last_seen_at = excluded.last_seen_at`,
		subscriberID, topic, offset, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("commit subscriber offset: %w", err)
	}
	return nil
}

func (r *subscriberRepo) Touch(ctx context.Context, subscriberID, topic string, at time.Time) error {
	_, err := r.store.db.ExecContext(ctx,
		"UPDATE subscriber_offsets SET last_seen_at = ? WHERE subscriber_id = ? AND topic = ?",
		at.Unix(), subscriberID, topic)
	if err != nil {
		return fmt.Errorf("touch subscriber: %w", err)
	}
	return nil
}

func (r *subscriberRepo) ListStale(ctx context.Context, before time.Time) ([]*change.SubscriberOffset, error) {
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT subscriber_id, topic, committed_offset, last_seen_at
		FROM subscriber_offsets WHERE last_seen_at IS NOT NULL AND last_seen_at < ?
		ORDER BY last_seen_at ASC`, before.Unix())
	if err != nil {
		return nil, fmt.Errorf("list stale subscribers: %w", err)
	}
	defer rows.Close()
	var result []*change.SubscriberOffset
	for rows.Next() {
		var so change.SubscriberOffset
		var lastSeen sql.NullInt64
		if err := rows.Scan(&so.SubscriberID, &so.Topic, &so.CommittedOffset, &lastSeen); err != nil {
			return nil, fmt.Errorf("scan subscriber offset: %w", err)
		}
		if lastSeen.Valid {
			tm := time.Unix(lastSeen.Int64, 0)
			so.LastSeenAt = &tm
		}
		result = append(result, &so)
	}
	return result, rows.Err()
}
