package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type traceRepo struct{ store *Store }

func (r *traceRepo) Record(ctx context.Context, upstream, method, path, reqBody, respBody string, statusCode int, duration time.Duration, errStr string) error {
	_, err := r.store.db.ExecContext(ctx, `
		INSERT INTO upstream_traces (upstream_name, method, path, request_body, response_body, status_code, duration_ms, error, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		upstream, method, path, reqBody, respBody, statusCode,
		duration.Milliseconds(), errStr, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("insert upstream trace: %w", err)
	}
	return nil
}

func (r *traceRepo) List(ctx context.Context, f TraceFilter) ([]*UpstreamTrace, int, error) {
	if f.Limit <= 0 {
		f.Limit = 20
	}
	conditions := []string{}
	args := []any{}
	if f.UpstreamName != "" {
		conditions = append(conditions, "upstream_name = ?")
		args = append(args, f.UpstreamName)
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + joinStrings(conditions, " AND ")
	}

	var total int
	countArgs := make([]any, len(args))
	copy(countArgs, args)
	if err := r.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM upstream_traces"+where, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count upstream traces: %w", err)
	}

	queryArgs := append(args, f.Limit, f.Offset)
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT id, upstream_name, method, path, request_body, response_body, status_code, duration_ms, error, created_at
		FROM upstream_traces`+where+`
		ORDER BY created_at DESC LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list upstream traces: %w", err)
	}
	defer rows.Close()
	var result []*UpstreamTrace
	for rows.Next() {
		var t UpstreamTrace
		var reqBody, respBody, errMsg sql.NullString
		var createdAt int64
		if err := rows.Scan(&t.ID, &t.UpstreamName, &t.Method, &t.Path,
			&reqBody, &respBody, &t.StatusCode, &t.DurationMs, &errMsg, &createdAt); err != nil {
			return nil, 0, fmt.Errorf("scan trace: %w", err)
		}
		t.RequestBody = reqBody.String
		t.ResponseBody = respBody.String
		t.Error = errMsg.String
		t.CreatedAt = time.Unix(createdAt, 0)
		result = append(result, &t)
	}
	return result, total, rows.Err()
}
