package store

import (
	"context"
	"time"

	"regdispatch/internal/domain/change"
	"regdispatch/internal/domain/enterprise"
	"regdispatch/internal/domain/event"
)

// ListFilter controls pagination for list queries.
type ListFilter struct {
	Offset int
	Limit  int
}

// ChangeFilter adds business filters to pagination.
type ChangeFilter struct {
	ListFilter
	EnterpriseID   string
	DepartmentCode string
	Status         string
	FromTime       *time.Time
	ToTime         *time.Time
}

// DispatchFilter controls dispatch task queries.
type DispatchFilter struct {
	ListFilter
	ChangeID       string
	DepartmentCode string
	Status         string
	ExpiredBefore  *time.Time
}

// AuditFilter controls audit record queries.
type AuditFilter struct {
	ListFilter
	Actor      string
	EntityType string
	EntityID   string
	FromTime   *time.Time
	ToTime     *time.Time
}

// DeadLetterFilter controls dead letter queries.
type DeadLetterFilter struct {
	ListFilter
	DepartmentCode string
	Status         string
}

// TraceFilter controls upstream trace queries.
type TraceFilter struct {
	ListFilter
	UpstreamName string
}

// EnterpriseRepository manages enterprise persistence.
type EnterpriseRepository interface {
	Create(ctx context.Context, e *enterprise.Enterprise) error
	GetByID(ctx context.Context, id string) (*enterprise.Enterprise, error)
	GetByCreditCode(ctx context.Context, code string) (*enterprise.Enterprise, error)
	List(ctx context.Context, f ListFilter) ([]*enterprise.Enterprise, int, error)
	UpdateAfterChange(ctx context.Context, id string, snap enterprise.Snapshot, version int) error
	UpdateStatus(ctx context.Context, id string, status string) error
}

// ChangeRepository manages change persistence.
type ChangeRepository interface {
	Create(ctx context.Context, c *change.Change) error
	GetByID(ctx context.Context, id string) (*change.Change, error)
	List(ctx context.Context, f ChangeFilter) ([]*change.Change, int, error)
	ListByEnterprise(ctx context.Context, enterpriseID string) ([]*change.Change, error)
	UpdateStatus(ctx context.Context, id string, status change.Status) error
	CompareAndSetStatus(ctx context.Context, id string, expectedFrom, newTo change.Status) error
	SetRevoked(ctx context.Context, id string, reason string) error
	SetResolutionOrder(ctx context.Context, id string, order int) error
	CountByEnterprise(ctx context.Context, enterpriseID string) (int, error)
}

// DispatchRepository manages dispatch task persistence.
type DispatchRepository interface {
	Create(ctx context.Context, t *change.DispatchTask) error
	CreateBatch(ctx context.Context, tasks []*change.DispatchTask) error
	GetByID(ctx context.Context, id string) (*change.DispatchTask, error)
	ListByChange(ctx context.Context, changeID string) ([]*change.DispatchTask, error)
	ListByDepartment(ctx context.Context, deptCode string, f DispatchFilter) ([]*change.DispatchTask, int, error)
	ListExpired(ctx context.Context, before time.Time) ([]*change.DispatchTask, error)
	UpdateStatus(ctx context.Context, id string, status change.DispatchStatus, result, errMsg string) error
	SetAcked(ctx context.Context, id, ackedBy string, ackedAt time.Time) error
	IncrementAttempt(ctx context.Context, id string, nextRetry time.Time) error
	MoveToDeadLetter(ctx context.Context, id string, err string, attempts int) error
	ListPendingRetry(ctx context.Context, before time.Time) ([]*change.DispatchTask, error)
}

// EventLogRepository manages the append-only event log.
type EventLogRepository interface {
	Append(ctx context.Context, entry *event.Entry) (int64, error)
	ReadFrom(ctx context.Context, topic string, offset int64, limit int) ([]*event.Entry, error)
	ReadByChange(ctx context.Context, changeID string) ([]*event.Entry, error)
	ReadAll(ctx context.Context, offset int64, limit int) ([]*event.Entry, error)
	Truncate(ctx context.Context, beforeOffset int64) (int64, error)
	MaxOffset(ctx context.Context) (int64, error)
	Count(ctx context.Context) (int64, error)
}

// SubscriberRepository manages subscriber offset persistence.
type SubscriberRepository interface {
	GetOffset(ctx context.Context, subscriberID, topic string) (int64, error)
	CommitOffset(ctx context.Context, subscriberID, topic string, offset int64) error
	Touch(ctx context.Context, subscriberID, topic string, at time.Time) error
	ListStale(ctx context.Context, before time.Time) ([]*change.SubscriberOffset, error)
}

// AuditRepository manages audit record persistence.
type AuditRepository interface {
	Record(ctx context.Context, r *change.AuditRecord) error
	List(ctx context.Context, f AuditFilter) ([]*change.AuditRecord, int, error)
}

// DeadLetterRepository manages dead letter persistence.
type DeadLetterRepository interface {
	Create(ctx context.Context, dl *change.DeadLetter) error
	List(ctx context.Context, f DeadLetterFilter) ([]*change.DeadLetter, int, error)
	UpdateStatus(ctx context.Context, id string, status string) error
	GetByID(ctx context.Context, id string) (*change.DeadLetter, error)
}

// CompensationRepository manages compensation record persistence.
type CompensationRepository interface {
	Create(ctx context.Context, cr *change.CompensationRecord) error
	UpdateStatus(ctx context.Context, id string, status string, errMsg string) error
	ListByChange(ctx context.Context, changeID string) ([]*change.CompensationRecord, error)
	ListPending(ctx context.Context) ([]*change.CompensationRecord, error)
}

// TraceRepository manages upstream request/response traces.
type TraceRepository interface {
	Record(ctx context.Context, upstream, method, path, reqBody, respBody string, statusCode int, duration time.Duration, err string) error
	List(ctx context.Context, f TraceFilter) ([]*UpstreamTrace, int, error)
}

// UpstreamTrace is a persisted request/response log entry.
type UpstreamTrace struct {
	ID           int64     `json:"id"`
	UpstreamName string    `json:"upstream_name"`
	Method       string    `json:"method"`
	Path         string    `json:"path"`
	RequestBody  string    `json:"request_body,omitempty"`
	ResponseBody string    `json:"response_body,omitempty"`
	StatusCode   int       `json:"status_code"`
	DurationMs   int64     `json:"duration_ms"`
	Error        string    `json:"error,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// Repositories aggregates all repository interfaces for convenience.
type Repositories struct {
	Enterprises  EnterpriseRepository
	Changes      ChangeRepository
	Dispatch     DispatchRepository
	EventLog     EventLogRepository
	Subscribers  SubscriberRepository
	Audit        AuditRepository
	DeadLetters  DeadLetterRepository
	Compensation CompensationRepository
	Traces       TraceRepository
}
