package change

import (
	"fmt"
	"time"

	"regdispatch/internal/domain/enterprise"
)

// Status enumerates the lifecycle states of a registration change.
type Status string

const (
	StatusDraft          Status = "draft"
	StatusSubmitted      Status = "submitted"
	StatusDispatching    Status = "dispatching"
	StatusPartialSuccess Status = "partial_success"
	StatusPartialFailed  Status = "partial_failed"
	StatusCompensating   Status = "compensating"
	StatusCompleted      Status = "completed"
	StatusRolledBack     Status = "rolled_back"
	StatusRevoked        Status = "revoked"
)

// ChangeType enumerates the fields of an enterprise that can be changed.
const (
	TypeLegalRepresentative = "legal_representative"
	TypeName                = "name"
	TypeRegisteredCapital   = "registered_capital"
	TypeBusinessScope       = "business_scope"
)

// Change represents a single registration change submitted by an intake clerk.
type Change struct {
	ID                string    `json:"id"`
	EnterpriseID      string    `json:"enterprise_id"`
	ChangeType        string    `json:"change_type"`
	BeforeSnapshot    string    `json:"before_snapshot"`
	AfterSnapshot     string    `json:"after_snapshot"`
	NewValue          string    `json:"new_value"`
	EvidenceMaterials string    `json:"evidence_materials,omitempty"`
	EventTime         time.Time `json:"event_time"`
	Status            Status    `json:"status"`
	SubmittedBy       string    `json:"submitted_by"`
	RevokedReason     string    `json:"revoked_reason,omitempty"`
	ResolutionOrder   int       `json:"resolution_order"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// Before parses the stored before snapshot.
func (c *Change) Before() (enterprise.Snapshot, error) {
	return enterprise.ParseSnapshot(c.BeforeSnapshot)
}

// After parses the stored after snapshot.
func (c *Change) After() (enterprise.Snapshot, error) {
	return enterprise.ParseSnapshot(c.AfterSnapshot)
}

// Validate checks that the change is structurally valid.
func (c *Change) Validate() error {
	if c.EnterpriseID == "" {
		return fmt.Errorf("enterprise_id is required")
	}
	if c.ChangeType == "" {
		return fmt.Errorf("change_type is required")
	}
	if !isValidChangeType(c.ChangeType) {
		return fmt.Errorf("unsupported change type: %s", c.ChangeType)
	}
	if c.NewValue == "" {
		return fmt.Errorf("new_value is required")
	}
	if c.SubmittedBy == "" {
		return fmt.Errorf("submitted_by is required")
	}
	if c.EventTime.IsZero() {
		return fmt.Errorf("event_time is required")
	}
	return nil
}

func isValidChangeType(t string) bool {
	switch t {
	case TypeLegalRepresentative, TypeName, TypeRegisteredCapital, TypeBusinessScope:
		return true
	}
	return false
}

// DispatchStatus enumerates the lifecycle of a single dispatch task.
type DispatchStatus string

const (
	DispatchPending    DispatchStatus = "pending"
	DispatchDelivered  DispatchStatus = "delivered"
	DispatchAcked      DispatchStatus = "acknowledged"
	DispatchProcessing DispatchStatus = "processing"
	DispatchSucceeded  DispatchStatus = "succeeded"
	DispatchFailed     DispatchStatus = "failed"
	DispatchTimedOut   DispatchStatus = "timed_out"
	DispatchDeadLetter DispatchStatus = "dead_letter"
)

// DispatchTask represents one change dispatched to one department.
type DispatchTask struct {
	ID             string         `json:"id"`
	ChangeID       string         `json:"change_id"`
	DepartmentCode string         `json:"department_code"`
	Topic          string         `json:"topic"`
	Status         DispatchStatus `json:"status"`
	LogOffset      int64          `json:"log_offset"`
	AttemptCount   int            `json:"attempt_count"`
	MaxAttempts    int            `json:"max_attempts"`
	NextRetryAt    *time.Time     `json:"next_retry_at,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	AckedBy        string         `json:"acked_by,omitempty"`
	AckedAt        *time.Time     `json:"acked_at,omitempty"`
	Result         string         `json:"result,omitempty"`
	ResultError    string         `json:"result_error,omitempty"`
	CompletedAt    *time.Time     `json:"completed_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// CanRetry returns true if the task has not exceeded its max attempts.
func (t *DispatchTask) CanRetry() bool {
	return t.AttemptCount < t.MaxAttempts
}

// IsTerminal returns true if the dispatch has reached a final state.
func (t *DispatchTask) IsTerminal() bool {
	return t.Status == DispatchSucceeded || t.Status == DispatchDeadLetter
}

// DeadLetter records a permanently failed dispatch.
type DeadLetter struct {
	ID             string    `json:"id"`
	DispatchTaskID string    `json:"dispatch_task_id"`
	ChangeID       string    `json:"change_id"`
	DepartmentCode string    `json:"department_code"`
	LastError      string    `json:"last_error"`
	AttemptCount   int       `json:"attempt_count"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
}

// CompensationRecord tracks a rollback action for a partially failed change.
type CompensationRecord struct {
	ID             string     `json:"id"`
	ChangeID       string     `json:"change_id"`
	DepartmentCode string     `json:"department_code"`
	Action         string     `json:"action"`
	Status         string     `json:"status"`
	AttemptCount   int        `json:"attempt_count"`
	LastError      string     `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
}

const (
	CompensationPending   = "pending"
	CompensationCompleted = "completed"
	CompensationFailed    = "failed"
)

// AuditRecord captures who did what when for traceability.
type AuditRecord struct {
	ID         int64     `json:"id"`
	Actor      string    `json:"actor"`
	Action     string    `json:"action"`
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	Details    string    `json:"details,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// SubscriberOffset tracks how far a subscriber has consumed a topic.
type SubscriberOffset struct {
	SubscriberID    string     `json:"subscriber_id"`
	Topic           string     `json:"topic"`
	CommittedOffset int64      `json:"committed_offset"`
	LastSeenAt      *time.Time `json:"last_seen_at,omitempty"`
}
