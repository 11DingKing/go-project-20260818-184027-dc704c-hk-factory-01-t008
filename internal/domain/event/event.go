package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// Type identifies the kind of event recorded in the append-only event log.
type Type string

const (
	TypeChangeSubmitted       Type = "change.submitted"
	TypeChangeDispatched      Type = "change.dispatched"
	TypeDispatchAcked         Type = "dispatch.acked"
	TypeDispatchSucceeded     Type = "dispatch.succeeded"
	TypeDispatchFailed        Type = "dispatch.failed"
	TypeDispatchRetried       Type = "dispatch.retried"
	TypeCompensationStarted   Type = "compensation.started"
	TypeCompensationCompleted Type = "compensation.completed"
	TypeChangeRevoked         Type = "change.revoked"
	TypeChangeCompleted       Type = "change.completed"
)

// Entry is a single record in the append-only event log. The Offset is
// assigned by the store and serves as the consumption position for
// subscribers.
type Entry struct {
	Offset         int64     `json:"offset"`
	Topic          string    `json:"topic"`
	ChangeID       string    `json:"change_id"`
	DepartmentCode string    `json:"department_code,omitempty"`
	EventType      Type      `json:"event_type"`
	Payload        string    `json:"payload"`
	EventTime      time.Time `json:"event_time"`
	Sequence       int       `json:"sequence"`
	CreatedAt      time.Time `json:"created_at"`
}

// Payload is the structured content embedded in an event log entry.
type Payload struct {
	ChangeID       string `json:"change_id"`
	EnterpriseID   string `json:"enterprise_id"`
	ChangeType     string `json:"change_type"`
	DepartmentCode string `json:"department_code,omitempty"`
	Before         string `json:"before,omitempty"`
	After          string `json:"after,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Operator       string `json:"operator,omitempty"`
	Attempt        int    `json:"attempt,omitempty"`
}

// EncodePayload serialises a Payload to JSON.
func EncodePayload(p Payload) (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encode event payload: %w", err)
	}
	return string(b), nil
}

// DecodePayload deserialises a Payload from JSON.
func DecodePayload(raw string) (Payload, error) {
	var p Payload
	if raw == "" {
		return p, nil
	}
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return Payload{}, fmt.Errorf("decode event payload: %w", err)
	}
	return p, nil
}

// AllTopics returns the set of topics that subscribers can consume.
func AllTopics() []string {
	return []string{
		"topic.tax",
		"topic.social_security",
		"topic.provident_fund",
		"topic.industry_supervisor",
		"topic.market_regulator",
	}
}
