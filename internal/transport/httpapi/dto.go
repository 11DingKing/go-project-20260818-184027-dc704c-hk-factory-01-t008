package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"regdispatch/internal/domain/change"
	"regdispatch/internal/domain/enterprise"
)

// ErrorResponse is the standard JSON error envelope.
type ErrorResponse struct {
	Error     string `json:"error"`
	Category  string `json:"category,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// PaginatedResponse wraps a list with total count and pagination cursor.
type PaginatedResponse[T any] struct {
	Items  []T `json:"items"`
	Total  int `json:"total"`
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

// EnterpriseRequest is the payload for registering an enterprise.
type EnterpriseRequest struct {
	Name                string `json:"name"`
	LegalRepresentative string `json:"legal_representative"`
	UnifiedCreditCode   string `json:"unified_credit_code"`
	RegisteredCapital   string `json:"registered_capital"`
	BusinessScope       string `json:"business_scope"`
	IndustryCode        string `json:"industry_code"`
}

// EnterpriseResponse is the enterprise representation returned by the API.
type EnterpriseResponse struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	LegalRepresentative string `json:"legal_representative"`
	UnifiedCreditCode   string `json:"unified_credit_code"`
	RegisteredCapital   string `json:"registered_capital"`
	BusinessScope       string `json:"business_scope"`
	IndustryCode        string `json:"industry_code"`
	Status              string `json:"status"`
	Version             int    `json:"version"`
	CreatedAt           string `json:"created_at"`
	UpdatedAt           string `json:"updated_at"`
}

func toEnterpriseResponse(e *enterprise.Enterprise) EnterpriseResponse {
	return EnterpriseResponse{
		ID:                  e.ID,
		Name:                e.Name,
		LegalRepresentative: e.LegalRepresentative,
		UnifiedCreditCode:   e.UnifiedCreditCode,
		RegisteredCapital:   e.RegisteredCapital,
		BusinessScope:       e.BusinessScope,
		IndustryCode:        e.IndustryCode,
		Status:              e.Status,
		Version:             e.Version,
		CreatedAt:           e.CreatedAt.Format(time.RFC3339),
		UpdatedAt:           e.UpdatedAt.Format(time.RFC3339),
	}
}

// ChangeRequest is the payload for submitting a registration change.
type ChangeRequest struct {
	EnterpriseID      string `json:"enterprise_id"`
	ChangeType        string `json:"change_type"`
	NewValue          string `json:"new_value"`
	EvidenceMaterials string `json:"evidence_materials,omitempty"`
	SubmittedBy       string `json:"submitted_by"`
}

// ChangeResponse is the change representation returned by the API.
type ChangeResponse struct {
	ID              string `json:"id"`
	EnterpriseID    string `json:"enterprise_id"`
	ChangeType      string `json:"change_type"`
	NewValue        string `json:"new_value"`
	Status          string `json:"status"`
	EventTime       string `json:"event_time"`
	SubmittedBy     string `json:"submitted_by"`
	ResolutionOrder int    `json:"resolution_order"`
	RevokedReason   string `json:"revoked_reason,omitempty"`
}

func toChangeResponse(c *change.Change) ChangeResponse {
	return ChangeResponse{
		ID:              c.ID,
		EnterpriseID:    c.EnterpriseID,
		ChangeType:      c.ChangeType,
		NewValue:        c.NewValue,
		Status:          string(c.Status),
		EventTime:       c.EventTime.Format(time.RFC3339),
		SubmittedBy:     c.SubmittedBy,
		ResolutionOrder: c.ResolutionOrder,
		RevokedReason:   c.RevokedReason,
	}
}

// DispatchTaskResponse is the dispatch task representation returned by the API.
type DispatchTaskResponse struct {
	ID             string `json:"id"`
	ChangeID       string `json:"change_id"`
	DepartmentCode string `json:"department_code"`
	Status         string `json:"status"`
	AttemptCount   int    `json:"attempt_count"`
	MaxAttempts    int    `json:"max_attempts"`
	AckedBy        string `json:"acked_by,omitempty"`
	Result         string `json:"result,omitempty"`
	ResultError    string `json:"result_error,omitempty"`
	CreatedAt      string `json:"created_at"`
}

func toDispatchTaskResponse(t *change.DispatchTask) DispatchTaskResponse {
	resp := DispatchTaskResponse{
		ID:             t.ID,
		ChangeID:       t.ChangeID,
		DepartmentCode: t.DepartmentCode,
		Status:         string(t.Status),
		AttemptCount:   t.AttemptCount,
		MaxAttempts:    t.MaxAttempts,
		AckedBy:        t.AckedBy,
		Result:         t.Result,
		ResultError:    t.ResultError,
		CreatedAt:      t.CreatedAt.Format(time.RFC3339),
	}
	return resp
}

// AckRequest is the payload for acknowledging a dispatch.
type AckRequest struct {
	AckedBy string `json:"acked_by"`
}

// CompleteRequest is the payload for completing a dispatch.
type CompleteRequest struct {
	Operator string `json:"operator"`
	Result   string `json:"result"`
}

// FailRequest is the payload for failing a dispatch.
type FailRequest struct {
	Operator string `json:"operator"`
	Error    string `json:"error"`
}

// DispatchRequest is the payload for triggering dispatch.
type DispatchRequest struct {
	Operator string `json:"operator"`
}

// RevokeRequest is the payload for revoking a change.
type RevokeRequest struct {
	Operator string `json:"operator"`
	Reason   string `json:"reason"`
}

// CompensateRequest is the payload for triggering compensation.
type CompensateRequest struct {
	Operator string `json:"operator"`
}

// RedeliverRequest is the payload for manually redelivering a dead letter.
type RedeliverRequest struct {
	Operator string `json:"operator"`
}

// parsePagination extracts offset and limit from query parameters.
func parsePagination(r *http.Request) (int, int) {
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	return offset, limit
}

// writeJSON serialises and writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes a standard error response.
func writeError(w http.ResponseWriter, status int, category, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg, Category: category})
}
