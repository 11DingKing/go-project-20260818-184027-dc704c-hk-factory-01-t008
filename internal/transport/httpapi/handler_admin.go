package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"regdispatch/internal/errorsx"
	"regdispatch/internal/store"
)

func (s *Server) handleListAudit(w http.ResponseWriter, r *http.Request) {
	offset, limit := parsePagination(r)
	f := store.AuditFilter{
		ListFilter: store.ListFilter{Offset: offset, Limit: limit},
		Actor:      r.URL.Query().Get("actor"),
		EntityType: r.URL.Query().Get("entity_type"),
		EntityID:   r.URL.Query().Get("entity_id"),
	}
	if from := r.URL.Query().Get("from_time"); from != "" {
		if t, err := time.Parse(time.RFC3339, from); err == nil {
			f.FromTime = &t
		}
	}
	if to := r.URL.Query().Get("to_time"); to != "" {
		if t, err := time.Parse(time.RFC3339, to); err == nil {
			f.ToTime = &t
		}
	}
	items, total, err := s.orch.ListAuditRecords(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, PaginatedResponse[any]{
		Items: toSlice(items), Total: total, Offset: offset, Limit: limit,
	})
}

func toSlice(items any) []any {
	b, _ := json.Marshal(items)
	var result []any
	_ = json.Unmarshal(b, &result)
	return result
}

func (s *Server) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	offset, limit := parsePagination(r)
	f := store.DeadLetterFilter{
		ListFilter:     store.ListFilter{Offset: offset, Limit: limit},
		DepartmentCode: r.URL.Query().Get("department_code"),
	}
	if status := r.URL.Query().Get("status"); status != "" {
		f.Status = status
	}
	items, total, err := s.orch.ListDeadLetters(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, PaginatedResponse[any]{
		Items: toSlice(items), Total: total, Offset: offset, Limit: limit,
	})
}

func (s *Server) handleRedeliverDeadLetter(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req RedeliverRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Operator == "" {
		req.Operator = "system"
	}
	if err := s.orch.RedeliverDeadLetter(r.Context(), id, req.Operator); err != nil {
		if errorsx.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "not_found", "dead letter not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "redelivered", "dead_letter_id": id})
}

func (s *Server) handleExportReconciliation(w http.ResponseWriter, r *http.Request) {
	enterpriseID := r.URL.Query().Get("enterprise_id")
	departmentCode := r.URL.Query().Get("department_code")
	entries, err := s.orch.ExportReconciliation(r.Context(), enterpriseID, departmentCode)
	if err != nil {
		var be *errorsx.BusinessError
		if errors.As(err, &be) {
			writeError(w, http.StatusBadRequest, be.Category, be.Message)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "count": len(entries)})
}

func (s *Server) handleViewBacklog(w http.ResponseWriter, r *http.Request) {
	backlog, err := s.orch.ViewBacklog(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, backlog)
}

func (s *Server) handleListUpstreams(w http.ResponseWriter, r *http.Request) {
	if s.selector == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	ups := s.selector.All()
	result := make([]map[string]string, 0, len(ups))
	for _, up := range ups {
		status := up.Status()
		result = append(result, map[string]string{
			"name":          up.Name,
			"url":           up.URL,
			"breaker_state": status.State,
			"failures":      strconv.FormatUint(uint64(status.Failures), 10),
		})
	}
	writeJSON(w, http.StatusOK, result)
}
