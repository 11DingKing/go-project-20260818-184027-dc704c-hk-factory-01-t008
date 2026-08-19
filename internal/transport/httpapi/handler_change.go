package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"regdispatch/internal/domain/change"
	"regdispatch/internal/errorsx"
	"regdispatch/internal/store"
)

func (s *Server) handleSubmitChange(w http.ResponseWriter, r *http.Request) {
	var req ChangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "parse_error", "invalid request body")
		return
	}
	c := &change.Change{
		EnterpriseID:      req.EnterpriseID,
		ChangeType:        req.ChangeType,
		NewValue:          req.NewValue,
		EvidenceMaterials: req.EvidenceMaterials,
		SubmittedBy:       req.SubmittedBy,
	}
	if err := s.orch.SubmitChange(r.Context(), c); err != nil {
		var be *errorsx.BusinessError
		if errors.As(err, &be) && be.Category == "validation" {
			writeError(w, http.StatusBadRequest, be.Category, be.Message)
			return
		}
		if errorsx.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "not_found", "enterprise not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, toChangeResponse(c))
}

func (s *Server) handleListChanges(w http.ResponseWriter, r *http.Request) {
	offset, limit := parsePagination(r)
	f := store.ChangeFilter{
		ListFilter:     store.ListFilter{Offset: offset, Limit: limit},
		EnterpriseID:   r.URL.Query().Get("enterprise_id"),
		DepartmentCode: r.URL.Query().Get("department_code"),
		Status:         r.URL.Query().Get("status"),
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
	items, total, err := s.orch.ListChanges(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	resps := make([]ChangeResponse, 0, len(items))
	for _, c := range items {
		resps = append(resps, toChangeResponse(c))
	}
	writeJSON(w, http.StatusOK, PaginatedResponse[ChangeResponse]{
		Items: resps, Total: total, Offset: offset, Limit: limit,
	})
}

func (s *Server) handleGetChange(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	c, err := s.orch.GetChange(r.Context(), id)
	if err != nil {
		if errorsx.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "not_found", "change not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toChangeResponse(c))
}

func (s *Server) handleDispatchChange(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req DispatchRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Operator == "" {
		req.Operator = "system"
	}
	if err := s.orch.DispatchChange(r.Context(), id, req.Operator); err != nil {
		var be *errorsx.BusinessError
		if errors.As(err, &be) {
			if be.Category == "invalid_transition" {
				writeError(w, http.StatusConflict, be.Category, be.Message)
				return
			}
			if be.Category == "not_found" {
				writeError(w, http.StatusNotFound, be.Category, be.Message)
				return
			}
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "dispatched", "change_id": id})
}

func (s *Server) handleRevokeChange(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req RevokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "parse_error", "invalid request body")
		return
	}
	if req.Operator == "" {
		req.Operator = "system"
	}
	if err := s.orch.RevokeChange(r.Context(), id, req.Operator, req.Reason); err != nil {
		var be *errorsx.BusinessError
		if errors.As(err, &be) {
			if be.Category == "invalid_transition" {
				writeError(w, http.StatusConflict, be.Category, be.Message)
				return
			}
			if be.Category == "not_found" {
				writeError(w, http.StatusNotFound, be.Category, be.Message)
				return
			}
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked", "change_id": id})
}

func (s *Server) handleCompensateChange(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req CompensateRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Operator == "" {
		req.Operator = "system"
	}
	if err := s.orch.CompensateChange(r.Context(), id, req.Operator); err != nil {
		var be *errorsx.BusinessError
		if errors.As(err, &be) {
			if be.Category == "invalid_transition" {
				writeError(w, http.StatusConflict, be.Category, be.Message)
				return
			}
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "compensated", "change_id": id})
}

func (s *Server) handleResolveOrder(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.orch.ResolveOrder(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "resolved", "enterprise_id": id})
}

func parseOffsetParam(r *http.Request) int {
	v := r.URL.Query().Get("offset")
	n, _ := strconv.Atoi(v)
	return n
}
