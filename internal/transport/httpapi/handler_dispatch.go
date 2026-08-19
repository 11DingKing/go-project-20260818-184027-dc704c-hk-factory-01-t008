package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"regdispatch/internal/errorsx"
)

func (s *Server) handleAckDispatch(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "parse_error", "invalid request body")
		return
	}
	if req.AckedBy == "" {
		writeError(w, http.StatusBadRequest, "validation", "acked_by is required")
		return
	}
	if err := s.orch.AckDispatch(r.Context(), id, req.AckedBy); err != nil {
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
	writeJSON(w, http.StatusOK, map[string]string{"status": "acked", "task_id": id})
}

func (s *Server) handleCompleteDispatch(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req CompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "parse_error", "invalid request body")
		return
	}
	if req.Operator == "" {
		req.Operator = "system"
	}
	if err := s.orch.CompleteDispatch(r.Context(), id, req.Operator, req.Result); err != nil {
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
	writeJSON(w, http.StatusOK, map[string]string{"status": "completed", "task_id": id})
}

func (s *Server) handleFailDispatch(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req FailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "parse_error", "invalid request body")
		return
	}
	if req.Operator == "" {
		req.Operator = "system"
	}
	if err := s.orch.FailDispatch(r.Context(), id, req.Operator, req.Error); err != nil {
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
	writeJSON(w, http.StatusOK, map[string]string{"status": "failed", "task_id": id})
}
