package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"regdispatch/internal/domain/enterprise"
	"regdispatch/internal/errorsx"
)

func (s *Server) handleCreateEnterprise(w http.ResponseWriter, r *http.Request) {
	var req EnterpriseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "parse_error", "invalid request body")
		return
	}
	ent := &enterprise.Enterprise{
		Name:                req.Name,
		LegalRepresentative: req.LegalRepresentative,
		UnifiedCreditCode:   req.UnifiedCreditCode,
		RegisteredCapital:   req.RegisteredCapital,
		BusinessScope:       req.BusinessScope,
		IndustryCode:        req.IndustryCode,
	}
	if err := s.orch.RegisterEnterprise(r.Context(), ent); err != nil {
		var be *errorsx.BusinessError
		if errors.As(err, &be) && be.Category == "validation" {
			writeError(w, http.StatusBadRequest, be.Category, be.Message)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, toEnterpriseResponse(ent))
}

func (s *Server) handleListEnterprises(w http.ResponseWriter, r *http.Request) {
	offset, limit := parsePagination(r)
	items, total, err := s.orch.ListEnterprises(r.Context(), offset, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	resps := make([]EnterpriseResponse, 0, len(items))
	for _, e := range items {
		resps = append(resps, toEnterpriseResponse(e))
	}
	writeJSON(w, http.StatusOK, PaginatedResponse[EnterpriseResponse]{
		Items: resps, Total: total, Offset: offset, Limit: limit,
	})
}

func (s *Server) handleGetEnterprise(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ent, err := s.orch.GetEnterprise(r.Context(), id)
	if err != nil {
		if errorsx.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "not_found", "enterprise not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toEnterpriseResponse(ent))
}
