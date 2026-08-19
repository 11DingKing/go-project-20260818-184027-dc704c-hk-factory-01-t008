package httpapi

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"regdispatch/internal/errorsx"
	"regdispatch/internal/store"
)

func (s *Server) handleListDepartmentDispatches(w http.ResponseWriter, r *http.Request) {
	code := chi.URLParam(r, "code")
	offset, limit := parsePagination(r)
	f := store.DispatchFilter{
		ListFilter:     store.ListFilter{Offset: offset, Limit: limit},
		DepartmentCode: code,
	}
	if status := r.URL.Query().Get("status"); status != "" {
		f.Status = status
	}
	if changeID := r.URL.Query().Get("change_id"); changeID != "" {
		f.ChangeID = changeID
	}
	items, total, err := s.orch.ListDispatchesByDepartment(r.Context(), code, f)
	if err != nil {
		var be *errorsx.BusinessError
		if errors.As(err, &be) && be.Category == "validation" {
			writeError(w, http.StatusBadRequest, be.Category, be.Message)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	resps := make([]DispatchTaskResponse, 0, len(items))
	for _, t := range items {
		resps = append(resps, toDispatchTaskResponse(t))
	}
	writeJSON(w, http.StatusOK, PaginatedResponse[DispatchTaskResponse]{
		Items: resps, Total: total, Offset: offset, Limit: limit,
	})
}
