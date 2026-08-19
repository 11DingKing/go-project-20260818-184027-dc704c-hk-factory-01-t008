package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"regdispatch/internal/config"
	"regdispatch/internal/orchestrator"
	"regdispatch/internal/scheduler"
	"regdispatch/internal/store"
	"regdispatch/internal/transport/middleware"
	"regdispatch/internal/upstream"
)

// Server is the HTTP entry point for the registration dispatch service.
// It wires routes to the orchestrator and provides health, readiness, and
// management endpoints.
type Server struct {
	cfg      *config.Config
	orch     *orchestrator.Orchestrator
	store    *store.Store
	sched    *scheduler.Scheduler
	upstream *upstream.Client
	selector *upstream.Selector
	log      zerolog.Logger
	httpSrv  *http.Server
}

// New creates an HTTP server wired to the orchestrator, scheduler, and store.
func New(cfg *config.Config, orch *orchestrator.Orchestrator, st *store.Store, sched *scheduler.Scheduler, sel *upstream.Selector, up *upstream.Client, log zerolog.Logger) *Server {
	s := &Server{
		cfg:      cfg,
		orch:     orch,
		store:    st,
		sched:    sched,
		selector: sel,
		upstream: up,
		log:      log,
	}
	s.httpSrv = &http.Server{
		Addr:         formatAddr(cfg.Server.Port),
		Handler:      s.buildRouter(),
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}
	return s
}

func formatAddr(port int) string {
	return ":" + strconv.Itoa(port)
}

func (s *Server) buildRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.CORS)
	r.Use(middleware.ContentType)
	r.Use(middleware.Recoverer(s.log))
	r.Use(middleware.Logger(s.log))

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/enterprises", s.handleCreateEnterprise)
		r.Get("/enterprises", s.handleListEnterprises)
		r.Get("/enterprises/{id}", s.handleGetEnterprise)

		r.Post("/changes", s.handleSubmitChange)
		r.Get("/changes", s.handleListChanges)
		r.Get("/changes/{id}", s.handleGetChange)
		r.Post("/changes/{id}/dispatch", s.handleDispatchChange)
		r.Post("/changes/{id}/revoke", s.handleRevokeChange)
		r.Post("/changes/{id}/compensate", s.handleCompensateChange)
		r.Post("/changes/{id}/resolve", s.handleResolveOrder)

		r.Get("/departments/{code}/dispatches", s.handleListDepartmentDispatches)
		r.Get("/departments/{code}/subscribe", s.handleSubscribe)

		r.Post("/dispatch/{id}/ack", s.handleAckDispatch)
		r.Post("/dispatch/{id}/complete", s.handleCompleteDispatch)
		r.Post("/dispatch/{id}/fail", s.handleFailDispatch)

		r.Get("/audit", s.handleListAudit)
		r.Get("/dead-letters", s.handleListDeadLetters)
		r.Post("/dead-letters/{id}/redeliver", s.handleRedeliverDeadLetter)

		r.Get("/export/reconciliation", s.handleExportReconciliation)
		r.Get("/backlog", s.handleViewBacklog)
		r.Get("/admin/upstreams", s.handleListUpstreams)
	})

	return r
}

// Start begins listening for HTTP requests.
func (s *Server) Start() error {
	s.log.Info().Int("port", s.cfg.Server.Port).Msg("http server starting")
	return s.httpSrv.ListenAndServe()
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}

// ShutdownTimeout returns the configured shutdown grace period.
func (s *Server) ShutdownTimeout() time.Duration {
	return s.cfg.Server.ShutdownTimeout
}

// handleHealthz returns 200 as long as the process is alive.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// handleReadyz returns 200 only if all required dependencies are healthy:
// database ping, data directory writable, scheduler running, and schema
// migrated. Otherwise returns 503 with the failing component.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	checks := map[string]string{}
	ok := true

	if err := s.store.PingContext(ctx); err != nil {
		checks["database"] = "unreachable: " + err.Error()
		ok = false
	} else {
		checks["database"] = "ok"
	}

	if !s.sched.IsRunning() {
		checks["scheduler"] = "not running"
		ok = false
	} else {
		checks["scheduler"] = "ok"
	}

	version, err := s.store.SchemaVersion(ctx)
	if err != nil || version == 0 {
		checks["migrations"] = "not applied"
		ok = false
	} else {
		checks["migrations"] = "ok"
	}

	if ok {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "checks": checks})
	} else {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "checks": checks})
	}
}

// ErrServerClosed is returned when the server is shut down.
var ErrServerClosed = errors.New("server closed")
