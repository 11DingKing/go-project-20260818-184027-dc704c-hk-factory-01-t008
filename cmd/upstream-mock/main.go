package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

// DispatchRequest mirrors the payload sent by the gateway.
type DispatchRequest struct {
	ChangeID       string `json:"change_id"`
	EnterpriseID   string `json:"enterprise_id"`
	ChangeType     string `json:"change_type"`
	DepartmentCode string `json:"department_code"`
	BeforeValue    string `json:"before_value"`
	AfterValue     string `json:"after_value"`
	Attempt        int    `json:"attempt"`
}

// DispatchResponse mirrors the reply expected by the gateway.
type DispatchResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

func main() {
	port := flag.Int("port", 48444, "listen port")
	failDepts := flag.String("fail-departments", "", "comma-separated department codes that always fail")
	delayMs := flag.Int("delay-ms", 0, "artificial processing delay in milliseconds")
	flag.Parse()

	log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	failSet := make(map[string]bool)
	for _, d := range strings.Split(*failDepts, ",") {
		d = strings.TrimSpace(d)
		if d != "" {
			failSet[d] = true
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/departments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/api/departments/")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) < 1 || parts[0] == "" {
			http.Error(w, "department code required", http.StatusBadRequest)
			return
		}
		deptCode := parts[0]

		var req DispatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		if *delayMs > 0 {
			time.Sleep(time.Duration(*delayMs) * time.Millisecond)
		}

		resp := DispatchResponse{Success: true, Detail: fmt.Sprintf("processed by %s", deptCode)}
		if failSet[deptCode] {
			resp = DispatchResponse{Success: false, Error: fmt.Sprintf("department %s rejected change", deptCode)}
		}

		log.Info().
			Str("department", deptCode).
			Str("change_id", req.ChangeID).
			Bool("success", resp.Success).
			Msg("processed dispatch")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"alive"}`)
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	go func() {
		log.Info().Int("port", *port).Msg("upstream mock service starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("upstream mock failed")
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info().Msg("upstream mock shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
