package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"regdispatch/internal/clock"
	"regdispatch/internal/config"
	"regdispatch/internal/orchestrator"
	"regdispatch/internal/scheduler"
	"regdispatch/internal/store"
	"regdispatch/internal/transport/httpapi"
	"regdispatch/internal/upstream"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	log := setupLogger()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	log.Info().
		Int("port", cfg.Server.Port).
		Str("data_dir", cfg.Storage.DataDir).
		Msg("starting registration dispatch gateway")

	ctx := context.Background()

	st, err := store.Open(ctx, cfg.Storage.DBPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open store")
	}
	defer st.Close()

	version, err := st.EnsureSchema(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to apply migrations")
	}
	log.Info().Int("schema_version", version).Msg("schema migrated")

	repos := st.AllRepositories()

	clk := clock.RealClock{}
	selector := buildSelector(cfg)
	upClient := upstream.NewClient(selector, repos.Traces, clk, cfg.Upstream.Timeout)

	orch := orchestrator.New(repos, upClient, clk, log,
		cfg.Dispatch.MaxRetries, cfg.Dispatch.RetryBaseDelay, cfg.Dispatch.RetryMaxDelay)

	sched := scheduler.New(clk, log, st)
	scheduler.RegisterDefaultTasks(sched, orch, repos, clk, log,
		cfg.Compaction.Interval, cfg.Compaction.RetainEvents)

	if err := sched.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("failed to start scheduler")
	}

	server := httpapi.New(cfg, orch, st, sched, selector, upClient, log)

	go func() {
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("http server failed")
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info().Str("signal", sig.String()).Msg("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	sched.Stop(cfg.Server.ShutdownTimeout)
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("http server shutdown error")
	}
	log.Info().Msg("shutdown complete")
}

func buildSelector(cfg *config.Config) *upstream.Selector {
	breakerCfg := upstream.BreakerConfig{
		Threshold:   cfg.Upstream.BreakerThreshold,
		Timeout:     cfg.Upstream.BreakerTimeout,
		HalfOpenMax: cfg.Upstream.BreakerHalfOpenMax,
	}
	primary := upstream.NewUpstream("primary", cfg.Upstream.MockURL, breakerCfg)
	return upstream.NewSelector([]upstream.Upstream{primary})
}

func setupLogger() zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339
	return zerolog.New(os.Stdout).With().Timestamp().Logger()
}
