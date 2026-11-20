// Command runbox-worker claims jobs and executes them in sandboxed containers.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"runbox/internal/config"
	"runbox/internal/db"
	"runbox/internal/metrics"
	"runbox/internal/sandbox"
	"runbox/internal/store"
	"runbox/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "worker")
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return err
	}
	log = log.With("worker_id", cfg.WorkerID)

	langs, err := sandbox.LoadLanguages(cfg.LanguagesFile)
	if err != nil {
		return err
	}
	var seccomp []byte
	if cfg.SeccompProfile != "" {
		if seccomp, err = os.ReadFile(cfg.SeccompProfile); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(cfg.JobsDir, 0o755); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	docker := sandbox.NewDockerClient(cfg.DockerSocket)
	if err := docker.Negotiate(ctx); err != nil {
		return err
	}
	runner := sandbox.NewDockerRunner(docker, sandbox.DockerRunnerConfig{
		WorkerID:       cfg.WorkerID,
		JobsDir:        cfg.JobsDir,
		HostJobsDir:    cfg.HostJobsDir,
		Runtime:        cfg.Runtime,
		SeccompProfile: seccomp,
	})

	if n, err := runner.CleanupOrphans(ctx); err != nil {
		log.Warn("orphan cleanup failed", "err", err)
	} else if n > 0 {
		log.Warn("removed orphaned containers from a previous run", "count", n)
	}
	missing, err := runner.CheckImages(ctx, langs)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		log.Warn("sandbox images missing; jobs for these languages will fail until built (make images)", "images", missing)
	}

	reg := metrics.NewRegistry()
	m := worker.NewMetrics(reg)
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			reg.Handler().ServeHTTP(w, r)
		case "/healthz":
			w.Write([]byte("ok\n")) //nolint:errcheck
		default:
			http.NotFound(w, r)
		}
	}), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()
	defer metricsSrv.Close()

	w := worker.New(worker.Config{
		WorkerID:      cfg.WorkerID,
		Concurrency:   cfg.Concurrency,
		LeaseDuration: cfg.LeaseDuration,
		PollInterval:  cfg.PollInterval,
		ShutdownGrace: cfg.ShutdownGrace,
		ReapInterval:  cfg.ReapInterval,
	}, store.New(pool), runner, langs, m, log)

	log.Info("worker started", "concurrency", cfg.Concurrency, "runtime", orDefault(cfg.Runtime, "runc"),
		"custom_seccomp", len(seccomp) > 0)
	err = w.Run(ctx)
	runner.Close()
	return err
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}
