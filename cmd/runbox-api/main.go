// Command runbox-api serves the public HTTP API.
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

	"runbox/internal/api"
	"runbox/internal/config"
	"runbox/internal/db"
	"runbox/internal/metrics"
	"runbox/internal/sandbox"
	"runbox/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "api")
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadAPI()
	if err != nil {
		return err
	}
	langs, err := sandbox.LoadLanguages(cfg.LanguagesFile)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if cfg.AutoMigrate {
		applied, err := db.Migrate(ctx, pool)
		if err != nil {
			return err
		}
		if len(applied) > 0 {
			log.Info("applied migrations", "versions", applied)
		}
	}
	if cfg.AdminToken == "" {
		log.Warn("ADMIN_TOKEN not set: admin API disabled; create keys with runbox-admin")
	}

	reg := metrics.NewRegistry()
	srv := api.New(cfg, store.New(pool), langs, reg, log)
	go srv.Background(ctx)

	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      45 * time.Second, // > max long-poll wait
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.ListenAddr, "languages", len(langs))
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutCtx); err != nil {
			return err
		}
	}
	return nil
}
