// Package config loads runtime configuration from environment variables.
//
// Every setting has a sensible default so `docker compose up` works out of the
// box; production deployments override them via the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// API holds configuration for the runbox-api process.
type API struct {
	ListenAddr      string
	DatabaseURL     string
	AdminToken      string // empty disables /v1/admin/*
	AutoMigrate     bool
	LanguagesFile   string
	MaxQueueDepth   int // global queued-job ceiling; beyond it submissions get 503
	MaxQueuedPerKey int // per-key queued-job ceiling; beyond it submissions get 429
	RateLimitRPS    float64
	RateLimitBurst  int
	MaxSourceBytes  int
	MaxStdinBytes   int
	MaxAttempts     int // how many times a job may be claimed before it is marked lost
	TrustProxy      bool
}

// Worker holds configuration for the runbox-worker process.
type Worker struct {
	DatabaseURL    string
	WorkerID       string
	Concurrency    int
	LeaseDuration  time.Duration
	PollInterval   time.Duration
	ShutdownGrace  time.Duration
	ReapInterval   time.Duration
	DockerSocket   string
	JobsDir        string // where the worker writes per-job files
	HostJobsDir    string // the same directory as the Docker daemon sees it
	Runtime        string // "" (runc) or e.g. "runsc" for gVisor
	SeccompProfile string // optional path to a custom seccomp JSON profile
	LanguagesFile  string
	MetricsAddr    string
}

// LoadAPI reads API configuration from the environment.
func LoadAPI() (API, error) {
	c := API{
		ListenAddr:      env("LISTEN_ADDR", ":8080"),
		DatabaseURL:     env("DATABASE_URL", "postgres://runbox:runbox@localhost:5432/runbox?sslmode=disable"),
		AdminToken:      env("ADMIN_TOKEN", ""),
		AutoMigrate:     envBool("AUTO_MIGRATE", true),
		LanguagesFile:   env("LANGUAGES_FILE", ""),
		MaxQueueDepth:   envInt("MAX_QUEUE_DEPTH", 10000),
		MaxQueuedPerKey: envInt("MAX_QUEUED_PER_KEY", 500),
		RateLimitRPS:    envFloat("RATE_LIMIT_RPS", 20),
		RateLimitBurst:  envInt("RATE_LIMIT_BURST", 40),
		MaxSourceBytes:  envInt("MAX_SOURCE_BYTES", 256<<10),
		MaxStdinBytes:   envInt("MAX_STDIN_BYTES", 1<<20),
		MaxAttempts:     envInt("MAX_ATTEMPTS", 3),
		TrustProxy:      envBool("TRUST_PROXY", false),
	}
	if c.MaxAttempts < 1 {
		return c, fmt.Errorf("MAX_ATTEMPTS must be >= 1")
	}
	return c, nil
}

// LoadWorker reads worker configuration from the environment.
func LoadWorker() (Worker, error) {
	host, _ := os.Hostname()
	c := Worker{
		DatabaseURL:    env("DATABASE_URL", "postgres://runbox:runbox@localhost:5432/runbox?sslmode=disable"),
		WorkerID:       env("WORKER_ID", host),
		Concurrency:    envInt("WORKER_CONCURRENCY", 4),
		LeaseDuration:  envDuration("LEASE_DURATION", 30*time.Second),
		PollInterval:   envDuration("POLL_INTERVAL", 2*time.Second),
		ShutdownGrace:  envDuration("SHUTDOWN_GRACE", 30*time.Second),
		ReapInterval:   envDuration("REAP_INTERVAL", 10*time.Second),
		DockerSocket:   env("DOCKER_SOCKET", "/var/run/docker.sock"),
		JobsDir:        env("JOBS_DIR", "/var/lib/runbox/jobs"),
		Runtime:        env("CONTAINER_RUNTIME", ""),
		SeccompProfile: env("SECCOMP_PROFILE", ""),
		LanguagesFile:  env("LANGUAGES_FILE", ""),
		MetricsAddr:    env("METRICS_ADDR", ":9091"),
	}
	c.HostJobsDir = env("HOST_JOBS_DIR", c.JobsDir)
	if c.WorkerID == "" {
		return c, fmt.Errorf("WORKER_ID is empty and hostname is unavailable")
	}
	if c.Concurrency < 1 {
		return c, fmt.Errorf("WORKER_CONCURRENCY must be >= 1")
	}
	if c.LeaseDuration < 3*time.Second {
		return c, fmt.Errorf("LEASE_DURATION must be >= 3s")
	}
	return c, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
