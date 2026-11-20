// Package api implements the HTTP API.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"runbox/internal/auth"
	"runbox/internal/config"
	"runbox/internal/metrics"
	"runbox/internal/ratelimit"
	"runbox/internal/sandbox"
	"runbox/internal/store"
)

// Server holds API dependencies.
type Server struct {
	cfg     config.API
	store   *store.Store
	langs   map[string]sandbox.Language
	limiter *ratelimit.Limiter
	log     *slog.Logger
	reg     *metrics.Registry
	m       apiMetrics
	done    *completionHub
}

type apiMetrics struct {
	requests  *metrics.Vec
	latency   *metrics.Histogram
	submitted *metrics.Vec
	rejected  *metrics.Vec
	queued    *metrics.Vec
	running   *metrics.Vec
}

// New builds a Server and registers its metrics on reg.
func New(cfg config.API, st *store.Store, langs map[string]sandbox.Language, reg *metrics.Registry, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, store: st, langs: langs, limiter: ratelimit.New(), log: log, reg: reg, done: newCompletionHub(log)}
	s.m = apiMetrics{
		requests:  reg.NewCounter("runbox_http_requests_total", "HTTP requests by route and status code.", "route", "code"),
		latency:   reg.NewHistogram("runbox_http_request_duration_seconds", "HTTP request latency.", nil, "route"),
		submitted: reg.NewCounter("runbox_jobs_submitted_total", "Jobs accepted.", "language"),
		rejected:  reg.NewCounter("runbox_jobs_rejected_total", "Submissions rejected by backpressure or rate limiting.", "reason"),
		queued:    reg.NewGauge("runbox_queue_depth", "Jobs waiting to run."),
		running:   reg.NewGauge("runbox_jobs_running", "Jobs currently executing."),
	}
	return s
}

// Background runs the completion listener and periodic maintenance (queue
// gauges, limiter sweeping) until ctx is canceled.
func (s *Server) Background(ctx context.Context) {
	go s.done.run(ctx, s.store.Pool)
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		s.refreshGauges(ctx)
		s.limiter.Sweep(10 * time.Minute)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Server) refreshGauges(ctx context.Context) {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	q, r, err := s.store.StatusCounts(c)
	if err != nil {
		return
	}
	s.m.queued.Set(float64(q))
	s.m.running.Set(float64(r))
}

// Handler returns the root HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) }) //nolint:errcheck
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", s.reg.Handler())

	mux.Handle("GET /v1/languages", s.authed(s.listLanguages))
	mux.Handle("POST /v1/jobs", s.authed(s.submitJob))
	mux.Handle("GET /v1/jobs", s.authed(s.listJobs))
	mux.Handle("GET /v1/jobs/{id}", s.authed(s.getJob))
	mux.Handle("POST /v1/jobs/{id}/cancel", s.authed(s.cancelJob))

	mux.Handle("POST /v1/admin/keys", s.admin(s.createKey))
	mux.Handle("GET /v1/admin/keys", s.admin(s.listKeys))
	mux.Handle("POST /v1/admin/keys/{id}/revoke", s.admin(s.revokeKey))
	mux.Handle("GET /v1/admin/audit", s.admin(s.listAudit))
	mux.Handle("GET /v1/admin/audit/verify", s.admin(s.verifyAudit))
	mux.Handle("GET /v1/admin/jobs/{id}", s.admin(s.adminGetJob))

	return s.recoverer(s.instrument(mux))
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Pool.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "database unreachable")
		return
	}
	w.Write([]byte("ready\n")) //nolint:errcheck
}

// ---- middleware ----

type ctxKey int

const keyCtxKey ctxKey = 1

func keyFrom(ctx context.Context) *store.Key {
	k, _ := ctx.Value(keyCtxKey).(*store.Key)
	return k
}

// authed authenticates the API key and applies its rate limit.
func (s *Server) authed(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, ok := auth.BearerToken(r.Header.Get("Authorization"))
		if !ok || !strings.HasPrefix(tok, auth.Prefix) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing or malformed API key")
			return
		}
		key, err := s.store.KeyByPlaintext(r.Context(), tok)
		if errors.Is(err, store.ErrNotFound) {
			s.log.Warn("auth failed", "client_ip", s.clientIP(r), "prefix", auth.DisplayPrefix(tok))
			writeError(w, http.StatusUnauthorized, "unauthorized", "invalid or revoked API key")
			return
		}
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		rps, burst := s.cfg.RateLimitRPS, s.cfg.RateLimitBurst
		if key.RateRPS != nil {
			rps = *key.RateRPS
		}
		if key.RateBurst != nil {
			burst = *key.RateBurst
		}
		if allowed, wait := s.limiter.Allow(key.ID, rps, burst); !allowed {
			s.m.rejected.Inc("rate_limited")
			w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
			writeError(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded")
			return
		}
		h(w, r.WithContext(context.WithValue(r.Context(), keyCtxKey, key)))
	})
}

// admin guards operator endpoints with ADMIN_TOKEN.
func (s *Server) admin(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.AdminToken == "" {
			writeError(w, http.StatusNotFound, "not_found", "admin API disabled (ADMIN_TOKEN not set)")
			return
		}
		tok, ok := auth.BearerToken(r.Header.Get("Authorization"))
		if !ok || !auth.ConstantTimeEqual(tok, s.cfg.AdminToken) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "invalid admin token")
			return
		}
		h(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// instrument adds a request ID, structured access logging and metrics.
func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" || len(reqID) > 64 {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			reqID = hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-Id", reqID)
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := r.Pattern // set by ServeMux to the matched pattern (low cardinality)
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		s.m.requests.Inc(route, strconv.Itoa(rec.code))
		s.m.latency.Observe(elapsed.Seconds(), route)
		if route != "GET /metrics" && route != "GET /healthz" {
			s.log.Info("http", "req_id", reqID, "method", r.Method, "path", r.URL.Path,
				"status", rec.code, "duration_ms", elapsed.Milliseconds(), "client_ip", s.clientIP(r))
		}
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.log.Error("panic", "value", v, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) clientIP(r *http.Request) string {
	if s.cfg.TrustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			return strings.TrimSpace(strings.Split(xff, ",")[0])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.Error("internal error", "err", err, "path", r.URL.Path)
	writeError(w, http.StatusInternalServerError, "internal", "internal server error")
}
