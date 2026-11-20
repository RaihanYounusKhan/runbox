package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"runbox/internal/audit"
	"runbox/internal/sandbox"
	"runbox/internal/store"
)

// ---- wire types ----

type submitRequest struct {
	Language string         `json:"language"`
	Source   string         `json:"source"`
	Stdin    string         `json:"stdin"`
	Limits   sandbox.Limits `json:"limits"`
}

type jobResult struct {
	ExitCode        *int   `json:"exit_code"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	StdoutTruncated bool   `json:"stdout_truncated"`
	StderrTruncated bool   `json:"stderr_truncated"`
	DurationMs      *int64 `json:"duration_ms"`
}

type jobResponse struct {
	ID              string         `json:"id"`
	Language        string         `json:"language"`
	Status          sandbox.Status `json:"status"`
	Attempts        int            `json:"attempts"`
	CancelRequested bool           `json:"cancel_requested,omitempty"`
	Limits          sandbox.Limits `json:"limits"`
	SourceSHA256    string         `json:"source_sha256"`
	CreatedAt       time.Time      `json:"created_at"`
	StartedAt       *time.Time     `json:"started_at,omitempty"`
	FinishedAt      *time.Time     `json:"finished_at,omitempty"`
	QueueWaitMs     *int64         `json:"queue_wait_ms,omitempty"`
	Result          *jobResult     `json:"result,omitempty"`
	Error           *string        `json:"error,omitempty"`
	Source          string         `json:"source,omitempty"`
}

func toResponse(j *store.Job, includeSource bool) jobResponse {
	resp := jobResponse{
		ID: j.ID, Language: j.Language, Status: j.Status, Attempts: j.Attempts,
		CancelRequested: j.CancelRequested && j.Status == sandbox.StatusRunning,
		Limits:          j.Limits, SourceSHA256: j.SourceSHA256,
		CreatedAt: j.CreatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt, Error: j.Error,
	}
	if j.StartedAt != nil {
		ms := j.StartedAt.Sub(j.CreatedAt).Milliseconds()
		resp.QueueWaitMs = &ms
	}
	if j.Status.Terminal() && j.Status != sandbox.StatusCanceled || j.ExitCode != nil {
		resp.Result = &jobResult{
			ExitCode: j.ExitCode,
			// Program output may be arbitrary bytes; JSON strings must be UTF-8.
			Stdout:          strings.ToValidUTF8(string(j.Stdout), "�"),
			Stderr:          strings.ToValidUTF8(string(j.Stderr), "�"),
			StdoutTruncated: j.StdoutTruncated, StderrTruncated: j.StderrTruncated,
			DurationMs: j.DurationMs,
		}
	}
	if includeSource {
		resp.Source = j.Source
	}
	return resp
}

// ---- job endpoints ----

func (s *Server) listLanguages(w http.ResponseWriter, _ *http.Request) {
	type lang struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	var out []lang
	for _, l := range sandbox.SortedLanguages(s.langs) {
		out = append(out, lang{l.Name, l.Version})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"languages":      out,
		"default_limits": sandbox.DefaultLimits,
		"max_limits":     sandbox.MaxLimits,
	})
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	key := keyFrom(r.Context())

	// Bound the body before decoding: source + stdin + JSON overhead.
	r.Body = http.MaxBytesReader(w, r.Body, int64(s.cfg.MaxSourceBytes+s.cfg.MaxStdinBytes)*2+64<<10)
	var req submitRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "too_large", "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}

	if _, ok := s.langs[req.Language]; !ok {
		writeError(w, http.StatusBadRequest, "unknown_language", fmt.Sprintf("unsupported language %q; see GET /v1/languages", req.Language))
		return
	}
	switch {
	case req.Source == "":
		writeError(w, http.StatusBadRequest, "bad_request", "source is required")
		return
	case len(req.Source) > s.cfg.MaxSourceBytes:
		writeError(w, http.StatusRequestEntityTooLarge, "too_large", fmt.Sprintf("source exceeds %d bytes", s.cfg.MaxSourceBytes))
		return
	case len(req.Stdin) > s.cfg.MaxStdinBytes:
		writeError(w, http.StatusRequestEntityTooLarge, "too_large", fmt.Sprintf("stdin exceeds %d bytes", s.cfg.MaxStdinBytes))
		return
	case !utf8.ValidString(req.Source) || strings.ContainsRune(req.Source, 0):
		writeError(w, http.StatusBadRequest, "bad_request", "source must be valid UTF-8 without NUL bytes")
		return
	}
	limits := req.Limits.WithDefaults()
	if err := limits.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_limits", err.Error())
		return
	}

	// Backpressure: refuse work we can't get to soon rather than letting
	// the queue (and everyone's latency) grow without bound.
	total, forKey, err := s.store.QueueDepth(r.Context(), key.ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if total >= s.cfg.MaxQueueDepth {
		s.m.rejected.Inc("queue_full")
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "queue_full", "the job queue is full; retry later")
		return
	}
	if forKey >= s.cfg.MaxQueuedPerKey {
		s.m.rejected.Inc("key_queue_full")
		w.Header().Set("Retry-After", "2")
		writeError(w, http.StatusTooManyRequests, "too_many_queued", fmt.Sprintf("this key already has %d queued jobs", forKey))
		return
	}

	job, err := s.store.CreateJob(r.Context(), store.NewJob{
		APIKeyID:    key.ID,
		Language:    req.Language,
		Source:      req.Source,
		Stdin:       []byte(req.Stdin),
		Limits:      limits,
		MaxAttempts: s.cfg.MaxAttempts,
		ClientIP:    s.clientIP(r),
	})
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.m.submitted.Inc(req.Language)
	w.Header().Set("Location", "/v1/jobs/"+job.ID)
	writeJSON(w, http.StatusAccepted, toResponse(job, false))
}

// getJob returns a job. With ?wait=<duration> (max 30s) it long-polls until
// the job reaches a terminal state, which saves clients a polling loop.
func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	key := keyFrom(r.Context())
	id := r.PathValue("id")

	var wait time.Duration
	if v := r.URL.Query().Get("wait"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			writeError(w, http.StatusBadRequest, "bad_request", "wait must be a duration like 10s")
			return
		}
		wait = min(d, 30*time.Second)
	}
	includeSource := r.URL.Query().Get("include") == "source"

	// Subscribe before the first read so a completion that lands between
	// the read and the wait isn't missed.
	var doneCh <-chan struct{}
	if wait > 0 {
		ch, unsubscribe := s.done.subscribe(id)
		defer unsubscribe()
		doneCh = ch
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	for {
		job, err := s.store.GetJob(r.Context(), id, key.ID)
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "job not found")
			return
		}
		if err != nil {
			if r.Context().Err() != nil {
				return
			}
			s.internalError(w, r, err)
			return
		}
		if job.Status.Terminal() || wait == 0 {
			writeJSON(w, http.StatusOK, toResponse(job, includeSource))
			return
		}
		// Woken by NOTIFY normally; the periodic re-check is a safety net
		// for a lost notification, and tighter while the listener is down.
		recheck := 2 * time.Second
		if !s.done.isHealthy() {
			recheck = 250 * time.Millisecond
		}
		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			wait = 0 // final read, then respond with whatever state it's in
		case <-doneCh:
			doneCh = nil
		case <-time.After(recheck):
		}
	}
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	key := keyFrom(r.Context())
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := s.store.ListJobs(r.Context(), key.ID, r.URL.Query().Get("status"), limit)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]jobResponse, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toResponse(j, false))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	key := keyFrom(r.Context())
	job, err := s.store.RequestCancel(r.Context(), r.PathValue("id"), key.ID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "job not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "not_cancelable", fmt.Sprintf("job already %s", job.Status))
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusAccepted, toResponse(job, false))
	}
}

// ---- admin endpoints ----

func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name          string   `json:"name"`
		MaxConcurrent int      `json:"max_concurrent"`
		RateRPS       *float64 `json:"rate_rps"`
		RateBurst     *int     `json:"rate_burst"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	if strings.TrimSpace(req.Name) == "" || len(req.Name) > 100 {
		writeError(w, http.StatusBadRequest, "bad_request", "name is required (max 100 chars)")
		return
	}
	if req.MaxConcurrent < 0 || req.MaxConcurrent > 1000 {
		writeError(w, http.StatusBadRequest, "bad_request", "max_concurrent must be between 1 and 1000")
		return
	}
	k, plain, err := s.store.CreateKey(r.Context(), store.NewKeyParams{
		Name: req.Name, MaxConcurrent: req.MaxConcurrent, RateRPS: req.RateRPS, RateBurst: req.RateBurst,
	}, "admin@"+s.clientIP(r))
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"key": k, "api_key": plain,
		"note": "store api_key now; it cannot be retrieved again"})
}

func (s *Server) listKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.store.ListKeys(r.Context())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if keys == nil {
		keys = []store.Key{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func (s *Server) revokeKey(w http.ResponseWriter, r *http.Request) {
	k, err := s.store.RevokeKey(r.Context(), r.PathValue("id"), "admin@"+s.clientIP(r))
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "key not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "already_revoked", "key already revoked")
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, map[string]any{"key": k})
	}
}

func (s *Server) listAudit(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	jobID := q.Get("job_id")
	entries, err := audit.List(r.Context(), s.store.Pool, audit.ListFilter{AfterSeq: after, JobID: jobID, Limit: limit})
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if entries == nil {
		entries = []audit.Entry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *Server) verifyAudit(w http.ResponseWriter, r *http.Request) {
	rep, err := audit.Verify(r.Context(), s.store.Pool)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	code := http.StatusOK
	if !rep.OK {
		code = http.StatusConflict
		s.log.Error("AUDIT CHAIN VERIFICATION FAILED", "first_bad_seq", rep.FirstBadSeq, "problem", rep.Problem)
	}
	writeJSON(w, code, rep)
}

func (s *Server) adminGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.GetJob(r.Context(), r.PathValue("id"), "")
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": toResponse(job, true), "api_key_id": job.APIKeyID})
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]string{"code": errCode, "message": msg}})
}
