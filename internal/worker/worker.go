// Package worker pulls jobs from the queue and runs them in the sandbox.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"runbox/internal/metrics"
	"runbox/internal/sandbox"
	"runbox/internal/store"
)

// Config tunes a Worker.
type Config struct {
	WorkerID      string
	Concurrency   int
	LeaseDuration time.Duration
	PollInterval  time.Duration
	ShutdownGrace time.Duration
	ReapInterval  time.Duration
}

// Worker runs Concurrency job loops plus a lease reaper and a LISTEN loop.
type Worker struct {
	cfg    Config
	store  *store.Store
	runner sandbox.Runner
	langs  map[string]sandbox.Language
	log    *slog.Logger
	m      *Metrics
	wake   chan struct{}
}

// Metrics exposed by a worker.
type Metrics struct {
	Completed  *metrics.Vec       // runbox_worker_jobs_completed_total{language,status}
	Duration   *metrics.Histogram // runbox_worker_job_duration_seconds{language}
	QueueWait  *metrics.Histogram // runbox_worker_queue_wait_seconds
	Busy       *metrics.Vec       // runbox_worker_busy_slots
	Retries    *metrics.Vec       // runbox_worker_infra_retries_total
	ClaimErrs  *metrics.Vec       // runbox_worker_claim_errors_total
	Reaped     *metrics.Vec       // runbox_worker_reaped_total{outcome}
	LeasesLost *metrics.Vec       // runbox_worker_leases_lost_total
}

// NewMetrics registers worker metrics on reg.
func NewMetrics(reg *metrics.Registry) *Metrics {
	return &Metrics{
		Completed:  reg.NewCounter("runbox_worker_jobs_completed_total", "Jobs finished by this worker.", "language", "status"),
		Duration:   reg.NewHistogram("runbox_worker_job_duration_seconds", "Wall-clock execution time inside the sandbox.", nil, "language"),
		QueueWait:  reg.NewHistogram("runbox_worker_queue_wait_seconds", "Time from submission to claim.", nil),
		Busy:       reg.NewGauge("runbox_worker_busy_slots", "Job slots currently executing."),
		Retries:    reg.NewCounter("runbox_worker_infra_retries_total", "Jobs requeued after an infrastructure error."),
		ClaimErrs:  reg.NewCounter("runbox_worker_claim_errors_total", "Errors while claiming jobs."),
		Reaped:     reg.NewCounter("runbox_worker_reaped_total", "Expired leases handled by the reaper.", "outcome"),
		LeasesLost: reg.NewCounter("runbox_worker_leases_lost_total", "Jobs abandoned because this worker lost the lease."),
	}
}

// New builds a Worker.
func New(cfg Config, st *store.Store, runner sandbox.Runner, langs map[string]sandbox.Language, m *Metrics, log *slog.Logger) *Worker {
	return &Worker{
		cfg: cfg, store: st, runner: runner, langs: langs, m: m, log: log,
		wake: make(chan struct{}, cfg.Concurrency),
	}
}

var (
	errCanceledByUser = errors.New("canceled by user")
	errLeaseLost      = errors.New("lease lost")
)

// Run blocks until ctx is canceled and all in-flight jobs have finished or
// been released. Shutdown is two-phase: when ctx is canceled the worker
// stops claiming new jobs; running jobs get ShutdownGrace to finish, after
// which they are killed and released back to the queue.
func (w *Worker) Run(ctx context.Context) error {
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()

	go func() {
		<-ctx.Done()
		w.log.Info("shutdown requested; draining", "grace", w.cfg.ShutdownGrace)
		t := time.NewTimer(w.cfg.ShutdownGrace)
		defer t.Stop()
		select {
		case <-t.C:
			w.log.Warn("grace period over; killing in-flight jobs")
			hardCancel()
		case <-hardCtx.Done():
		}
	}()

	go w.listen(ctx)
	go w.reapLoop(ctx)

	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			w.loop(ctx, hardCtx, slot)
		}(i)
	}
	wg.Wait()
	w.log.Info("all job slots stopped")
	return nil
}

// loop is one job slot.
func (w *Worker) loop(ctx, hardCtx context.Context, slot int) {
	log := w.log.With("slot", slot)
	errBackoff := 500 * time.Millisecond
	for ctx.Err() == nil {
		job, err := w.store.ClaimJob(ctx, w.cfg.WorkerID, w.cfg.LeaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.m.ClaimErrs.Inc()
			log.Error("claim failed", "err", err, "retry_in", errBackoff)
			sleep(ctx, errBackoff)
			errBackoff = min(errBackoff*2, 10*time.Second)
			continue
		}
		errBackoff = 500 * time.Millisecond
		if job == nil {
			// Idle: wait for a NOTIFY, or poll as a fallback (NOTIFYs can be
			// missed while the listener reconnects; jitter avoids herds).
			jitter := time.Duration(rand.Int64N(int64(w.cfg.PollInterval/2) + 1))
			select {
			case <-ctx.Done():
				return
			case <-w.wake:
			case <-time.After(w.cfg.PollInterval + jitter):
			}
			continue
		}
		w.execute(hardCtx, job, log.With("job_id", job.ID, "attempt", job.Attempts))
	}
}

// execute runs one claimed job to completion.
func (w *Worker) execute(hardCtx context.Context, job *store.Job, log *slog.Logger) {
	w.m.Busy.Inc()
	defer w.m.Busy.Dec()
	if job.StartedAt != nil {
		w.m.QueueWait.Observe(job.StartedAt.Sub(job.CreatedAt).Seconds())
	}

	// DB writes after the run use their own context so that a shutdown
	// doesn't prevent recording the outcome.
	dbCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 15*time.Second)
	}

	lang, ok := w.langs[job.Language]
	if !ok {
		// The API validates languages, so this means API and worker configs
		// disagree. Retrying on another worker might help.
		c, cancel := dbCtx()
		defer cancel()
		_, err := w.store.RetryOrFail(c, job.ID, w.cfg.WorkerID, "language "+job.Language+" not configured on worker "+w.cfg.WorkerID)
		if err != nil {
			log.Error("record failure", "err", err)
		}
		return
	}

	jobCtx, cancelJob := context.WithCancelCause(hardCtx)
	defer cancelJob(nil)

	// Heartbeat: renew the lease and pick up cancellation requests.
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(w.cfg.LeaseDuration / 3)
		defer t.Stop()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
				c, cancel := dbCtx()
				cancelReq, ok, err := w.store.Heartbeat(c, job.ID, w.cfg.WorkerID, w.cfg.LeaseDuration)
				cancel()
				switch {
				case err != nil:
					// Transient DB error: keep going; the lease is long
					// enough to survive a few missed beats.
					log.Warn("heartbeat failed", "err", err)
				case !ok:
					cancelJob(errLeaseLost)
					return
				case cancelReq:
					cancelJob(errCanceledByUser)
					return
				}
			}
		}
	}()

	res, runErr := w.runner.Run(jobCtx, sandbox.Spec{
		JobID:    job.ID,
		Language: lang,
		Source:   []byte(job.Source),
		Stdin:    job.Stdin,
		Limits:   job.Limits,
	})
	cause := context.Cause(jobCtx)
	cancelJob(nil)
	<-hbDone

	c, cancel := dbCtx()
	defer cancel()

	switch {
	case errors.Is(cause, errLeaseLost):
		// Someone else may already be running this job; writing a result
		// now would clobber theirs. The fenced UPDATE would reject it
		// anyway, but there's no point trying.
		w.m.LeasesLost.Inc()
		log.Warn("lease lost; abandoning job")
		return

	case hardCtx.Err() != nil && !errors.Is(cause, errCanceledByUser):
		// Shutdown grace expired mid-run: not the job's fault, give it back.
		if err := w.store.ReleaseJob(c, job.ID, w.cfg.WorkerID, "worker shutdown"); err != nil {
			log.Error("release job", "err", err)
		} else {
			log.Info("released job back to queue")
		}
		return

	case runErr != nil:
		w.m.Retries.Inc()
		requeued, err := w.store.RetryOrFail(c, job.ID, w.cfg.WorkerID, runErr.Error())
		if err != nil {
			log.Error("record infra failure", "err", err)
		}
		log.Error("sandbox infrastructure error", "err", runErr, "requeued", requeued)
		if !requeued {
			w.m.Completed.Inc(job.Language, string(sandbox.StatusError))
		}
		return
	}

	if errors.Is(cause, errCanceledByUser) {
		res.Status = sandbox.StatusCanceled
	}
	ok, err := w.store.CompleteJob(c, job.ID, w.cfg.WorkerID, res, "")
	if err != nil {
		log.Error("record result", "err", err)
		return
	}
	if !ok {
		w.m.LeasesLost.Inc()
		log.Warn("result discarded: lease lost before completion")
		return
	}
	w.m.Completed.Inc(job.Language, string(res.Status))
	w.m.Duration.Observe(res.Duration.Seconds(), job.Language)
	log.Info("job finished", "status", res.Status, "exit_code", deref(res.ExitCode), "duration_ms", res.Duration.Milliseconds())
}

// listen wakes idle slots when the API NOTIFYs about a new job.
func (w *Worker) listen(ctx context.Context) {
	pool := w.store.Pool
	for ctx.Err() == nil {
		if err := w.listenOnce(ctx, pool); err != nil && ctx.Err() == nil {
			w.log.Warn("LISTEN connection lost; falling back to polling until reconnected", "err", err)
			sleep(ctx, 2*time.Second)
		}
	}
}

func (w *Worker) listenOnce(ctx context.Context, pool *pgxpool.Pool) error {
	pc, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// Take the connection out of the pool for good: a LISTENing session
	// must not be handed to other callers.
	conn := pc.Hijack()
	defer conn.Close(context.Background()) //nolint:errcheck
	if _, err := conn.Exec(ctx, "LISTEN "+store.NotifyChannel); err != nil {
		return err
	}
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		select {
		case w.wake <- struct{}{}:
		default: // every slot already has a pending wake-up
		}
	}
}

// reapLoop periodically requeues jobs whose worker vanished.
func (w *Worker) reapLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			requeued, lost, err := w.store.ReapExpired(ctx)
			if err != nil {
				if ctx.Err() == nil {
					w.log.Error("reaper", "err", err)
				}
				continue
			}
			if requeued+lost > 0 {
				w.m.Reaped.Add(float64(requeued), "requeued")
				w.m.Reaped.Add(float64(lost), "lost")
				w.log.Warn("reaped expired leases", "requeued", requeued, "lost", lost)
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func deref(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}
