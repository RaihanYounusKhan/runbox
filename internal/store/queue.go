package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"runbox/internal/audit"
	"runbox/internal/sandbox"
)

// The queue is the jobs table itself. Workers claim rows with
// SELECT ... FOR UPDATE SKIP LOCKED, so concurrent workers never block on or
// double-claim the same row, and a claimed job carries a lease that the
// worker must keep renewing. If the worker dies, the lease expires and the
// reaper puts the job back (at-least-once execution).
//
// Every write after the claim is fenced on (lease_owner, status='running'):
// a worker that lost its lease can't overwrite the result of whoever
// re-ran the job.

// claimSQL picks the oldest runnable job whose key is below its concurrency
// cap. The per-key check is a soft limit: two workers claiming at the same
// instant can overshoot by one, which is acceptable for fairness purposes.
const claimSQL = `
WITH candidate AS (
    SELECT j.id
    FROM jobs j
    JOIN api_keys k ON k.id = j.api_key_id
    WHERE j.status = 'queued'
      AND j.run_after <= now()
      AND (SELECT count(*) FROM jobs r
           WHERE r.api_key_id = j.api_key_id AND r.status = 'running') < k.max_concurrent
    ORDER BY j.created_at
    LIMIT 1
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE jobs SET
    status           = 'running',
    attempts         = jobs.attempts + 1,
    lease_owner      = $1,
    lease_expires_at = now() + make_interval(secs => $2),
    started_at       = now(),
    cancel_requested = false,
    error            = NULL
FROM candidate
WHERE jobs.id = candidate.id
RETURNING ` // + jobCols("jobs.")

// ClaimJob claims the next job for workerID, or returns (nil, nil) if
// nothing is runnable.
func (s *Store) ClaimJob(ctx context.Context, workerID string, lease time.Duration) (*Job, error) {
	var j *Job
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		j, err = scanJob(tx.QueryRow(ctx, claimSQL+jobCols("jobs."), workerID, lease.Seconds()))
		if errors.Is(err, ErrNotFound) {
			j = nil
			return nil
		}
		if err != nil {
			return err
		}
		_, err = audit.Append(ctx, tx, "worker:"+workerID, audit.ActionJobStarted, j.ID, map[string]any{
			"attempt": j.Attempts,
			"queued_ms": func() int64 {
				if j.StartedAt == nil {
					return 0
				}
				return j.StartedAt.Sub(j.CreatedAt).Milliseconds()
			}(),
		})
		return err
	})
	return j, err
}

// Heartbeat extends the lease. ok=false means the lease was lost (expired
// and reaped, or the job was finished by someone else): the caller must stop
// and must not write a result.
func (s *Store) Heartbeat(ctx context.Context, jobID, workerID string, lease time.Duration) (cancelRequested, ok bool, err error) {
	err = s.Pool.QueryRow(ctx, `UPDATE jobs SET lease_expires_at = now() + make_interval(secs => $3)
		WHERE id = $1 AND lease_owner = $2 AND status = 'running'
		RETURNING cancel_requested`, jobID, workerID, lease.Seconds()).Scan(&cancelRequested)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	return cancelRequested, err == nil, err
}

// CompleteJob records a terminal result. Returns ok=false if the lease was
// lost in the meantime (the result is discarded).
func (s *Store) CompleteJob(ctx context.Context, jobID, workerID string, res sandbox.Result, errMsg string) (bool, error) {
	ok := false
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var errArg any
		if errMsg != "" {
			errArg = errMsg
		}
		var attempts int
		err := tx.QueryRow(ctx, `UPDATE jobs SET
				status = $3, exit_code = $4, stdout = $5, stderr = $6,
				stdout_truncated = $7, stderr_truncated = $8, duration_ms = $9, error = $10,
				finished_at = now(), lease_owner = NULL, lease_expires_at = NULL
			WHERE id = $1 AND lease_owner = $2 AND status = 'running'
			RETURNING attempts`,
			jobID, workerID, string(res.Status), res.ExitCode, res.Stdout, res.Stderr,
			res.StdoutTruncated, res.StderrTruncated, res.Duration.Milliseconds(), errArg).Scan(&attempts)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		details := map[string]any{
			"status":           res.Status,
			"exit_code":        res.ExitCode,
			"duration_ms":      res.Duration.Milliseconds(),
			"attempt":          attempts,
			"stdout_bytes":     len(res.Stdout),
			"stderr_bytes":     len(res.Stderr),
			"stdout_sha256":    sha256Hex(res.Stdout),
			"stderr_sha256":    sha256Hex(res.Stderr),
			"output_truncated": res.StdoutTruncated || res.StderrTruncated,
		}
		if errMsg != "" {
			details["error"] = errMsg
		}
		if _, err = audit.Append(ctx, tx, "worker:"+workerID, audit.ActionJobFinished, jobID, details); err != nil {
			return err
		}
		return notifyDone(ctx, tx, jobID)
	})
	return ok, err
}

// RetryOrFail handles an infrastructure failure: requeue with exponential
// backoff if attempts remain, otherwise mark the job as errored.
func (s *Store) RetryOrFail(ctx context.Context, jobID, workerID, errMsg string) (requeued bool, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		var attempts, maxAttempts int
		err := tx.QueryRow(ctx, `SELECT attempts, max_attempts FROM jobs
			WHERE id = $1 AND lease_owner = $2 AND status = 'running' FOR UPDATE`, jobID, workerID).Scan(&attempts, &maxAttempts)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // lease lost; someone else owns it now
		}
		if err != nil {
			return err
		}
		actor := "worker:" + workerID
		if attempts < maxAttempts {
			requeued = true
			delay := Backoff(attempts)
			if _, err := tx.Exec(ctx, `UPDATE jobs SET status = 'queued', lease_owner = NULL, lease_expires_at = NULL,
				started_at = NULL, error = $2, run_after = now() + make_interval(secs => $3) WHERE id = $1`,
				jobID, errMsg, delay.Seconds()); err != nil {
				return err
			}
			_, err = audit.Append(ctx, tx, actor, audit.ActionJobRetried, jobID, map[string]any{
				"attempt": attempts, "error": errMsg, "retry_in_ms": delay.Milliseconds(),
			})
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE jobs SET status = 'error', lease_owner = NULL, lease_expires_at = NULL,
			finished_at = now(), error = $2 WHERE id = $1`, jobID, errMsg); err != nil {
			return err
		}
		if _, err = audit.Append(ctx, tx, actor, audit.ActionJobFinished, jobID, map[string]any{
			"status": sandbox.StatusError, "attempt": attempts, "error": errMsg,
		}); err != nil {
			return err
		}
		return notifyDone(ctx, tx, jobID)
	})
	return requeued, err
}

// ReleaseJob puts a job back in the queue without counting the attempt,
// used when a worker shuts down while the job is running.
func (s *Store) ReleaseJob(ctx context.Context, jobID, workerID, reason string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE jobs SET status = 'queued', attempts = GREATEST(attempts - 1, 0),
			lease_owner = NULL, lease_expires_at = NULL, started_at = NULL, run_after = now()
			WHERE id = $1 AND lease_owner = $2 AND status = 'running'`, jobID, workerID)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		_, err = audit.Append(ctx, tx, "worker:"+workerID, audit.ActionJobReleased, jobID, map[string]any{"reason": reason})
		return err
	})
}

// ReapExpired finds running jobs whose lease has expired (their worker
// crashed or was partitioned away) and requeues or fails them. Safe to run
// from every worker concurrently thanks to SKIP LOCKED.
func (s *Store) ReapExpired(ctx context.Context) (requeued, lost int, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text, attempts, max_attempts, COALESCE(lease_owner, '')
			FROM jobs WHERE status = 'running' AND lease_expires_at < now()
			ORDER BY lease_expires_at LIMIT 100 FOR UPDATE SKIP LOCKED`)
		if err != nil {
			return err
		}
		type expired struct {
			id                    string
			attempts, maxAttempts int
			owner                 string
		}
		var list []expired
		for rows.Next() {
			var e expired
			if err := rows.Scan(&e.id, &e.attempts, &e.maxAttempts, &e.owner); err != nil {
				rows.Close()
				return err
			}
			list = append(list, e)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for _, e := range list {
			msg := fmt.Sprintf("lease held by %q expired (worker lost)", e.owner)
			if e.attempts < e.maxAttempts {
				delay := Backoff(e.attempts)
				if _, err := tx.Exec(ctx, `UPDATE jobs SET status = 'queued', lease_owner = NULL, lease_expires_at = NULL,
					started_at = NULL, error = $2, run_after = now() + make_interval(secs => $3) WHERE id = $1`,
					e.id, msg, delay.Seconds()); err != nil {
					return err
				}
				if _, err := audit.Append(ctx, tx, "system:reaper", audit.ActionJobRequeued, e.id, map[string]any{
					"attempt": e.attempts, "previous_owner": e.owner, "retry_in_ms": delay.Milliseconds(),
				}); err != nil {
					return err
				}
				requeued++
				continue
			}
			msg = fmt.Sprintf("%s after %d attempts", msg, e.attempts)
			if _, err := tx.Exec(ctx, `UPDATE jobs SET status = 'error', lease_owner = NULL, lease_expires_at = NULL,
				finished_at = now(), error = $2 WHERE id = $1`, e.id, msg); err != nil {
				return err
			}
			if _, err := audit.Append(ctx, tx, "system:reaper", audit.ActionJobLost, e.id, map[string]any{
				"attempts": e.attempts, "previous_owner": e.owner,
			}); err != nil {
				return err
			}
			if err := notifyDone(ctx, tx, e.id); err != nil {
				return err
			}
			lost++
		}
		return nil
	})
	return requeued, lost, err
}

// Backoff returns the retry delay after the given attempt: 2s, 4s, 8s ... capped at 60s.
func Backoff(attempt int) time.Duration {
	secs := math.Min(60, math.Pow(2, float64(max(attempt, 1))))
	return time.Duration(secs * float64(time.Second))
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
