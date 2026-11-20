package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"runbox/internal/audit"
	"runbox/internal/ids"
	"runbox/internal/sandbox"
)

// NewJob is a validated submission.
type NewJob struct {
	APIKeyID    string
	Language    string
	Source      string
	Stdin       []byte
	Limits      sandbox.Limits
	MaxAttempts int
	ClientIP    string
}

// CreateJob inserts a queued job, audits it, and notifies idle workers.
// The NOTIFY is transactional: workers are only woken once the row is visible.
func (s *Store) CreateJob(ctx context.Context, nj NewJob) (*Job, error) {
	sum := sha256.Sum256([]byte(nj.Source))
	limits, err := json.Marshal(nj.Limits)
	if err != nil {
		return nil, err
	}
	id := ids.NewUUID()
	var j *Job
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		j, err = scanJob(tx.QueryRow(ctx,
			`INSERT INTO jobs (id, api_key_id, language, source, source_sha256, stdin, limits, status, max_attempts)
			 VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 'queued', $8)
			 RETURNING `+jobCols(""),
			id, nj.APIKeyID, nj.Language, nj.Source, hex.EncodeToString(sum[:]), nj.Stdin, string(limits), nj.MaxAttempts))
		if err != nil {
			return err
		}
		if _, err := audit.Append(ctx, tx, "key:"+nj.APIKeyID, audit.ActionJobSubmitted, id, map[string]any{
			"language":      nj.Language,
			"source_sha256": j.SourceSHA256,
			"source_bytes":  len(nj.Source),
			"stdin_bytes":   len(nj.Stdin),
			"limits":        nj.Limits,
			"client_ip":     nj.ClientIP,
		}); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, id)
		return err
	})
	return j, err
}

// GetJob fetches a job. If apiKeyID is non-empty, jobs owned by other keys
// are reported as not found (not forbidden) so IDs can't be probed.
func (s *Store) GetJob(ctx context.Context, id, apiKeyID string) (*Job, error) {
	if !ids.ValidUUID(id) {
		return nil, ErrNotFound
	}
	if apiKeyID == "" {
		return scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobCols("")+` FROM jobs WHERE id = $1`, id))
	}
	return scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobCols("")+` FROM jobs WHERE id = $1 AND api_key_id = $2`, id, apiKeyID))
}

// ListJobs returns a key's most recent jobs, newest first.
func (s *Store) ListJobs(ctx context.Context, apiKeyID string, status string, limit int) ([]*Job, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var st any
	if status != "" {
		st = status
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+jobCols("")+` FROM jobs
		WHERE api_key_id = $1 AND ($2::text IS NULL OR status = $2)
		ORDER BY created_at DESC LIMIT $3`, apiKeyID, st, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// QueueDepth returns the number of queued jobs overall and for one key.
func (s *Store) QueueDepth(ctx context.Context, apiKeyID string) (total, forKey int, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE api_key_id = $1)
		FROM jobs WHERE status = 'queued'`, apiKeyID).Scan(&total, &forKey)
	return
}

// StatusCounts returns counts of queued and running jobs (for metrics).
func (s *Store) StatusCounts(ctx context.Context) (queued, running int, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status = 'queued'),
		count(*) FILTER (WHERE status = 'running') FROM jobs WHERE status IN ('queued','running')`).Scan(&queued, &running)
	return
}

// RequestCancel cancels a queued job immediately, or flags a running job so
// its worker kills it at the next heartbeat. Finished jobs are a conflict.
func (s *Store) RequestCancel(ctx context.Context, id, apiKeyID string) (*Job, error) {
	if !ids.ValidUUID(id) {
		return nil, ErrNotFound
	}
	var j *Job
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		cur, err := scanJob(tx.QueryRow(ctx,
			`SELECT `+jobCols("")+` FROM jobs WHERE id = $1 AND api_key_id = $2 FOR UPDATE`, id, apiKeyID))
		if err != nil {
			return err
		}
		actor := "key:" + apiKeyID
		switch cur.Status {
		case sandbox.StatusQueued:
			j, err = scanJob(tx.QueryRow(ctx, `UPDATE jobs SET status = 'canceled', finished_at = now()
				WHERE id = $1 RETURNING `+jobCols(""), id))
			if err != nil {
				return err
			}
			if _, err = audit.Append(ctx, tx, actor, audit.ActionJobCanceled, id, map[string]any{"while": "queued"}); err != nil {
				return err
			}
			return notifyDone(ctx, tx, id)
		case sandbox.StatusRunning:
			if cur.CancelRequested {
				j = cur
				return nil
			}
			j, err = scanJob(tx.QueryRow(ctx, `UPDATE jobs SET cancel_requested = true
				WHERE id = $1 RETURNING `+jobCols(""), id))
			if err != nil {
				return err
			}
			_, err = audit.Append(ctx, tx, actor, audit.ActionJobCancelReq, id, map[string]any{})
			return err
		default:
			j = cur
			return ErrConflict
		}
	})
	if errors.Is(err, ErrConflict) {
		return j, ErrConflict
	}
	return j, err
}
