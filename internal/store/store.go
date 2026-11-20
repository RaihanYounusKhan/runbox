// Package store is the Postgres data layer: API keys, jobs, and the job
// queue. Every state transition that matters is audited in the same
// transaction that performs it.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"runbox/internal/sandbox"
)

var (
	// ErrNotFound means the row doesn't exist or isn't visible to the caller.
	ErrNotFound = errors.New("not found")
	// ErrConflict means the requested transition isn't valid in the current state.
	ErrConflict = errors.New("conflict")
)

// NotifyChannel is the LISTEN/NOTIFY channel used to wake idle workers.
const NotifyChannel = "runbox_jobs"

// DoneChannel carries the ID of every job that reaches a terminal state,
// waking API requests that are long-polling for it.
const DoneChannel = "runbox_job_done"

// notifyDone queues a completion notification, delivered on commit.
func notifyDone(ctx context.Context, tx pgx.Tx, jobID string) error {
	_, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, DoneChannel, jobID)
	return err
}

// Store wraps a pgx pool.
type Store struct {
	Pool *pgxpool.Pool
}

// New returns a Store.
func New(pool *pgxpool.Pool) *Store { return &Store{Pool: pool} }

// inTx runs fn in a transaction, committing if it returns nil.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Job is a row of the jobs table.
type Job struct {
	ID              string
	APIKeyID        string
	Language        string
	Source          string
	SourceSHA256    string
	Stdin           []byte
	Limits          sandbox.Limits
	Status          sandbox.Status
	Attempts        int
	MaxAttempts     int
	CancelRequested bool
	LeaseOwner      *string
	LeaseExpiresAt  *time.Time
	CreatedAt       time.Time
	StartedAt       *time.Time
	FinishedAt      *time.Time
	ExitCode        *int
	Stdout          []byte
	Stderr          []byte
	StdoutTruncated bool
	StderrTruncated bool
	DurationMs      *int64
	Error           *string
}

// jobCols lists job columns, qualified with prefix (e.g. "jobs.") when the
// query joins other tables.
func jobCols(p string) string {
	cols := []string{
		"id::text", "api_key_id::text", "language", "source", "source_sha256", "stdin", "limits::text",
		"status", "attempts", "max_attempts", "cancel_requested", "lease_owner", "lease_expires_at",
		"created_at", "started_at", "finished_at", "exit_code", "stdout", "stderr",
		"stdout_truncated", "stderr_truncated", "duration_ms", "error",
	}
	for i, c := range cols {
		cols[i] = p + c
	}
	return strings.Join(cols, ", ")
}

func scanJob(row pgx.Row) (*Job, error) {
	var j Job
	var limits, status string
	err := row.Scan(&j.ID, &j.APIKeyID, &j.Language, &j.Source, &j.SourceSHA256, &j.Stdin, &limits,
		&status, &j.Attempts, &j.MaxAttempts, &j.CancelRequested, &j.LeaseOwner, &j.LeaseExpiresAt,
		&j.CreatedAt, &j.StartedAt, &j.FinishedAt, &j.ExitCode, &j.Stdout, &j.Stderr,
		&j.StdoutTruncated, &j.StderrTruncated, &j.DurationMs, &j.Error)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	j.Status = sandbox.Status(status)
	if err := json.Unmarshal([]byte(limits), &j.Limits); err != nil {
		return nil, fmt.Errorf("decode limits: %w", err)
	}
	return &j, nil
}
