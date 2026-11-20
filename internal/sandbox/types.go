// Package sandbox runs untrusted programs in isolated containers.
package sandbox

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Limits bounds the resources a single job may consume.
type Limits struct {
	TimeoutMs   int     `json:"timeout_ms"`
	MemoryMB    int     `json:"memory_mb"`
	CPUs        float64 `json:"cpus"`
	Pids        int     `json:"pids"`
	OutputBytes int     `json:"output_bytes"` // per stream (stdout, stderr)
}

// DefaultLimits apply to any field the client leaves at zero.
var DefaultLimits = Limits{TimeoutMs: 5_000, MemoryMB: 128, CPUs: 0.5, Pids: 64, OutputBytes: 64 << 10}

// MaxLimits are the ceilings a client may request.
var MaxLimits = Limits{TimeoutMs: 30_000, MemoryMB: 512, CPUs: 2, Pids: 256, OutputBytes: 1 << 20}

// WithDefaults fills zero fields from DefaultLimits.
func (l Limits) WithDefaults() Limits {
	if l.TimeoutMs == 0 {
		l.TimeoutMs = DefaultLimits.TimeoutMs
	}
	if l.MemoryMB == 0 {
		l.MemoryMB = DefaultLimits.MemoryMB
	}
	if l.CPUs == 0 {
		l.CPUs = DefaultLimits.CPUs
	}
	if l.Pids == 0 {
		l.Pids = DefaultLimits.Pids
	}
	if l.OutputBytes == 0 {
		l.OutputBytes = DefaultLimits.OutputBytes
	}
	return l
}

// Validate checks every field is positive and within MaxLimits.
func (l Limits) Validate() error {
	check := func(name string, v, max float64, min float64) error {
		if v < min || v > max {
			return fmt.Errorf("limits.%s must be between %g and %g", name, min, max)
		}
		return nil
	}
	return errors.Join(
		check("timeout_ms", float64(l.TimeoutMs), float64(MaxLimits.TimeoutMs), 100),
		check("memory_mb", float64(l.MemoryMB), float64(MaxLimits.MemoryMB), 16),
		check("cpus", l.CPUs, MaxLimits.CPUs, 0.05),
		check("pids", float64(l.Pids), float64(MaxLimits.Pids), 8),
		check("output_bytes", float64(l.OutputBytes), float64(MaxLimits.OutputBytes), 1024),
	)
}

// Timeout returns the wall-clock limit as a Duration.
func (l Limits) Timeout() time.Duration { return time.Duration(l.TimeoutMs) * time.Millisecond }

// Status is the terminal outcome of a job.
type Status string

const (
	StatusQueued         Status = "queued"
	StatusRunning        Status = "running"
	StatusSucceeded      Status = "succeeded"             // exit code 0
	StatusFailed         Status = "failed"                // non-zero exit code
	StatusTimedOut       Status = "timed_out"             // wall-clock limit hit
	StatusMemoryExceeded Status = "memory_limit_exceeded" // OOM-killed
	StatusOutputExceeded Status = "output_limit_exceeded" // wrote too much
	StatusCanceled       Status = "canceled"
	StatusError          Status = "error" // infrastructure failure, not the program's fault
)

// Terminal reports whether no further transitions are possible.
func (s Status) Terminal() bool { return s != StatusQueued && s != StatusRunning }

// Spec describes one execution.
type Spec struct {
	JobID    string
	Language Language
	Source   []byte
	Stdin    []byte
	Limits   Limits
}

// Result is what came out of an execution.
type Result struct {
	Status          Status
	ExitCode        *int
	Stdout          []byte
	Stderr          []byte
	StdoutTruncated bool
	StderrTruncated bool
	Duration        time.Duration
}

// Runner executes a Spec. It returns a non-nil error only for infrastructure
// failures (Docker unreachable, image missing, ...) which are safe to retry.
// Anything the program itself did, including crashing, is reported in Result.
//
// If ctx is canceled the runner kills the program and returns StatusCanceled.
type Runner interface {
	Run(ctx context.Context, spec Spec) (Result, error)
}
