// Package ratelimit provides per-key token-bucket rate limiting.
//
// Buckets live in process memory, so with N API replicas the effective limit
// is N times the configured rate. That is an accepted trade-off for this
// project; a shared limiter (Redis, or Postgres) would fix it at the cost of a
// network round-trip per request.
package ratelimit

import (
	"math"
	"sync"
	"time"
)

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter holds one token bucket per key.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	now     func() time.Time
}

// New returns an empty Limiter.
func New() *Limiter {
	return &Limiter{buckets: make(map[string]*bucket), now: time.Now}
}

// Allow takes one token from key's bucket, which refills at rps tokens per
// second up to burst. If no token is available it returns false and how long
// until one will be.
func (l *Limiter) Allow(key string, rps float64, burst int) (bool, time.Duration) {
	if rps <= 0 || burst <= 0 {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(burst), last: now}
		l.buckets[key] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens = math.Min(float64(burst), b.tokens+elapsed*rps)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / rps * float64(time.Second))
	return false, wait
}

// Sweep drops buckets idle for longer than maxIdle (they would be full anyway).
func (l *Limiter) Sweep(maxIdle time.Duration) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoff := l.now().Add(-maxIdle)
	n := 0
	for k, b := range l.buckets {
		if b.last.Before(cutoff) {
			delete(l.buckets, k)
			n++
		}
	}
	return n
}
