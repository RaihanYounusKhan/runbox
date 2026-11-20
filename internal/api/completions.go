package api

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"runbox/internal/store"
)

// completionHub lets long-polling requests sleep until their job finishes.
// One dedicated connection LISTENs on store.DoneChannel; each notification
// carries a job ID and wakes every request waiting on that job. Without it,
// waiters would have to poll the database, adding up to a poll interval of
// latency to every request.
type completionHub struct {
	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
	healthy bool
	log     *slog.Logger
}

func newCompletionHub(log *slog.Logger) *completionHub {
	return &completionHub{waiters: map[string]map[chan struct{}]struct{}{}, log: log}
}

// subscribe returns a channel closed when jobID is reported done, and a
// function to release the subscription.
func (h *completionHub) subscribe(jobID string) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	h.mu.Lock()
	if h.waiters[jobID] == nil {
		h.waiters[jobID] = map[chan struct{}]struct{}{}
	}
	h.waiters[jobID][ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if set, ok := h.waiters[jobID]; ok {
			if _, still := set[ch]; still {
				delete(set, ch)
				if len(set) == 0 {
					delete(h.waiters, jobID)
				}
			}
		}
	}
}

func (h *completionHub) notify(jobID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.waiters[jobID] {
		close(ch)
	}
	delete(h.waiters, jobID)
}

// isHealthy reports whether the listener is connected. While it isn't,
// waiters fall back to polling more often.
func (h *completionHub) isHealthy() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy
}

func (h *completionHub) setHealthy(v bool) {
	h.mu.Lock()
	h.healthy = v
	h.mu.Unlock()
}

// run keeps the LISTEN connection alive until ctx is canceled.
func (h *completionHub) run(ctx context.Context, pool *pgxpool.Pool) {
	for ctx.Err() == nil {
		err := h.listenOnce(ctx, pool)
		h.setHealthy(false)
		if ctx.Err() != nil {
			return
		}
		h.log.Warn("completion listener disconnected; long-polls fall back to polling", "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

func (h *completionHub) listenOnce(ctx context.Context, pool *pgxpool.Pool) error {
	pc, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	conn := pc.Hijack()
	defer conn.Close(context.Background()) //nolint:errcheck
	if _, err := conn.Exec(ctx, "LISTEN "+store.DoneChannel); err != nil {
		return err
	}
	h.setHealthy(true)
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		h.notify(n.Payload)
	}
}
