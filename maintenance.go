package caddypgstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const reaperMinimumInterval = 10 * time.Minute

// leaseReaper is triggered by successful owner-guarded Unlock calls. It is
// intentionally event-driven: when no locks are active, no timer or polling
// goroutine is kept alive solely for cleanup.
type leaseReaper struct {
	store *Storage

	mu          sync.Mutex
	running     bool
	lastAttempt time.Time
}

func (r *leaseReaper) trigger() {
	if r == nil || r.store == nil || r.store.isClosing() {
		return
	}
	r.mu.Lock()
	now := time.Now()
	if r.running || (!r.lastAttempt.IsZero() && now.Sub(r.lastAttempt) < reaperMinimumInterval) {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.lastAttempt = now
	r.mu.Unlock()

	if !r.store.startBackground(func(ctx context.Context) {
		defer func() {
			r.mu.Lock()
			r.running = false
			r.mu.Unlock()
		}()
		opCtx, cancel := r.store.operationContext(ctx)
		defer cancel()
		query := fmt.Sprintf("DELETE FROM %s WHERE expires_at <= clock_timestamp()", r.store.locksTable)
		if _, err := r.store.pool.Exec(opCtx, query); err != nil {
			logger := r.store.logger
			if logger == nil {
				logger = zap.NewNop()
			}
			logger.Warn("expired PostgreSQL lock cleanup failed", zap.Error(err))
		}
	}) {
		r.mu.Lock()
		r.running = false
		r.mu.Unlock()
	}
}
