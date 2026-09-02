package caddypgstore

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type lockReservation struct {
	name  string
	owner uuid.UUID

	mu              sync.Mutex
	acquired        bool
	heartbeatCancel context.CancelFunc
	heartbeatDone   chan struct{}
	released        chan struct{}
}

func (r *lockReservation) isAcquired() bool {
	r.mu.Lock()
	acquired := r.acquired
	r.mu.Unlock()
	return acquired
}

func (r *lockReservation) setAcquired(acquired bool) {
	r.mu.Lock()
	r.acquired = acquired
	r.mu.Unlock()
}

func (r *lockReservation) stopHeartbeat() {
	r.mu.Lock()
	cancel := r.heartbeatCancel
	done := r.heartbeatDone
	r.heartbeatCancel = nil
	r.heartbeatDone = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (r *lockReservation) setHeartbeat(cancel context.CancelFunc, done chan struct{}) {
	r.mu.Lock()
	r.heartbeatCancel = cancel
	r.heartbeatDone = done
	r.mu.Unlock()
}

// reserve serializes calls for a name within one Storage instance. A pending
// reservation remains in the registry while it polls PostgreSQL, preventing a
// second local caller from bypassing the first caller's owner token.
func (s *Storage) reserve(ctx context.Context, name string, try bool) (*lockReservation, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if s.isClosing() {
			return nil, false, ErrStorageClosed
		}
		s.stateMu.Lock()
		if s.closing.Load() {
			s.stateMu.Unlock()
			return nil, false, ErrStorageClosed
		}
		if current, ok := s.reservations[name]; !ok {
			reservation := &lockReservation{name: name, owner: uuid.New(), released: make(chan struct{})}
			s.reservations[name] = reservation
			s.stateMu.Unlock()
			return reservation, true, nil
		} else {
			released := current.released
			s.stateMu.Unlock()
			if try {
				return nil, false, nil
			}
			select {
			case <-released:
				continue
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}
	}
}

func (s *Storage) tryAcquire(ctx context.Context, name string, owner uuid.UUID) (bool, error) {
	ttlMicros := s.lockTTL.Microseconds()
	if ttlMicros < 1 {
		ttlMicros = 1
	}
	query := fmt.Sprintf(`
INSERT INTO %s (key, owner, expires_at)
VALUES ($1, $2, clock_timestamp() + ($3::bigint * interval '1 microsecond'))
ON CONFLICT (key) DO UPDATE
SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at
WHERE %s.expires_at <= clock_timestamp()
RETURNING key`, s.locksTable, s.locksTable)
	var returned string
	if err := s.pool.QueryRow(ctx, query, name, owner, ttlMicros).Scan(&returned); err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Storage) Lock(ctx context.Context, name string) error {
	if err := validateStorageKey(name, false); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	reservation, ok, err := s.reserve(ctx, name, false)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("lock reservation for %q was not created", name)
	}
	for {
		if err := s.checkOpen(); err != nil {
			s.removeReservation(reservation)
			return err
		}
		opCtx, cancel := s.operationContext(ctx)
		acquired, acquireErr := s.tryAcquire(opCtx, name, reservation.owner)
		cancel()
		if acquireErr != nil {
			s.removeReservation(reservation)
			return fmt.Errorf("lock %q: %w", name, acquireErr)
		}
		if acquired {
			reservation.setAcquired(true)
			if !s.startHeartbeat(reservation) {
				reservation.setAcquired(false)
				opCtx, cancel := s.operationContext(context.Background())
				_, _ = s.deleteLock(opCtx, name, reservation.owner)
				cancel()
				s.removeReservation(reservation)
				return ErrStorageClosed
			}
			return nil
		}
		wait := jitterDuration(s.lockPollInterval)
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-contextDone(ctx):
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			s.removeReservation(reservation)
			return contextErr(ctx)
		}
	}
}

func (s *Storage) TryLock(ctx context.Context, name string) (bool, error) {
	if err := validateStorageKey(name, false); err != nil {
		return false, err
	}
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	reservation, ok, err := s.reserve(ctx, name, true)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	opCtx, cancel := s.operationContext(ctx)
	acquired, acquireErr := s.tryAcquire(opCtx, name, reservation.owner)
	cancel()
	if acquireErr != nil {
		s.removeReservation(reservation)
		return false, fmt.Errorf("try lock %q: %w", name, acquireErr)
	}
	if !acquired {
		s.removeReservation(reservation)
		return false, nil
	}
	reservation.setAcquired(true)
	if !s.startHeartbeat(reservation) {
		reservation.setAcquired(false)
		opCtx, cancel := s.operationContext(context.Background())
		_, _ = s.deleteLock(opCtx, name, reservation.owner)
		cancel()
		s.removeReservation(reservation)
		return false, ErrStorageClosed
	}
	return true, nil
}

func (s *Storage) startHeartbeat(reservation *lockReservation) bool {
	interval := s.lockTTL / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ctx, cancel := context.WithCancel(s.lifeCtx)
	done := make(chan struct{})
	reservation.setHeartbeat(cancel, done)
	started := s.startBackground(func(_ context.Context) {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				opCtx, opCancel := s.operationContext(ctx)
				err := s.renewWithOwner(opCtx, reservation.name, reservation.owner, s.lockTTL)
				opCancel()
				if err == ErrLockLost {
					logger := s.logger
					if logger == nil {
						logger = zap.NewNop()
					}
					logger.Warn("PostgreSQL lock lease lost")
					return
				} else if err != nil {
					logger := s.logger
					if logger == nil {
						logger = zap.NewNop()
					}
					logger.Debug("PostgreSQL lock heartbeat failed", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	})
	if !started {
		cancel()
		close(done)
	}
	return started
}

func (s *Storage) RenewLockLease(ctx context.Context, name string, leaseDuration time.Duration) error {
	if err := validateStorageKey(name, false); err != nil {
		return err
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.stateMu.Lock()
	reservation, ok := s.reservations[name]
	s.stateMu.Unlock()
	if !ok || !reservation.isAcquired() {
		return ErrLockNotHeld
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.renewWithOwner(opCtx, name, reservation.owner, leaseDuration); err != nil {
		return err
	}
	return nil
}

func (s *Storage) renewWithOwner(ctx context.Context, name string, owner uuid.UUID, duration time.Duration) error {
	micros := duration.Microseconds()
	if micros < 1 {
		micros = 1
	}
	query := fmt.Sprintf(`
UPDATE %s
SET expires_at = GREATEST(
		expires_at,
		clock_timestamp() + ($3::bigint * interval '1 microsecond')
	)
WHERE key = $1 AND owner = $2
RETURNING key`, s.locksTable)
	var returned string
	if err := s.pool.QueryRow(ctx, query, name, owner, micros).Scan(&returned); err != nil {
		if err == pgx.ErrNoRows {
			return ErrLockLost
		}
		return fmt.Errorf("renew lock %q: %w", name, err)
	}
	return nil
}

func (s *Storage) Unlock(ctx context.Context, name string) error {
	if err := validateStorageKey(name, false); err != nil {
		return err
	}
	s.stateMu.Lock()
	reservation, ok := s.reservations[name]
	s.stateMu.Unlock()
	if !ok || !reservation.isAcquired() {
		return ErrLockNotHeld
	}
	reservation.stopHeartbeat()
	opCtx, cancel := s.operationContext(ctx)
	deleted, err := s.deleteLock(opCtx, name, reservation.owner)
	cancel()
	s.removeReservation(reservation)
	if err != nil {
		return fmt.Errorf("unlock %q: %w", name, err)
	}
	if !deleted {
		return ErrLockLost
	}
	// Unlock's owner-guarded DELETE is complete before maintenance starts. The
	// reaper is deliberately best-effort and can never delay or fail Unlock.
	if s.reaper != nil {
		s.reaper.trigger()
	}
	return nil
}

func (s *Storage) deleteLock(ctx context.Context, name string, owner uuid.UUID) (bool, error) {
	query := fmt.Sprintf(`DELETE FROM %s WHERE key = $1 AND owner = $2`, s.locksTable)
	result, err := s.pool.Exec(ctx, query, name, owner)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

func contextDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func jitterDuration(base time.Duration) time.Duration {
	if base <= 0 {
		return time.Millisecond
	}
	factor := 0.9 + rand.Float64()*0.2
	wait := time.Duration(float64(base) * factor)
	if wait < time.Millisecond {
		return time.Millisecond
	}
	return wait
}
