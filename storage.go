package caddypgstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/certmagic"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// Storage implements certmagic.Storage on top of PostgreSQL. It is safe for
// concurrent use by multiple goroutines and by multiple process instances.
type Storage struct {
	pool             *pgxpool.Pool
	schema           string
	objectsTable     string
	locksTable       string
	operationTimeout time.Duration
	lockTTL          time.Duration
	lockPollInterval time.Duration
	lifeCtx          context.Context
	lifeCancel       context.CancelFunc

	stateMu      sync.Mutex
	reservations map[string]*lockReservation

	backgroundMu sync.Mutex
	backgroundWG sync.WaitGroup
	closing      atomic.Bool

	reaper *leaseReaper
	logger *zap.Logger
}

func newStorage(pool *pgxpool.Pool, schema string, operationTimeout, lockTTL, lockPollInterval time.Duration) *Storage {
	return newStorageWithContext(context.Background(), pool, schema, operationTimeout, lockTTL, lockPollInterval)
}

func newStorageWithContext(parent context.Context, pool *pgxpool.Pool, schema string, operationTimeout, lockTTL, lockPollInterval time.Duration) *Storage {
	if parent == nil {
		parent = context.Background()
	}
	if operationTimeout <= 0 {
		operationTimeout = defaultOperationTimeout
	}
	if lockTTL <= 0 {
		lockTTL = defaultLockTTL
	}
	if lockPollInterval <= 0 {
		lockPollInterval = defaultLockPollInterval
	}
	lifeCtx, lifeCancel := context.WithCancel(parent)
	objects, locks := tableNames(schema)
	s := &Storage{
		pool:             pool,
		schema:           schema,
		objectsTable:     objects,
		locksTable:       locks,
		operationTimeout: operationTimeout,
		lockTTL:          lockTTL,
		lockPollInterval: lockPollInterval,
		lifeCtx:          lifeCtx,
		lifeCancel:       lifeCancel,
		reservations:     make(map[string]*lockReservation),
		logger:           zap.NewNop(),
	}
	s.reaper = &leaseReaper{store: s}
	return s
}

func (s *Storage) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, s.operationTimeout)
}

func (s *Storage) isClosing() bool {
	return s.closing.Load()
}

func (s *Storage) checkOpen() error {
	if s == nil || s.pool == nil || s.isClosing() {
		return ErrStorageClosed
	}
	return nil
}

// startBackground starts work tied to the storage lifecycle. The check and
// WaitGroup Add are serialized with close so Cleanup cannot race Add/Wait.
func (s *Storage) startBackground(fn func(context.Context)) bool {
	s.backgroundMu.Lock()
	if s.closing.Load() {
		s.backgroundMu.Unlock()
		return false
	}
	s.backgroundWG.Add(1)
	ctx := s.lifeCtx
	s.backgroundMu.Unlock()

	go func() {
		defer s.backgroundWG.Done()
		fn(ctx)
	}()
	return true
}

func validateStorageKey(key string, allowEmpty bool) error {
	if key == "" {
		if allowEmpty {
			return nil
		}
		return fmt.Errorf("storage key must not be empty")
	}
	if strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
		return fmt.Errorf("storage key %q must not have a leading or trailing slash", key)
	}
	return nil
}

func (s *Storage) Store(ctx context.Context, key string, value []byte) error {
	if err := validateStorageKey(key, false); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	query := fmt.Sprintf(`
INSERT INTO %s (key, value, modified_at)
VALUES ($1, $2, clock_timestamp())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, modified_at = EXCLUDED.modified_at`, s.objectsTable)
	if _, err := s.pool.Exec(opCtx, query, key, value); err != nil {
		return fmt.Errorf("store %q: %w", key, err)
	}
	return nil
}

func (s *Storage) Load(ctx context.Context, key string) ([]byte, error) {
	if err := validateStorageKey(key, false); err != nil {
		return nil, err
	}
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	query := fmt.Sprintf(`SELECT value FROM %s WHERE key = $1`, s.objectsTable)
	var value []byte
	if err := s.pool.QueryRow(opCtx, query, key).Scan(&value); err != nil {
		if err == pgx.ErrNoRows {
			return nil, notFound("key", key)
		}
		return nil, fmt.Errorf("load %q: %w", key, err)
	}
	return value, nil
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	if err := validateStorageKey(key, true); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	query := fmt.Sprintf(`
DELETE FROM %s
WHERE $1 = '' OR key = $1 OR left(key, length($1) + 1) = $1 || '/'`, s.objectsTable)
	if _, err := s.pool.Exec(opCtx, query, key); err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	return nil
}

func (s *Storage) Exists(ctx context.Context, key string) bool {
	if validateStorageKey(key, true) != nil || s.checkOpen() != nil {
		return false
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	query := fmt.Sprintf(`
SELECT EXISTS (
	SELECT 1 FROM %s
	WHERE $1 = '' OR key = $1 OR left(key, length($1) + 1) = $1 || '/'
)`, s.objectsTable)
	var exists bool
	if err := s.pool.QueryRow(opCtx, query, key).Scan(&exists); err != nil {
		logger := s.logger
		if logger == nil {
			logger = zap.NewNop()
		}
		logger.Warn("PostgreSQL Exists check failed", zap.Error(err))
		return false
	}
	return exists
}

func (s *Storage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	if err := validateStorageKey(prefix, true); err != nil {
		return nil, err
	}
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	query := fmt.Sprintf(`
SELECT key FROM %s
WHERE $1 = '' OR key = $1 OR left(key, length($1) + 1) = $1 || '/'
ORDER BY key`, s.objectsTable)
	rows, err := s.pool.Query(opCtx, query, prefix)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", prefix, err)
	}
	defer rows.Close()

	result := make(map[string]struct{})
	exact := false
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("list %q: %w", prefix, err)
		}
		if key == prefix {
			exact = true
			// A terminal object is returned by List only when it is below the
			// requested directory, not when the requested key names the object.
			continue
		}
		relative := key
		if prefix != "" {
			relative = strings.TrimPrefix(key, prefix+"/")
		}
		parts := strings.Split(relative, "/")
		if recursive {
			for i := 1; i <= len(parts); i++ {
				candidate := strings.Join(parts[:i], "/")
				if prefix != "" {
					candidate = prefix + "/" + candidate
				}
				result[candidate] = struct{}{}
			}
		} else {
			candidate := parts[0]
			if prefix != "" {
				candidate = prefix + "/" + candidate
			}
			result[candidate] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list %q: %w", prefix, err)
	}
	if len(result) == 0 && !exact {
		return nil, notFound("prefix", prefix)
	}
	keys := make([]string, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *Storage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	if err := validateStorageKey(key, true); err != nil {
		return certmagic.KeyInfo{}, err
	}
	if err := s.checkOpen(); err != nil {
		return certmagic.KeyInfo{}, err
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	query := fmt.Sprintf(`SELECT modified_at, octet_length(value) FROM %s WHERE key = $1`, s.objectsTable)
	var modified time.Time
	var size int64
	if err := s.pool.QueryRow(opCtx, query, key).Scan(&modified, &size); err == nil {
		return certmagic.KeyInfo{Key: key, Modified: modified, Size: size, IsTerminal: true}, nil
	} else if err != pgx.ErrNoRows {
		return certmagic.KeyInfo{}, fmt.Errorf("stat %q: %w", key, err)
	}

	directoryQuery := fmt.Sprintf(`
SELECT EXISTS (
	SELECT 1 FROM %s
	WHERE $1 = '' OR left(key, length($1) + 1) = $1 || '/'
)`, s.objectsTable)
	var exists bool
	if err := s.pool.QueryRow(opCtx, directoryQuery, key).Scan(&exists); err != nil {
		return certmagic.KeyInfo{}, fmt.Errorf("stat %q: %w", key, err)
	}
	if !exists {
		return certmagic.KeyInfo{}, notFound("key", key)
	}
	return certmagic.KeyInfo{Key: key, IsTerminal: false}, nil
}

func (s *Storage) close() {
	if s == nil {
		return
	}
	s.closing.Store(true)
	s.stateMu.Lock()
	reservations := make([]*lockReservation, 0, len(s.reservations))
	for _, reservation := range s.reservations {
		reservations = append(reservations, reservation)
	}
	s.stateMu.Unlock()

	s.backgroundMu.Lock()
	s.lifeCancel()
	s.backgroundMu.Unlock()

	// Stop heartbeats and best-effort release leases before closing the pool.
	for _, reservation := range reservations {
		reservation.stopHeartbeat()
		if reservation.isAcquired() {
			ctx, cancel := s.operationContext(context.Background())
			_, _ = s.deleteLock(ctx, reservation.name, reservation.owner)
			cancel()
		}
		s.removeReservation(reservation)
	}
	s.backgroundWG.Wait()
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Storage) removeReservation(reservation *lockReservation) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if current, ok := s.reservations[reservation.name]; ok && current == reservation {
		delete(s.reservations, reservation.name)
		close(reservation.released)
	}
}

var _ certmagic.Storage = (*Storage)(nil)
var _ certmagic.TryLocker = (*Storage)(nil)
var _ certmagic.LockLeaseRenewer = (*Storage)(nil)
