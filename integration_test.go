package caddypgstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not set")
	}
	return dsn
}

var integrationSchemaCounter atomic.Uint64

func randomIntegrationSchema() string {
	return fmt.Sprintf("caddy_pgstore_test_%d_%d", time.Now().UnixNano(), integrationSchemaCounter.Add(1))
}

func openIntegrationPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	config.MaxConns = 10
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open integration pool: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping integration pool: %v", err)
	}
	return pool
}

func setupIntegrationStorage(t *testing.T, ttl time.Duration) (*Storage, *pgxpool.Pool, string) {
	t.Helper()
	dsn := integrationDSN(t)
	schema := randomIntegrationSchema()
	pool := openIntegrationPool(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := ensureSchema(ctx, pool, schema); err != nil {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE")
		cancel()
		pool.Close()
		t.Fatalf("ensure integration schema: %v", err)
	}
	cancel()
	store := newStorage(pool, schema, 30*time.Second, ttl, 10*time.Millisecond)
	return store, pool, schema
}

func cleanupIntegrationStorage(t *testing.T, stores ...*Storage) {
	t.Helper()
	for _, store := range stores {
		if store != nil {
			store.close()
		}
	}
}

func dropIntegrationSchema(t *testing.T, dsn, schema string) {
	t.Helper()
	pool := openIntegrationPool(t, dsn)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE"); err != nil {
		t.Errorf("drop integration schema: %v", err)
	}
}

func TestIntegrationStorageObjectSemantics(t *testing.T) {
	dsn := integrationDSN(t)
	store, pool, schema := setupIntegrationStorage(t, 2*time.Second)
	defer func() {
		cleanupIntegrationStorage(t, store)
		dropIntegrationSchema(t, dsn, schema)
		_ = pool
	}()

	ctx := context.Background()
	if err := store.Store(ctx, "a", []byte("root")); err != nil {
		t.Fatal(err)
	}
	if err := store.Store(ctx, "a/b", []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := store.Store(ctx, "a/c/d", []byte("d")); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(ctx, "a"); err != nil || string(got) != "root" {
		t.Fatalf("Load(a) = %q, %v", got, err)
	}
	if !store.Exists(ctx, "a/c") || !store.Exists(ctx, "a/c/d") || store.Exists(ctx, "missing") {
		t.Fatal("Exists did not report terminal and implicit directory keys correctly")
	}
	shallow, err := store.List(ctx, "a", false)
	if err != nil {
		t.Fatalf("List shallow: %v", err)
	}
	if want := []string{"a/b", "a/c"}; !equalStrings(shallow, want) {
		t.Fatalf("List shallow = %v, want %v", shallow, want)
	}
	deep, err := store.List(ctx, "a", true)
	if err != nil {
		t.Fatalf("List recursive: %v", err)
	}
	if want := []string{"a/b", "a/c", "a/c/d"}; !equalStrings(deep, want) {
		t.Fatalf("List recursive = %v, want %v", deep, want)
	}
	info, err := store.Stat(ctx, "a/c")
	if err != nil || info.IsTerminal {
		t.Fatalf("Stat implicit directory = %+v, %v", info, err)
	}
	info, err = store.Stat(ctx, "a/c/d")
	if err != nil || !info.IsTerminal || info.Size != 1 {
		t.Fatalf("Stat terminal = %+v, %v", info, err)
	}
	if err := store.Delete(ctx, "a/c"); err != nil {
		t.Fatal(err)
	}
	if store.Exists(ctx, "a/c") || store.Exists(ctx, "a/c/d") {
		t.Fatal("Delete did not remove an implicit subtree")
	}
	if _, err := store.Load(ctx, "a/c/d"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load deleted key error = %v, want fs.ErrNotExist", err)
	}
}

func TestIntegrationTryLockOnlyOneWinner(t *testing.T) {
	dsn := integrationDSN(t)
	schema := randomIntegrationSchema()
	first := openIntegrationPool(t, dsn)
	second := openIntegrationPool(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := ensureSchema(ctx, first, schema); err != nil {
		_, _ = first.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE")
		cancel()
		first.Close()
		second.Close()
		t.Fatalf("ensure schema: %v", err)
	}
	cancel()
	a := newStorage(first, schema, 30*time.Second, 2*time.Second, 10*time.Millisecond)
	b := newStorage(second, schema, 30*time.Second, 2*time.Second, 10*time.Millisecond)
	defer func() {
		cleanupIntegrationStorage(t, a, b)
		dropIntegrationSchema(t, dsn, schema)
	}()

	var winners atomic.Int32
	var winner *Storage
	var winnerMu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			store := a
			if i%2 == 1 {
				store = b
			}
			ok, err := store.TryLock(context.Background(), "concurrent")
			if err != nil {
				t.Errorf("TryLock: %v", err)
				return
			}
			if ok {
				winners.Add(1)
				winnerMu.Lock()
				winner = store
				winnerMu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if got := winners.Load(); got != 1 {
		t.Fatalf("TryLock winners = %d, want 1", got)
	}
	winnerMu.Lock()
	if winner != nil {
		if err := winner.Unlock(context.Background(), "concurrent"); err != nil {
			t.Fatalf("unlock winner: %v", err)
		}
	}
	winnerMu.Unlock()
}

func TestIntegrationBlockingLockCriticalSection(t *testing.T) {
	dsn := integrationDSN(t)
	schema := randomIntegrationSchema()
	first := openIntegrationPool(t, dsn)
	second := openIntegrationPool(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := ensureSchema(ctx, first, schema); err != nil {
		_, _ = first.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE")
		cancel()
		first.Close()
		second.Close()
		t.Fatalf("ensure schema: %v", err)
	}
	cancel()
	a := newStorage(first, schema, 30*time.Second, 2*time.Second, 10*time.Millisecond)
	b := newStorage(second, schema, 30*time.Second, 2*time.Second, 10*time.Millisecond)
	defer func() {
		cleanupIntegrationStorage(t, a, b)
		dropIntegrationSchema(t, dsn, schema)
	}()

	var active atomic.Int32
	var maximum atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			store := a
			if i%2 == 1 {
				store = b
			}
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			if err := store.Lock(ctx, "critical"); err != nil {
				t.Errorf("Lock: %v", err)
				return
			}
			current := active.Add(1)
			for {
				old := maximum.Load()
				if current <= old || maximum.CompareAndSwap(old, current) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
			active.Add(-1)
			if err := store.Unlock(context.Background(), "critical"); err != nil {
				t.Errorf("Unlock: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum critical-section concurrency = %d, want 1", got)
	}
}

func TestIntegrationLeaseExpiryAndOwnerGuard(t *testing.T) {
	dsn := integrationDSN(t)
	schema := randomIntegrationSchema()
	first := openIntegrationPool(t, dsn)
	second := openIntegrationPool(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := ensureSchema(ctx, first, schema); err != nil {
		_, _ = first.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE")
		cancel()
		first.Close()
		second.Close()
		t.Fatalf("ensure schema: %v", err)
	}
	cancel()
	a := newStorage(first, schema, 30*time.Second, 2*time.Second, 10*time.Millisecond)
	b := newStorage(second, schema, 30*time.Second, 2*time.Second, 10*time.Millisecond)
	defer func() {
		cleanupIntegrationStorage(t, a, b)
		dropIntegrationSchema(t, dsn, schema)
	}()

	if ok, err := a.TryLock(context.Background(), "lease"); err != nil || !ok {
		t.Fatalf("first TryLock: ok=%v err=%v", ok, err)
	}
	stopTestHeartbeat(a, "lease")
	// Force the first owner's lease stale, then let the second instance take it.
	if _, err := first.Exec(context.Background(), fmt.Sprintf("UPDATE %s SET expires_at = clock_timestamp() - interval '1 second' WHERE key = $1", a.locksTable), "lease"); err != nil {
		t.Fatalf("expire first lease: %v", err)
	}
	ok, err := b.TryLock(context.Background(), "lease")
	if err != nil || !ok {
		t.Fatalf("second TryLock after expiry: ok=%v err=%v", ok, err)
	}
	if err := a.Unlock(context.Background(), "lease"); !errors.Is(err, ErrLockLost) {
		t.Fatalf("stale owner Unlock error = %v, want ErrLockLost", err)
	}
	if err := b.RenewLockLease(context.Background(), "lease", time.Second); err != nil {
		t.Fatalf("new owner lease was affected by stale Unlock: %v", err)
	}
	if err := b.Unlock(context.Background(), "lease"); err != nil {
		t.Fatalf("second Unlock: %v", err)
	}
}

func TestIntegrationHeartbeatRenewsLease(t *testing.T) {
	dsn := integrationDSN(t)
	schema := randomIntegrationSchema()
	first := openIntegrationPool(t, dsn)
	second := openIntegrationPool(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := ensureSchema(ctx, first, schema); err != nil {
		_, _ = first.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE")
		cancel()
		first.Close()
		second.Close()
		t.Fatalf("ensure schema: %v", err)
	}
	cancel()
	a := newStorage(first, schema, 30*time.Second, 15*time.Second, 10*time.Millisecond)
	b := newStorage(second, schema, 30*time.Second, 15*time.Second, 10*time.Millisecond)
	defer func() {
		cleanupIntegrationStorage(t, a, b)
		dropIntegrationSchema(t, dsn, schema)
	}()
	if ok, err := a.TryLock(context.Background(), "heartbeat"); err != nil || !ok {
		t.Fatalf("TryLock: ok=%v err=%v", ok, err)
	}
	time.Sleep(20 * time.Second)
	ok, err := b.TryLock(context.Background(), "heartbeat")
	if err != nil {
		t.Fatalf("second TryLock: %v", err)
	}
	if ok {
		t.Fatal("heartbeat did not keep the lease alive")
	}
	if err := a.Unlock(context.Background(), "heartbeat"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestIntegrationUnlockTriggersBestEffortReaper(t *testing.T) {
	dsn := integrationDSN(t)
	store, pool, schema := setupIntegrationStorage(t, 2*time.Second)
	defer func() {
		cleanupIntegrationStorage(t, store)
		dropIntegrationSchema(t, dsn, schema)
		_ = pool
	}()

	if _, err := pool.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (key, owner, expires_at) VALUES ($1, $2, clock_timestamp() - interval '1 second')", store.locksTable), "stale", uuid.New()); err != nil {
		t.Fatalf("insert stale lease: %v", err)
	}
	if ok, err := store.TryLock(context.Background(), "trigger"); err != nil || !ok {
		t.Fatalf("trigger TryLock: ok=%v err=%v", ok, err)
	}
	if err := store.Unlock(context.Background(), "trigger"); err != nil {
		t.Fatalf("trigger Unlock: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var exists bool
		if err := pool.QueryRow(context.Background(), fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE key = $1)", store.locksTable), "stale").Scan(&exists); err == nil && !exists {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("reaper did not remove the expired lease")
}

func TestIntegrationModuleProvisionAndCleanup(t *testing.T) {
	dsn := integrationDSN(t)
	schema := randomIntegrationSchema()
	defer dropIntegrationSchema(t, dsn, schema)
	module := &PostgresStorage{
		DatabaseURL:      dsn,
		Schema:           schema,
		MaxOpenConns:     3,
		MaxIdleConns:     1,
		ConnMaxLifetime:  caddy.Duration(5 * time.Minute),
		OperationTimeout: caddy.Duration(30 * time.Second),
		LockTTL:          caddy.Duration(2 * time.Second),
		LockPollInterval: caddy.Duration(20 * time.Millisecond),
	}
	ctx := caddy.Context{Context: context.Background()}
	if err := module.Provision(ctx); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	storage, err := module.CertMagicStorage()
	if err != nil {
		t.Fatalf("CertMagicStorage: %v", err)
	}
	if _, ok := storage.(*Storage); !ok {
		t.Fatalf("CertMagicStorage returned %T", storage)
	}
	if err := module.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	noCreate := false
	module = &PostgresStorage{
		DatabaseURL:      dsn,
		Schema:           schema,
		AutoCreate:       &noCreate,
		OperationTimeout: caddy.Duration(30 * time.Second),
		LockTTL:          caddy.Duration(2 * time.Second),
	}
	if err := module.Provision(ctx); err != nil {
		t.Fatalf("Provision with auto_create=false: %v", err)
	}
	if err := module.Cleanup(); err != nil {
		t.Fatalf("Cleanup after auto_create=false: %v", err)
	}
}

func stopTestHeartbeat(store *Storage, name string) {
	store.stateMu.Lock()
	reservation := store.reservations[name]
	store.stateMu.Unlock()
	if reservation != nil {
		reservation.stopHeartbeat()
	}
}

func equalStrings(got, want []string) bool {
	gotCopy := append([]string(nil), got...)
	wantCopy := append([]string(nil), want...)
	sort.Strings(gotCopy)
	sort.Strings(wantCopy)
	if len(gotCopy) != len(wantCopy) {
		return false
	}
	for i := range gotCopy {
		if gotCopy[i] != wantCopy[i] {
			return false
		}
	}
	return true
}
