package caddypgstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestValidateStorageKey(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		allowEmpty bool
		wantError  bool
	}{
		{name: "simple", key: "simple"},
		{name: "nested/key", key: "nested/key"},
		{name: "empty allowed", key: "", allowEmpty: true},
		{name: "empty rejected", key: "", wantError: true},
		{name: "/leading", key: "/leading", wantError: true},
		{name: "trailing/", key: "trailing/", wantError: true},
		{name: "// is preserved", key: "a//b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateStorageKey(tt.key, tt.allowEmpty)
			if (err != nil) != tt.wantError {
				t.Fatalf("validateStorageKey(%q, %v) error = %v, wantError %v", tt.key, tt.allowEmpty, err, tt.wantError)
			}
		})
	}
}

func TestReservationSerializesLocalLockCalls(t *testing.T) {
	s := newStorage(nil, "public", time.Second, time.Second, time.Millisecond)
	defer s.close()

	first, ok, err := s.reserve(context.Background(), "same", false)
	if err != nil || !ok {
		t.Fatalf("first reserve: ok=%v err=%v", ok, err)
	}
	secondDone := make(chan struct{})
	var second *lockReservation
	go func() {
		var err error
		second, _, err = s.reserve(context.Background(), "same", false)
		if err != nil {
			t.Errorf("second reserve: %v", err)
		}
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("second local reservation bypassed first reservation")
	case <-time.After(25 * time.Millisecond):
	}
	s.removeReservation(first)
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second local reservation did not proceed after release")
	}
	if second == nil {
		t.Fatal("second reservation is nil")
	}
	s.removeReservation(second)
}

func TestTryReservationDoesNotBlock(t *testing.T) {
	s := newStorage(nil, "public", time.Second, time.Second, time.Millisecond)
	defer s.close()
	first, ok, err := s.reserve(context.Background(), "same", false)
	if err != nil || !ok {
		t.Fatalf("first reserve: ok=%v err=%v", ok, err)
	}
	defer s.removeReservation(first)
	started := time.Now()
	_, ok, err = s.reserve(context.Background(), "same", true)
	if err != nil || ok {
		t.Fatalf("try reserve: ok=%v err=%v", ok, err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("try reserve blocked for %s", elapsed)
	}
}

func TestReservationCancellation(t *testing.T) {
	s := newStorage(nil, "public", time.Second, time.Second, time.Millisecond)
	defer s.close()
	first, ok, err := s.reserve(context.Background(), "same", false)
	if err != nil || !ok {
		t.Fatalf("first reserve: ok=%v err=%v", ok, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, err = s.reserve(ctx, "same", false)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reserve error = %v, want deadline exceeded", err)
	}
	s.removeReservation(first)
}

func TestCaddyfileUnmarshal(t *testing.T) {
	d := caddyfile.NewTestDispenser(`postgres {
		database_url https://example.invalid
		schema tenant_a
		auto_create false
		max_open_conns 7
		max_idle_conns 3
		conn_max_lifetime 2m
		operation_timeout 4s
		lock_ttl 30s
		lock_poll_interval 250ms
	}`)
	var p PostgresStorage
	if err := p.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("UnmarshalCaddyfile: %v", err)
	}
	if p.DatabaseURL != "https://example.invalid" || p.Schema != "tenant_a" || p.AutoCreate == nil || *p.AutoCreate {
		t.Fatalf("unexpected parsed configuration: %+v", p)
	}
	if p.MaxOpenConns != 7 || p.MaxIdleConns != 3 || time.Duration(p.ConnMaxLifetime) != 2*time.Minute || time.Duration(p.OperationTimeout) != 4*time.Second || time.Duration(p.LockTTL) != 30*time.Second || time.Duration(p.LockPollInterval) != 250*time.Millisecond {
		t.Fatalf("unexpected duration/connection configuration: %+v", p)
	}
}

func TestModuleRegistration(t *testing.T) {
	info, err := caddy.GetModule("caddy.storage.postgres")
	if err != nil {
		t.Fatalf("GetModule: %v", err)
	}
	if info.New == nil {
		t.Fatal("registered module has no constructor")
	}
}

func TestValidateUsesDatabaseURLEnvironment(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://example.invalid/test")
	p := PostgresStorage{}
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate with DATABASE_URL: %v", err)
	}
	t.Setenv("DATABASE_URL", "")
	missing := PostgresStorage{}
	if err := missing.Validate(); err == nil {
		t.Fatal("Validate should require database_url when DATABASE_URL is empty")
	}
}

func TestBackgroundStartAndCloseRace(t *testing.T) {
	s := newStorage(nil, "public", time.Second, time.Second, time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.startBackground(func(ctx context.Context) { <-ctx.Done() })
		}()
	}
	s.close()
	wg.Wait()
	if !s.isClosing() {
		t.Fatal("storage should be marked closing")
	}
}
