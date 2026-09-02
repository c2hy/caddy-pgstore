package caddypgstore

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const (
	defaultSchema           = "public"
	defaultMaxOpenConns     = 10
	defaultMaxIdleConns     = 2
	defaultConnMaxLifetime  = 30 * time.Minute
	defaultOperationTimeout = 10 * time.Second
	defaultLockTTL          = 5 * time.Minute
	defaultLockPollInterval = time.Second
)

// PostgresStorage is the Caddy module that exposes PostgreSQL as CertMagic storage.
type PostgresStorage struct {
	DatabaseURL      string         `json:"database_url,omitempty"`
	Schema           string         `json:"schema,omitempty"`
	AutoCreate       *bool          `json:"auto_create,omitempty"`
	MaxOpenConns     int            `json:"max_open_conns,omitempty"`
	MaxIdleConns     int            `json:"max_idle_conns,omitempty"`
	ConnMaxLifetime  caddy.Duration `json:"conn_max_lifetime,omitempty"`
	OperationTimeout caddy.Duration `json:"operation_timeout,omitempty"`
	LockTTL          caddy.Duration `json:"lock_ttl,omitempty"`
	LockPollInterval caddy.Duration `json:"lock_poll_interval,omitempty"`

	pool  *pgxpool.Pool
	store *Storage
}

func (PostgresStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.postgres",
		New: func() caddy.Module {
			return new(PostgresStorage)
		},
	}
}

func (p *PostgresStorage) Validate() error {
	url := p.DatabaseURL
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		return fmt.Errorf("database_url is required (or set DATABASE_URL)")
	}
	if _, err := pgxpool.ParseConfig(url); err != nil {
		return fmt.Errorf("invalid database_url: %w", err)
	}
	schema := p.Schema
	if schema == "" {
		schema = defaultSchema
	}
	if err := validateSchemaName(schema); err != nil {
		return err
	}
	maxOpen := p.MaxOpenConns
	if maxOpen == 0 {
		maxOpen = defaultMaxOpenConns
	}
	maxIdle := p.MaxIdleConns
	if maxIdle == 0 {
		maxIdle = defaultMaxIdleConns
	}
	if maxOpen < 1 {
		return fmt.Errorf("max_open_conns must be positive")
	}
	if maxIdle < 0 || maxIdle > maxOpen {
		return fmt.Errorf("max_idle_conns must be between 0 and max_open_conns")
	}
	if durationOrDefault(p.ConnMaxLifetime, defaultConnMaxLifetime) <= 0 {
		return fmt.Errorf("conn_max_lifetime must be positive")
	}
	if durationOrDefault(p.OperationTimeout, defaultOperationTimeout) <= 0 {
		return fmt.Errorf("operation_timeout must be positive")
	}
	if durationOrDefault(p.LockTTL, defaultLockTTL) <= 0 {
		return fmt.Errorf("lock_ttl must be positive")
	}
	if durationOrDefault(p.LockPollInterval, defaultLockPollInterval) <= 0 {
		return fmt.Errorf("lock_poll_interval must be positive")
	}
	return nil
}

func (p *PostgresStorage) Provision(ctx caddy.Context) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.DatabaseURL == "" {
		p.DatabaseURL = os.Getenv("DATABASE_URL")
	}
	if p.Schema == "" {
		p.Schema = defaultSchema
	}
	if p.MaxOpenConns == 0 {
		p.MaxOpenConns = defaultMaxOpenConns
	}
	if p.MaxIdleConns == 0 {
		p.MaxIdleConns = defaultMaxIdleConns
	}
	if p.ConnMaxLifetime == 0 {
		p.ConnMaxLifetime = caddy.Duration(defaultConnMaxLifetime)
	}
	if p.OperationTimeout == 0 {
		p.OperationTimeout = caddy.Duration(defaultOperationTimeout)
	}
	if p.LockTTL == 0 {
		p.LockTTL = caddy.Duration(defaultLockTTL)
	}
	if p.LockPollInterval == 0 {
		p.LockPollInterval = caddy.Duration(defaultLockPollInterval)
	}

	poolConfig, err := pgxpool.ParseConfig(p.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parsing database_url: %w", err)
	}
	poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	poolConfig.MaxConns = int32(p.MaxOpenConns)
	poolConfig.MinConns = 0
	poolConfig.MaxConnLifetime = time.Duration(p.ConnMaxLifetime)
	// pgxpool has no MaxIdleConns field like database/sql. AfterRelease is
	// invoked asynchronously just before an idle connection is returned, so
	// use the configured limit as a best-effort cap while retaining pgxpool's
	// normal health checks and lifetime handling.
	maxIdle := p.MaxIdleConns
	var poolRef atomic.Pointer[pgxpool.Pool]
	poolConfig.AfterRelease = func(_ *pgx.Conn) bool {
		pool := poolRef.Load()
		if pool == nil {
			return true
		}
		return int(pool.Stat().IdleConns()) < maxIdle
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("creating PostgreSQL connection pool: %w", err)
	}
	poolRef.Store(pool)

	pingCtx, pingCancel := context.WithTimeout(ctx, time.Duration(p.OperationTimeout))
	if err := pool.Ping(pingCtx); err != nil {
		pingCancel()
		pool.Close()
		return fmt.Errorf("pinging PostgreSQL: %w", err)
	}
	pingCancel()
	schemaCtx, schemaCancel := context.WithTimeout(ctx, time.Duration(p.OperationTimeout))
	defer schemaCancel()
	autoCreate := true
	if p.AutoCreate != nil {
		autoCreate = *p.AutoCreate
	}
	if autoCreate {
		if err := ensureSchema(schemaCtx, pool, p.Schema); err != nil {
			pool.Close()
			return err
		}
	} else if err := checkSchema(schemaCtx, pool, p.Schema); err != nil {
		pool.Close()
		return err
	}

	p.pool = pool
	p.store = newStorageWithContext(ctx, pool, p.Schema, time.Duration(p.OperationTimeout), time.Duration(p.LockTTL), time.Duration(p.LockPollInterval))
	p.store.logger = ctx.Logger(p)
	if p.store.logger == nil {
		p.store.logger = zap.NewNop()
	}
	return nil
}

func (p *PostgresStorage) CertMagicStorage() (certmagic.Storage, error) {
	if p.store == nil {
		return nil, fmt.Errorf("PostgreSQL storage has not been provisioned")
	}
	return p.store, nil
}

func (p *PostgresStorage) Cleanup() error {
	if p.store != nil {
		p.store.close()
		p.store = nil
	} else if p.pool != nil {
		p.pool.Close()
	}
	p.pool = nil
	return nil
}

func init() {
	caddy.RegisterModule(PostgresStorage{})
}

func durationOrDefault(value caddy.Duration, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return time.Duration(value)
}

var (
	_ caddy.Module           = (*PostgresStorage)(nil)
	_ caddy.Provisioner      = (*PostgresStorage)(nil)
	_ caddy.Validator        = (*PostgresStorage)(nil)
	_ caddy.CleanerUpper     = (*PostgresStorage)(nil)
	_ caddy.StorageConverter = (*PostgresStorage)(nil)
)
