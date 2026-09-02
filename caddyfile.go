package caddypgstore

import (
	"fmt"
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

// UnmarshalCaddyfile parses the postgres storage module.
//
// Syntax:
//
//	storage postgres {
//	    database_url {$DATABASE_URL}
//	    schema public
//	    auto_create true
//	    max_open_conns 10
//	    max_idle_conns 2
//	    conn_max_lifetime 30m
//	    operation_timeout 10s
//	    lock_ttl 5m
//	    lock_poll_interval 1s
//	}
func (p *PostgresStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()
	if d.NextArg() {
		p.DatabaseURL = d.Val()
		if d.NextArg() {
			return d.ArgErr()
		}
	}
	for nesting, ok := d.Nesting(), true; ok; {
		if !d.NextBlock(nesting) {
			break
		}
		switch d.Val() {
		case "database_url":
			if !d.NextArg() {
				return d.ArgErr()
			}
			p.DatabaseURL = d.Val()
			if d.NextArg() {
				return d.ArgErr()
			}
		case "schema":
			if !d.NextArg() {
				return d.ArgErr()
			}
			p.Schema = d.Val()
			if d.NextArg() {
				return d.ArgErr()
			}
		case "auto_create":
			var value string
			if !d.NextArg() {
				return d.ArgErr()
			}
			value = d.Val()
			parsed, err := parseBool(value)
			if err != nil {
				return d.Errf("auto_create: %v", err)
			}
			p.AutoCreate = &parsed
			if d.NextArg() {
				return d.ArgErr()
			}
		case "max_open_conns":
			if !d.NextArg() {
				return d.ArgErr()
			}
			value, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.Errf("max_open_conns: %v", err)
			}
			p.MaxOpenConns = value
			if d.NextArg() {
				return d.ArgErr()
			}
		case "max_idle_conns":
			if !d.NextArg() {
				return d.ArgErr()
			}
			value, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.Errf("max_idle_conns: %v", err)
			}
			p.MaxIdleConns = value
			if d.NextArg() {
				return d.ArgErr()
			}
		case "conn_max_lifetime":
			value, err := nextDuration(d)
			if err != nil {
				return err
			}
			p.ConnMaxLifetime = caddy.Duration(value)
		case "operation_timeout":
			value, err := nextDuration(d)
			if err != nil {
				return err
			}
			p.OperationTimeout = caddy.Duration(value)
		case "lock_ttl":
			value, err := nextDuration(d)
			if err != nil {
				return err
			}
			p.LockTTL = caddy.Duration(value)
		case "lock_poll_interval":
			value, err := nextDuration(d)
			if err != nil {
				return err
			}
			p.LockPollInterval = caddy.Duration(value)
		default:
			return d.Errf("unrecognized subdirective %q", d.Val())
		}
	}
	return nil
}

func nextDuration(d *caddyfile.Dispenser) (time.Duration, error) {
	if !d.NextArg() {
		return 0, d.ArgErr()
	}
	value, err := time.ParseDuration(d.Val())
	if err != nil {
		return 0, d.Errf("invalid duration %q: %v", d.Val(), err)
	}
	if d.NextArg() {
		return 0, d.ArgErr()
	}
	return value, nil
}

func parseBool(value string) (bool, error) {
	switch value {
	case "true", "yes", "on":
		return true, nil
	case "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("expected true or false, got %q", value)
	}
}

var _ caddyfile.Unmarshaler = (*PostgresStorage)(nil)
