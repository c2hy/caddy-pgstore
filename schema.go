package caddypgstore

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"
)

var schemaNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateSchemaName(schema string) error {
	if !schemaNameRE.MatchString(schema) {
		return fmt.Errorf("invalid PostgreSQL schema name %q", schema)
	}
	return nil
}

func quoteIdentifier(identifier string) string {
	// Callers validate identifiers before they reach this function. Escaping here
	// keeps this helper safe if it is reused for a less restrictive identifier.
	return `"` + replaceQuotes(identifier) + `"`
}

func replaceQuotes(value string) string {
	result := make([]byte, 0, len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '"' {
			result = append(result, '"')
		}
		result = append(result, value[i])
	}
	return string(result)
}

func tableNames(schema string) (objects, locks string) {
	quotedSchema := quoteIdentifier(schema)
	return quotedSchema + "." + quoteIdentifier("certmagic_objects"),
		quotedSchema + "." + quoteIdentifier("certmagic_locks")
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	quotedSchema := quoteIdentifier(schema)
	objects, locks := tableNames(schema)
	statements := []string{
		"CREATE SCHEMA IF NOT EXISTS " + quotedSchema,
		"CREATE TABLE IF NOT EXISTS " + objects + " (" +
			"key text PRIMARY KEY, " +
			"value bytea NOT NULL, " +
			"modified_at timestamptz NOT NULL" +
			")",
		"CREATE TABLE IF NOT EXISTS " + locks + " (" +
			"key text PRIMARY KEY, " +
			"owner uuid NOT NULL, " +
			"expires_at timestamptz NOT NULL" +
			")",
		"CREATE INDEX IF NOT EXISTS " + quoteIdentifier("certmagic_locks_expires_at_idx") +
			" ON " + locks + " (expires_at)",
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("initializing PostgreSQL schema %q: %w", schema, err)
		}
	}
	return nil
}

func checkSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	const query = `
SELECT table_name
FROM information_schema.tables
WHERE table_schema = $1
  AND table_name = ANY($2::text[])
ORDER BY table_name`
	rows, err := pool.Query(ctx, query, schema, []string{"certmagic_objects", "certmagic_locks"})
	if err != nil {
		return fmt.Errorf("checking PostgreSQL schema %q: %w", schema, err)
	}
	defer rows.Close()

	found := make(map[string]struct{}, 2)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return fmt.Errorf("reading PostgreSQL schema check: %w", err)
		}
		found[table] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading PostgreSQL schema check: %w", err)
	}
	for _, table := range []string{"certmagic_objects", "certmagic_locks"} {
		if _, ok := found[table]; !ok {
			return fmt.Errorf("PostgreSQL schema %q is missing table %q", schema, table)
		}
	}

	const columnQuery = `
SELECT table_name, column_name, udt_name
FROM information_schema.columns
WHERE table_schema = $1
  AND table_name = ANY($2::text[])`
	columns, err := pool.Query(ctx, columnQuery, schema, []string{"certmagic_objects", "certmagic_locks"})
	if err != nil {
		return fmt.Errorf("checking PostgreSQL schema %q columns: %w", schema, err)
	}
	defer columns.Close()
	expected := map[string]map[string]string{
		"certmagic_objects": {
			"key":         "text",
			"value":       "bytea",
			"modified_at": "timestamptz",
		},
		"certmagic_locks": {
			"key":        "text",
			"owner":      "uuid",
			"expires_at": "timestamptz",
		},
	}
	seen := make(map[string]map[string]bool, len(expected))
	for columns.Next() {
		var table, column, dataType string
		if err := columns.Scan(&table, &column, &dataType); err != nil {
			return fmt.Errorf("reading PostgreSQL schema %q columns: %w", schema, err)
		}
		want, ok := expected[table][column]
		if !ok {
			continue
		}
		if dataType != want {
			return fmt.Errorf("PostgreSQL schema %q table %q column %q has type %q, want %q", schema, table, column, dataType, want)
		}
		if seen[table] == nil {
			seen[table] = make(map[string]bool)
		}
		seen[table][column] = true
	}
	if err := columns.Err(); err != nil {
		return fmt.Errorf("reading PostgreSQL schema %q columns: %w", schema, err)
	}
	for table, required := range expected {
		for column := range required {
			if !seen[table][column] {
				return fmt.Errorf("PostgreSQL schema %q table %q is missing column %q", schema, table, column)
			}
		}
	}
	return nil
}
