package store

import (
	"context"
	_ "embed"
	"fmt"
)

//go:embed schema.sql
var schemaSQL string

// Migrate applies all pending migrations to the store. It is safe to call
// multiple times; already-applied migrations are skipped.
func (s *Store) Migrate(ctx context.Context) error {
	// Ensure the version tracking table exists before querying it.
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_version (
			version    INTEGER PRIMARY KEY,
			applied_at INTEGER NOT NULL
		)`); err != nil {
		return fmt.Errorf("create schema_version table: %w", err)
	}
	return s.ApplyMigration(ctx, 1, schemaSQL)
}

// EnsureSchema is a convenience that runs migrations and returns the
// resulting schema version.
func (s *Store) EnsureSchema(ctx context.Context) (int, error) {
	if err := s.Migrate(ctx); err != nil {
		return 0, fmt.Errorf("migrate: %w", err)
	}
	return s.SchemaVersion(ctx)
}
