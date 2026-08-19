package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps a database/sql connection with SQLite-specific configuration.
// It implements all repository interfaces and provides transaction support
// for multi-step writes with commit/rollback semantics.
type Store struct {
	db     *sql.DB
	mu     sync.RWMutex
	closed bool
}

// Open creates or opens a SQLite database at dbPath, applying WAL mode for
// concurrent reads and durable writes. The data directory is created if missing.
func Open(ctx context.Context, dbPath string) (*Store, error) {
	dir := filepath.Dir(dbPath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create data directory %s: %w", dir, err)
		}
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1) // SQLite serialises writes
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)

	s := &Store{db: db}
	if err := s.ping(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return s, nil
}

func (s *Store) ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.db.PingContext(ctx)
}

// PingContext checks that the database is reachable.
func (s *Store) PingContext(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// DB returns the underlying database handle for migration use.
func (s *Store) DB() *sql.DB { return s.db }

// Close closes the database connection.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

// BeginTx starts a serializable transaction. Callers must Commit or Rollback.
func (s *Store) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
}

// ApplyMigration executes a migration script and records its version. This is
// idempotent: re-applying the same version is a no-op.
func (s *Store) ApplyMigration(ctx context.Context, version int, script string) error {
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existing int
	err = tx.QueryRowContext(ctx, "SELECT version FROM schema_version WHERE version = ?", version).Scan(&existing)
	if err == nil {
		return tx.Rollback() // already applied
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("check schema version: %w", err)
	}

	if _, err := tx.ExecContext(ctx, script); err != nil {
		return fmt.Errorf("execute migration %d: %w", version, err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
		version, time.Now().Unix()); err != nil {
		return fmt.Errorf("record schema version %d: %w", version, err)
	}
	return tx.Commit()
}

// SchemaVersion returns the highest applied migration version, or 0 if none.
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_version").Scan(&v)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("query schema version: %w", err)
	}
	return v, nil
}

// DataDirWritable checks that the data directory is writable by creating
// and removing a temporary file.
func (s *Store) DataDirWritable(dir string) error {
	probe := filepath.Join(dir, ".writeprobe")
	if err := os.WriteFile(probe, []byte("ok"), 0o644); err != nil {
		return fmt.Errorf("data directory not writable: %w", err)
	}
	os.Remove(probe)
	return nil
}

// AllRepositories returns a Repositories aggregate wired to this store.
func (s *Store) AllRepositories() *Repositories {
	return &Repositories{
		Enterprises:  &enterpriseRepo{store: s},
		Changes:      &changeRepo{store: s},
		Dispatch:     &dispatchRepo{store: s},
		EventLog:     &eventLogRepo{store: s},
		Subscribers:  &subscriberRepo{store: s},
		Audit:        &auditRepo{store: s},
		DeadLetters:  &deadLetterRepo{store: s},
		Compensation: &compensationRepo{store: s},
		Traces:       &traceRepo{store: s},
	}
}
