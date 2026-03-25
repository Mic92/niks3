package hook

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	// Pure-Go SQLite driver (no CGO required).
	_ "modernc.org/sqlite"
)

// Queue wraps a SQLite database for persisting store paths that need uploading.
type Queue struct {
	db *sql.DB
}

// OpenQueue opens (or creates) the SQLite database at dbPath.
// Parent directories are created automatically.
func OpenQueue(dbPath string) (*Queue, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("creating database directory: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	ctx := context.Background()

	// WAL mode for better concurrent read/write performance.
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("setting WAL mode: %w", err)
	}

	// Busy timeout so concurrent access retries instead of failing immediately.
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("setting busy timeout: %w", err)
	}

	// Create the queue table.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS upload_queue (
			store_path TEXT PRIMARY KEY,
			created_at DATETIME NOT NULL DEFAULT (datetime('now'))
		)
	`); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("creating table: %w", err)
	}

	return &Queue{db: db}, nil
}

// Enqueue inserts store paths into the queue, silently ignoring duplicates.
func (q *Queue) Enqueue(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	ctx := context.Background()

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, "INSERT OR IGNORE INTO upload_queue (store_path) VALUES (?)")
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}

	defer func() { _ = stmt.Close() }()

	for _, p := range paths {
		if _, err := stmt.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("inserting path %q: %w", p, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// FetchBatch returns up to batchSize of the oldest queued store paths.
func (q *Queue) FetchBatch(batchSize int) ([]string, error) {
	rows, err := q.db.QueryContext(
		context.Background(),
		"SELECT store_path FROM upload_queue ORDER BY created_at LIMIT ?",
		batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("querying batch: %w", err)
	}

	defer func() { _ = rows.Close() }()

	var paths []string

	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		paths = append(paths, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return paths, nil
}

// Remove deletes the given store paths from the queue.
func (q *Queue) Remove(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	// Build a single DELETE ... WHERE store_path IN (...) for efficiency.
	placeholders := make([]string, len(paths))
	args := make([]any, len(paths))

	for i, p := range paths {
		placeholders[i] = "?"
		args[i] = p
	}

	query := "DELETE FROM upload_queue WHERE store_path IN (" + strings.Join(placeholders, ",") + ")" //nolint:gosec // placeholders are all "?", no injection
	if _, err := q.db.ExecContext(context.Background(), query, args...); err != nil {
		return fmt.Errorf("deleting paths: %w", err)
	}

	return nil
}

// Count returns the number of paths currently in the queue.
func (q *Queue) Count() (int, error) {
	var count int
	if err := q.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM upload_queue").Scan(&count); err != nil {
		return 0, fmt.Errorf("counting queue: %w", err)
	}

	return count, nil
}

// Close closes the underlying database connection.
func (q *Queue) Close() error {
	if err := q.db.Close(); err != nil {
		return fmt.Errorf("closing database: %w", err)
	}

	return nil
}
