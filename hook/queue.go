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

	// Pragmas go in the DSN so they apply to every pooled connection; a one-off
	// ExecContext only configures the single connection serving that call,
	// leaving other connections without busy_timeout and failing with SQLITE_BUSY.
	dsn := "file:" + dbPath + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	ctx := context.Background()

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

// FetchBatch returns up to batchSize store paths from the head of the queue.
// rowid rather than created_at (second resolution) gives the strict order
// Retry relies on.
func (q *Queue) FetchBatch(batchSize int) ([]string, error) {
	rows, err := q.db.QueryContext(
		context.Background(),
		"SELECT store_path FROM upload_queue ORDER BY rowid LIMIT ?",
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

// removeChunk stays well below SQLite's bound parameter limit (32766); the
// worker removes whole closures, which can be larger than that.
const removeChunk = 1000

// Remove deletes the given store paths from the queue.
func (q *Queue) Remove(paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	ctx := context.Background()

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	for len(paths) > 0 {
		n := min(len(paths), removeChunk)
		chunk := paths[:n]
		paths = paths[n:]

		args := make([]any, n)
		for i, p := range chunk {
			args[i] = p
		}

		query := "DELETE FROM upload_queue WHERE store_path IN (?" + strings.Repeat(",?", n-1) + ")" //nolint:gosec // only "?" placeholders
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("deleting paths: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// Retry moves paths whose upload failed to the back of the queue so that one
// bad path cannot block the head. As a consequence everything already tried
// sorts behind everything untried.
func (q *Queue) Retry(paths []string) error {
	ctx := context.Background()

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	for _, p := range paths {
		if _, err := tx.ExecContext(ctx,
			"UPDATE upload_queue SET rowid = (SELECT MAX(rowid) FROM upload_queue) + 1 WHERE store_path = ?", p,
		); err != nil {
			return fmt.Errorf("requeueing path %q: %w", p, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
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
