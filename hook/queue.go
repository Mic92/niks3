package hook

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	// Pure-Go SQLite driver (no CGO required).
	_ "modernc.org/sqlite"
)

// Queue wraps a SQLite database for persisting store paths that need uploading.
type Queue struct {
	db    *sql.DB
	mu    sync.Mutex
	owner *os.File
	roots string
}

// OpenQueue opens (or creates) the SQLite database at dbPath.
// Parent directories are created automatically. Only one owner may open a queue.
func OpenQueue(dbPath string) (*Queue, error) {
	return openQueue(dbPath, false)
}

// OpenQueueWithRoots retains queued outputs using native Nix garbage collector roots.
func OpenQueueWithRoots(dbPath string) (*Queue, error) {
	return openQueue(dbPath, true)
}

func openQueue(dbPath string, retain bool) (*Queue, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("creating database directory: %w", err)
	}

	dbPath, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, fmt.Errorf("resolving queue path: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(dbPath); err == nil {
		dbPath = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("resolving queue path: %w", err)
	} else {
		parent, err := filepath.EvalSymlinks(filepath.Dir(dbPath))
		if err != nil {
			return nil, fmt.Errorf("resolving queue directory: %w", err)
		}
		dbPath = filepath.Join(parent, filepath.Base(dbPath))
	}
	owner, err := os.OpenFile(dbPath+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening queue owner lock: %w", err)
	}
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = owner.Close()

		return nil, fmt.Errorf("queue already owned: %w", err)
	}
	success := false
	defer func() {
		if !success {
			_ = owner.Close()
		}
	}()

	// Pragmas go in the DSN so they apply to every pooled connection; a one-off
	// ExecContext only configures the single connection serving that call,
	// leaving other connections without busy_timeout and failing with SQLITE_BUSY.
	dsn := "file:" + dbPath + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Writers wait for the one connection in turn. With a pool SQLite polls
	// for the lock, and a writer that keeps losing fails with SQLITE_BUSY.
	db.SetMaxOpenConns(1)

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

	q := &Queue{db: db, owner: owner}
	if err := q.openRoots(dbPath, retain); err != nil {
		_ = db.Close()

		return nil, err
	}
	success = true

	return q, nil
}

// Enqueue inserts store paths into the queue, silently ignoring duplicates.
func (q *Queue) Enqueue(paths []string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.owner == nil {
		return errors.New("queue is closed")
	}
	err := q.retain(paths)
	if err == nil {
		err = q.enqueue(paths)
	}
	if err != nil {
		return errors.Join(err, q.releaseUnqueued(paths))
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
	q.mu.Lock()
	defer q.mu.Unlock()
	removed := paths
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

	return q.release(removed)
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
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.owner != nil {
		defer func() { _ = q.owner.Close(); q.owner = nil }()
	}
	if err := q.db.Close(); err != nil {
		return fmt.Errorf("closing database: %w", err)
	}

	return nil
}

func (q *Queue) enqueue(paths []string) error {
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
