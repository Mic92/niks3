package hook

import (
	"context"
	"fmt"
)

// HoldWrite starts a write transaction on the queue's own database, as a
// commit stuck on a slow fsync would hold it, and returns the function that
// commits it.
func (q *Queue) HoldWrite(ctx context.Context) (func() error, error) {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM upload_queue WHERE store_path = ''"); err != nil {
		_ = tx.Rollback()

		return nil, fmt.Errorf("taking the write lock: %w", err)
	}

	return tx.Commit, nil
}
