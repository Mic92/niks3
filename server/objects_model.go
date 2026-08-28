package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/minio/minio-go/v7"
)

const (
	DeletionBatchSize = 1000
	markStaleRetries  = 20
)

// ObjectCleanupStats contains statistics about object cleanup operations.
type ObjectCleanupStats struct {
	MarkedCount  int
	DeletedCount int
	FailedCount  int
}

// markStaleObjects tombstones unreachable, non-pending objects. REPEATABLE
// READ turns a race with a closure commit into a retry instead of
// tombstoning a just-resurrected row.
func (s *Service) markStaleObjects(ctx context.Context) (int64, error) {
	for attempt := range markStaleRetries {
		n, err := s.markStaleObjectsOnce(ctx)
		if err == nil {
			return n, nil
		}

		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || (pgErr.Code != "40001" && pgErr.Code != "40P01") {
			return 0, err
		}

		slog.Info("mark phase conflicted with a push, retrying", "attempt", attempt+1, "code", pgErr.Code)
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}

	return 0, fmt.Errorf("mark phase gave up after %d serialization failures", markStaleRetries)
}

func (s *Service) markStaleObjectsOnce(ctx context.Context) (int64, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return 0, fmt.Errorf("begin mark tx: %w", err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	n, err := pg.New(tx).MarkStaleObjects(ctx)
	if err != nil {
		return 0, fmt.Errorf("mark stale objects: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit mark tx: %w", err)
	}

	return n, nil
}

// reapBatch locks a batch of expired tombstones, deletes them from S3 while
// holding the locks, and drops the rows that are gone from S3. Returns a
// keyset cursor, "" when done.
func (s *Service) reapBatch(ctx context.Context, cutoff time.Time, afterKey string, stats *ObjectCleanupStats) (string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin reap tx: %w", err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	commit := func() error {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit reap tx: %w", err)
		}

		return nil
	}

	queries := pg.New(tx)

	keys, err := queries.LockObjectsForDeletion(ctx, pg.LockObjectsForDeletionParams{
		Cutoff:     pgtype.Timestamp{Time: cutoff, Valid: true},
		AfterKey:   afterKey,
		LimitCount: DeletionBatchSize,
	})
	if err != nil {
		return "", fmt.Errorf("lock objects for deletion: %w", err)
	}

	if len(keys) == 0 {
		return "", commit()
	}

	lastKey := keys[len(keys)-1]

	pendingNow, err := queries.GetPendingKeysAmong(ctx, keys)
	if err != nil {
		return "", fmt.Errorf("recheck pending keys: %w", err)
	}

	skip := make(map[string]struct{}, len(pendingNow))
	for _, k := range pendingNow {
		skip[k] = struct{}{}
	}

	toDelete := make([]string, 0, len(keys))

	for _, k := range keys {
		if _, pending := skip[k]; !pending {
			toDelete = append(toDelete, k)
		}
	}

	deleted := s.deleteFromS3(ctx, toDelete, stats)

	if _, err := queries.DeleteTombstonedObjects(ctx, deleted); err != nil {
		return "", fmt.Errorf("delete object rows: %w", err)
	}

	if err := commit(); err != nil {
		return "", err
	}

	stats.DeletedCount += len(deleted)

	return lastKey, nil
}

// deleteFromS3 returns the keys that are gone from S3 afterwards.
func (s *Service) deleteFromS3(ctx context.Context, keys []string, stats *ObjectCleanupStats) []string {
	if len(keys) == 0 {
		return nil
	}

	if err := s.S3RateLimiter.Wait(ctx); err != nil {
		stats.FailedCount += len(keys)

		return nil
	}

	objectCh := make(chan minio.ObjectInfo, len(keys))
	for _, k := range keys {
		objectCh <- minio.ObjectInfo{Key: k}
	}

	close(objectCh)

	failed := make(map[string]struct{})

	for e := range s.MinioClient.RemoveObjects(ctx, s.Bucket, objectCh, minio.RemoveObjectsOptions{}) {
		if minio.ToErrorResponse(e.Err).Code == minio.NoSuchKey {
			continue
		}

		if isRateLimitError(e.Err) {
			s.S3RateLimiter.RecordThrottle()
		}

		// minio reports a request-level failure with an empty ObjectName.
		if e.ObjectName == "" {
			slog.Error("S3 multi-delete failed", "error", e.Err, "keys", len(keys))
			stats.FailedCount += len(keys)

			return nil
		}

		slog.Error("failed to delete object", "key", e.ObjectName, "error", e.Err)
		failed[e.ObjectName] = struct{}{}
	}

	if len(failed) == 0 {
		s.S3RateLimiter.RecordSuccess()
	}

	stats.FailedCount += len(failed)

	deleted := make([]string, 0, len(keys)-len(failed))

	for _, k := range keys {
		if _, bad := failed[k]; !bad {
			deleted = append(deleted, k)
		}
	}

	return deleted
}

// cleanupOrphanObjects marks, then reaps tombstones older than gracePeriod.
func (s *Service) cleanupOrphanObjects(ctx context.Context, gracePeriod time.Duration, onProgress func(ObjectCleanupStats)) (*ObjectCleanupStats, error) {
	stats := &ObjectCleanupStats{}

	notify := func() {
		if onProgress != nil {
			onProgress(*stats)
		}
	}

	marked, err := s.markStaleObjects(ctx)
	if err != nil {
		return stats, err
	}

	stats.MarkedCount = int(marked)

	notify()

	cutoff := time.Now().UTC().Add(-gracePeriod)
	afterKey := ""

	for {
		afterKey, err = s.reapBatch(ctx, cutoff, afterKey, stats)

		notify()

		if err != nil {
			return stats, err
		}

		if afterKey == "" {
			break
		}
	}

	if stats.FailedCount > 0 {
		return stats, fmt.Errorf("failed to delete %d objects from S3", stats.FailedCount)
	}

	return stats, nil
}
