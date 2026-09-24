package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
)

const (
	DeletionBatchSize = 1000
)

// ObjectCleanupStats contains statistics about object cleanup operations.
type ObjectCleanupStats struct {
	MarkedCount  int
	DeletedCount int
	FailedCount  int
}

// sweepBatchResult is the outcome of deleting one page of tombstoned objects.
type sweepBatchResult struct {
	s3Errors    []error
	batchErrors []error
}

// sweepPage deletes one page of tombstoned objects past the grace period,
// starting after afterKey. It returns the last key it selected, "" once
// nothing is left, and the outcome of the page.
//
// The page is one transaction that holds its rows FOR UPDATE from selecting
// them until their S3 delete is done and the rows are gone. A push about to
// offer one of these keys for upload waits for that lock
// (LockTombstonedObjects), so its upload lands after the delete instead of
// being lost to it. A push whose pending rows committed after the page's
// snapshot, and whose lock came and went before the sweep's, is caught by
// asking pending_objects again once the rows are held: such a key is left
// alone, its closure protects it.
func (s *Service) sweepPage(
	ctx context.Context,
	gracePeriod int32,
	afterKey string,
	stats *ObjectCleanupStats,
	onProgress func(ObjectCleanupStats),
) (string, sweepBatchResult, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", sweepBatchResult{}, fmt.Errorf("failed to begin sweep transaction: %w", err)
	}

	defer func() {
		// A no-op once the page committed.
		if err := tx.Rollback(context.WithoutCancel(ctx)); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			slog.Error("failed to roll back sweep transaction", "error", err)
		}
	}()

	queries := pg.New(tx)

	keys, err := queries.GetObjectsReadyForDeletion(ctx, pg.GetObjectsReadyForDeletionParams{
		GracePeriodSeconds: gracePeriod,
		AfterKey:           afterKey,
		LimitCount:         DeletionBatchSize,
	})
	if err != nil {
		return "", sweepBatchResult{}, fmt.Errorf("failed to get objects ready for deletion: %w", err)
	}

	if len(keys) == 0 {
		return "", sweepBatchResult{}, nil
	}

	unpending, err := queries.GetKeysNotPending(ctx, keys)
	if err != nil {
		return "", sweepBatchResult{}, fmt.Errorf("failed to recheck pending objects: %w", err)
	}

	var res sweepBatchResult
	if len(unpending) > 0 {
		res = s.removeS3Batch(ctx, queries, unpending, stats, onProgress)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", res, fmt.Errorf("failed to commit sweep page: %w", err)
	}

	return keys[len(keys)-1], res, nil
}

// removeS3Batch deletes one page of keys from S3 and reconciles the database
// through queries, the page's transaction: rows of deleted (or already
// absent) objects are removed, rows whose S3 delete failed keep their
// tombstone so the next sweep retries them. A failure does not mean the
// object is still there: when the multi-object delete fails as a request (a
// 5xx or a connection lost after S3 acted on it) minio reports every key of
// the page with that error.
func (s *Service) removeS3Batch(
	ctx context.Context,
	queries *pg.Queries,
	keys []string,
	stats *ObjectCleanupStats,
	onProgress func(ObjectCleanupStats),
) sweepBatchResult {
	objectCh := make(chan minio.ObjectInfo, len(keys))
	for _, key := range keys {
		objectCh <- minio.ObjectInfo{Key: key}
	}

	close(objectCh)

	if s.testHookBeforeSweepDelete != nil {
		s.testHookBeforeSweepDelete()
	}

	opts := minio.RemoveObjectsOptions{GovernanceBypass: false}
	deletedKeys := make([]string, 0, len(keys))

	var result sweepBatchResult

	for res := range s.MinioClient.RemoveObjectsWithResult(ctx, s.Bucket, objectCh, opts) {
		switch {
		case res.Err == nil:
			s.S3RateLimiter.RecordSuccess()
		case isRateLimitError(res.Err):
			// Track rate limit errors to enable adaptive rate limiting
			s.S3RateLimiter.RecordThrottle()
		}

		if res.Err != nil && minio.ToErrorResponse(res.Err).Code != minio.NoSuchKey {
			slog.Error("failed to remove object", "object", res.ObjectName, "error", res.Err)
			result.s3Errors = append(result.s3Errors, fmt.Errorf("failed to remove object %q: %w", res.ObjectName, res.Err))
			stats.FailedCount++
		} else {
			// Deleted, or already absent from S3: either way the object is gone,
			// so drop the database row to keep S3 and the database consistent.
			deletedKeys = append(deletedKeys, res.ObjectName)
			stats.DeletedCount++
		}

		if onProgress != nil {
			onProgress(*stats)
		}
	}

	if len(deletedKeys) > 0 {
		if err := queries.DeleteTombstonedObjects(ctx, deletedKeys); err != nil {
			slog.Error("failed to delete object rows", "error", err, "count", len(deletedKeys))
			result.batchErrors = append(result.batchErrors, fmt.Errorf("delete %d object rows: %w", len(deletedKeys), err))
		}
	}

	return result
}

// cleanupOrphanObjects marks unreachable objects and deletes them from S3.
// When onProgress is non-nil it is called after every individual
// mark/delete/fail so callers can expose live counters.
//
// Deletion runs page by page (sweepPage): select up to DeletionBatchSize
// tombstoned keys that are past the grace period and not pending in any
// closure, delete them from S3, reconcile the database, repeat. Pagination is
// keyset on the key because the rows are deleted underneath the scan.
func (s *Service) cleanupOrphanObjects(ctx context.Context, gracePeriod int32, onProgress func(ObjectCleanupStats)) (*ObjectCleanupStats, error) {
	queries := pg.New(s.Pool)
	stats := &ObjectCleanupStats{}

	marked, err := queries.MarkStaleObjects(ctx)
	if err != nil {
		return stats, fmt.Errorf("failed to mark stale objects: %w", err)
	}

	stats.MarkedCount = int(marked)

	if onProgress != nil {
		onProgress(*stats)
	}

	var s3Errs, batchErrs []error

	afterKey := ""

	for {
		if err := ctx.Err(); err != nil {
			return stats, err //nolint:wrapcheck // context error is the result
		}

		lastKey, res, err := s.sweepPage(ctx, gracePeriod, afterKey, stats, onProgress)
		s3Errs = append(s3Errs, res.s3Errors...)
		batchErrs = append(batchErrs, res.batchErrors...)

		if err != nil {
			return stats, err
		}

		if lastKey == "" {
			break
		}

		afterKey = lastKey
	}

	// Prioritize batch errors (database operations) over S3 errors
	// as they're more critical for data integrity
	if len(batchErrs) > 0 {
		batchErr := errors.Join(batchErrs...)
		if len(s3Errs) > 0 {
			s3Err := errors.Join(s3Errs...)

			return stats, fmt.Errorf("%d batch operation failures: %w (also %d S3 failures: %w)",
				len(batchErrs), batchErr, len(s3Errs), s3Err)
		}

		return stats, fmt.Errorf("%d batch operation failures: %w", len(batchErrs), batchErr)
	}

	if len(s3Errs) > 0 {
		return stats, fmt.Errorf("%d S3 failures: %w", len(s3Errs), errors.Join(s3Errs...))
	}

	return stats, nil
}
