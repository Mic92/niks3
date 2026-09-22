package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Mic92/niks3/server/pg"
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

// removeS3Batch deletes one page of keys from S3 and reconciles the database:
// rows of deleted (or already absent) objects are removed, rows whose S3
// delete failed are marked active again so the next mark phase retries them.
//
// The page is handed to S3 in a single request right after it was selected,
// so the window in which a concurrent push can re-upload one of these keys
// before it is deleted is the latency of that request rather than the time
// the key would spend buffered behind a thousand others.
func (s *Service) removeS3Batch(ctx context.Context, keys []string, stats *ObjectCleanupStats, onProgress func(ObjectCleanupStats)) sweepBatchResult {
	objectCh := make(chan minio.ObjectInfo, len(keys))
	for _, key := range keys {
		objectCh <- minio.ObjectInfo{Key: key}
	}

	close(objectCh)

	opts := minio.RemoveObjectsOptions{GovernanceBypass: false}
	failedKeys := make([]string, 0, len(keys))
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
			failedKeys = append(failedKeys, res.ObjectName)
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

	queries := pg.New(s.Pool)

	if len(failedKeys) > 0 {
		if err := queries.MarkObjectsAsActive(ctx, failedKeys); err != nil {
			slog.Error("failed to mark objects active", "error", err, "count", len(failedKeys))
			result.batchErrors = append(result.batchErrors, fmt.Errorf("mark %d objects active: %w", len(failedKeys), err))
		}
	}

	if len(deletedKeys) > 0 {
		// Conditional on the tombstone: a push that re-uploaded and registered
		// one of these keys after the S3 delete owns the row now.
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
// Deletion runs page by page: select up to DeletionBatchSize tombstoned keys
// that are past the grace period and not pending in any closure, delete them
// from S3, reconcile the database, repeat. Pagination is keyset on the key
// because the rows are deleted underneath the scan.
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

		keys, err := queries.GetObjectsReadyForDeletion(ctx, pg.GetObjectsReadyForDeletionParams{
			GracePeriodSeconds: gracePeriod,
			AfterKey:           afterKey,
			LimitCount:         DeletionBatchSize,
		})
		if err != nil {
			return stats, fmt.Errorf("failed to get objects ready for deletion: %w", err)
		}

		if len(keys) == 0 {
			break
		}

		afterKey = keys[len(keys)-1]

		res := s.removeS3Batch(ctx, keys, stats, onProgress)
		s3Errs = append(s3Errs, res.s3Errors...)
		batchErrs = append(batchErrs, res.batchErrors...)
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
