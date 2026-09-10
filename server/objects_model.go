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

func flushBatch(ctx context.Context, keys []string, operation func(context.Context, []string) error) ([]string, error) {
	if len(keys) == 0 {
		return keys, nil
	}

	if err := operation(ctx, keys); err != nil {
		slog.Error("batch operation failed", "error", err)
		// Return keys unchanged to allow retry
		return keys, err
	}

	// Only clear keys on success
	return keys[:0], nil
}

func (s *Service) getObjectsForDeletion(ctx context.Context,
	objectCh chan<- minio.ObjectInfo,
	queryErr *error,
	stats *ObjectCleanupStats,
	gracePeriod int32,
	onProgress func(ObjectCleanupStats),
) {
	defer close(objectCh)

	queries := pg.New(s.Pool)

	// First, mark stale objects and get count
	marked, err := queries.MarkStaleObjects(ctx)
	if err != nil {
		*queryErr = fmt.Errorf("failed to mark stale objects: %w", err)
		slog.Error("failed to mark stale objects", "error", err)

		return
	}

	stats.MarkedCount = int(marked)

	if onProgress != nil {
		onProgress(*stats)
	}

	// Then, get objects ready for deletion (marked > gracePeriod ago).
	// The consumer removes or re-activates rows only after a full batch, so
	// pages are keyed on the last key handed out. Re-running a plain LIMIT
	// query would return the previous page again while its batch is still
	// in flight and send the same keys to S3 twice.
	afterKey := ""

	for {
		objs, err := queries.GetObjectsReadyForDeletion(ctx, pg.GetObjectsReadyForDeletionParams{
			GracePeriodSeconds: gracePeriod,
			AfterKey:           afterKey,
			LimitCount:         DeletionBatchSize,
		})
		if err != nil {
			*queryErr = fmt.Errorf("failed to get objects ready for deletion: %w", err)
			slog.Error("failed to get objects ready for deletion", "error", err)

			break
		}

		if len(objs) == 0 {
			break
		}

		afterKey = objs[len(objs)-1]

		for _, obj := range objs {
			select {
			case objectCh <- minio.ObjectInfo{Key: obj}:
			case <-ctx.Done():
				*queryErr = ctx.Err()

				return
			}
		}
	}
}

// handleDeletedObject processes a successfully deleted object and flushes batch if needed.
func handleDeletedObject(ctx context.Context, objectName string, deletedKeys []string, queries *pg.Queries) ([]string, error) {
	deletedKeys = append(deletedKeys, objectName)

	if len(deletedKeys) >= DeletionBatchSize {
		var err error

		deletedKeys, err = flushBatch(ctx, deletedKeys, queries.DeleteObjects)

		return deletedKeys, err
	}

	return deletedKeys, nil
}

// handleFailedObject processes a failed deletion and flushes batch if needed.
func handleFailedObject(ctx context.Context, objectName string, resultErr error, failedKeys []string, queries *pg.Queries) ([]string, []error, error) {
	s3Errors := []error{fmt.Errorf("failed to remove object %q: %w", objectName, resultErr)}
	slog.Error("failed to remove object", "object", objectName, "error", resultErr)
	failedKeys = append(failedKeys, objectName)

	if len(failedKeys) >= DeletionBatchSize {
		var err error

		failedKeys, err = flushBatch(ctx, failedKeys, queries.MarkObjectsAsActive)

		return failedKeys, s3Errors, err
	}

	return failedKeys, s3Errors, nil
}

// removeObjectSingle deletes one key with DeleteObject after a batch delete
// reported it as failed. A batch error is reported for every key in the batch
// and says nothing about the individual key: some S3 implementations fail the
// whole DeleteObjects request when any key in it is missing while still
// deleting the others. The single delete answers per key, and a key that is
// already gone counts as deleted.
func (s *Service) removeObjectSingle(ctx context.Context, key string) error {
	if err := s.S3RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for rate limiter: %w", err)
	}

	err := s.MinioClient.RemoveObject(ctx, s.Bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		if isRateLimitError(err) {
			s.S3RateLimiter.RecordThrottle()
		}

		if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
			return nil
		}

		return fmt.Errorf("single delete after batch failure: %w", err)
	}

	s.S3RateLimiter.RecordSuccess()

	return nil
}

func (s *Service) removeS3Objects(ctx context.Context,
	objectCh <-chan minio.ObjectInfo,
	stats *ObjectCleanupStats,
	onProgress func(ObjectCleanupStats),
) ([]error, []error) {
	opts := minio.RemoveObjectsOptions{GovernanceBypass: false}
	failedKeys := make([]string, 0, DeletionBatchSize)
	deletedKeys := make([]string, 0, DeletionBatchSize)

	queries := pg.New(s.Pool)

	notifyProgress := func() {
		if onProgress != nil {
			onProgress(*stats)
		}
	}

	var s3Errors, batchErrors []error

	for result := range s.MinioClient.RemoveObjectsWithResult(ctx, s.Bucket, objectCh, opts) {
		removeErr := result.Err

		switch {
		case removeErr == nil:
			s.S3RateLimiter.RecordSuccess()
		case minio.ToErrorResponse(removeErr).Code == minio.NoSuchKey:
			// If object doesn't exist in S3, treat it as successfully deleted
			// to maintain consistency between S3 and database
			removeErr = nil
		default:
			// Track rate limit errors to enable adaptive rate limiting
			if isRateLimitError(removeErr) {
				s.S3RateLimiter.RecordThrottle()
			}

			slog.Debug("batch delete failed for object, retrying with a single delete",
				"object", result.ObjectName, "error", removeErr)

			removeErr = s.removeObjectSingle(ctx, result.ObjectName)
		}

		if removeErr != nil {
			var (
				newS3Errors []error
				err         error
			)

			failedKeys, newS3Errors, err = handleFailedObject(ctx, result.ObjectName, removeErr, failedKeys, queries)

			s3Errors = append(s3Errors, newS3Errors...)
			if err != nil {
				batchErrors = append(batchErrors, err)
			}

			stats.FailedCount++
			notifyProgress()

			continue
		}

		var err error

		deletedKeys, err = handleDeletedObject(ctx, result.ObjectName, deletedKeys, queries)
		if err != nil {
			batchErrors = append(batchErrors, err)
		}

		stats.DeletedCount++
		notifyProgress()
	}

	// Flush remaining batches
	if _, err := flushBatch(ctx, failedKeys, queries.MarkObjectsAsActive); err != nil {
		batchErrors = append(batchErrors, err)
	}

	if _, err := flushBatch(ctx, deletedKeys, queries.DeleteObjects); err != nil {
		batchErrors = append(batchErrors, err)
	}

	return s3Errors, batchErrors
}

// cleanupOrphanObjects marks unreachable objects and deletes them from S3.
// When onProgress is non-nil it is called after every individual
// mark/delete/fail so callers can expose live counters.
func (s *Service) cleanupOrphanObjects(ctx context.Context, gracePeriod int32, onProgress func(ObjectCleanupStats)) (*ObjectCleanupStats, error) {
	// limit channel size to 1000, as minio limits to 1000 in one request
	objectCh := make(chan minio.ObjectInfo, DeletionBatchSize)

	stats := &ObjectCleanupStats{}

	var queryErr error

	go s.getObjectsForDeletion(ctx, objectCh, &queryErr, stats, gracePeriod, onProgress)

	s3Errs, batchErrs := s.removeS3Objects(ctx, objectCh, stats, onProgress)

	if queryErr != nil {
		return stats, queryErr
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
