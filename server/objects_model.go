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
	markedCount *int,
	gracePeriod int32,
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

	*markedCount = int(marked)

	// Then, get objects ready for deletion (marked > gracePeriod ago)
	for {
		objs, err := queries.GetObjectsReadyForDeletion(ctx, pg.GetObjectsReadyForDeletionParams{
			GracePeriodSeconds: gracePeriod,
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

		for _, obj := range objs {
			objectCh <- minio.ObjectInfo{Key: obj}
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

func (s *Service) removeS3Objects(ctx context.Context,
	objectCh <-chan minio.ObjectInfo,
	stats *ObjectCleanupStats,
) ([]error, []error) {
	opts := minio.RemoveObjectsOptions{GovernanceBypass: false}
	failedKeys := make([]string, 0, DeletionBatchSize)
	deletedKeys := make([]string, 0, DeletionBatchSize)

	queries := pg.New(s.Pool)

	var s3Errors, batchErrors []error

	for result := range s.MinioClient.RemoveObjectsWithResult(ctx, s.Bucket, objectCh, opts) {
		if result.Err != nil {
			// If object doesn't exist in S3, treat it as successfully deleted
			// to maintain consistency between S3 and database
			if minio.ToErrorResponse(result.Err).Code == "NoSuchKey" {
				var err error

				deletedKeys, err = handleDeletedObject(ctx, result.ObjectName, deletedKeys, queries)
				if err != nil {
					batchErrors = append(batchErrors, err)
				}

				stats.DeletedCount++

				continue
			}

			var (
				newS3Errors []error
				err         error
			)

			failedKeys, newS3Errors, err = handleFailedObject(ctx, result.ObjectName, result.Err, failedKeys, queries)

			s3Errors = append(s3Errors, newS3Errors...)
			if err != nil {
				batchErrors = append(batchErrors, err)
			}

			stats.FailedCount++

			continue
		}

		var err error

		deletedKeys, err = handleDeletedObject(ctx, result.ObjectName, deletedKeys, queries)
		if err != nil {
			batchErrors = append(batchErrors, err)
		}

		stats.DeletedCount++
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

func (s *Service) cleanupOrphanObjects(ctx context.Context, gracePeriod int32) (*ObjectCleanupStats, error) {
	// limit channel size to 1000, as minio limits to 1000 in one request
	objectCh := make(chan minio.ObjectInfo, DeletionBatchSize)

	stats := &ObjectCleanupStats{}

	var queryErr error

	go s.getObjectsForDeletion(ctx, objectCh, &queryErr, &stats.MarkedCount, gracePeriod)

	s3Errs, batchErrs := s.removeS3Objects(ctx, objectCh, stats)

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
