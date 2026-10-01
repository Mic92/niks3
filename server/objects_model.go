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

// sweepBatch deletes one batch of tombstoned objects from S3 and then their
// rows. The rows stay locked from the query to the commit, so a push that
// wants to treat one of them as present waits and finds it gone. Keys whose
// S3 delete fails keep their tombstone for the next run. It returns the last
// key it looked at, or "" when nothing is left.
func (s *Service) sweepBatch(ctx context.Context, gracePeriod int32, afterKey string, stats *ObjectCleanupStats) (string, []error, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("begin sweep transaction: %w", err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	keys, err := pg.New(tx).LockObjectsReadyForDeletion(ctx, pg.LockObjectsReadyForDeletionParams{
		GracePeriodSeconds: gracePeriod,
		AfterKey:           afterKey,
		LimitCount:         DeletionBatchSize,
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to get objects ready for deletion: %w", err)
	}

	if len(keys) == 0 {
		return "", nil, nil
	}

	objectCh := make(chan minio.ObjectInfo, len(keys))
	for _, key := range keys {
		objectCh <- minio.ObjectInfo{Key: key}
	}

	close(objectCh)

	var (
		deleted []string
		s3Errs  []error
	)

	for result := range s.MinioClient.RemoveObjectsWithResult(ctx, s.Bucket, objectCh, minio.RemoveObjectsOptions{}) {
		switch {
		case result.Err == nil:
			s.S3RateLimiter.RecordSuccess()
		case isRateLimitError(result.Err):
			s.S3RateLimiter.RecordThrottle()
		}

		// NoSuchKey: the object is gone already, so the row can go too.
		if result.Err != nil && minio.ToErrorResponse(result.Err).Code != minio.NoSuchKey {
			slog.Error("failed to remove object", "object", result.ObjectName, "error", result.Err)

			s3Errs = append(s3Errs, fmt.Errorf("failed to remove object %q: %w", result.ObjectName, result.Err))
			stats.FailedCount++

			continue
		}

		deleted = append(deleted, result.ObjectName)
	}

	if err := pg.New(tx).DeleteObjects(ctx, deleted); err != nil {
		return "", s3Errs, fmt.Errorf("delete object rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", s3Errs, fmt.Errorf("commit sweep transaction: %w", err)
	}

	stats.DeletedCount += len(deleted)

	return keys[len(keys)-1], s3Errs, nil
}

// cleanupOrphanObjects marks unreachable objects and deletes them from S3.
// When onProgress is non-nil it is called after every batch so callers can
// expose live counters.
func (s *Service) cleanupOrphanObjects(ctx context.Context, gracePeriod int32, onProgress func(ObjectCleanupStats)) (*ObjectCleanupStats, error) {
	stats := &ObjectCleanupStats{}

	notify := func() {
		if onProgress != nil {
			onProgress(*stats)
		}
	}

	marked, err := pg.New(s.Pool).MarkStaleObjects(ctx)
	if err != nil {
		return stats, fmt.Errorf("failed to mark stale objects: %w", err)
	}

	stats.MarkedCount = int(marked)
	notify()

	var errs []error

	for afterKey := ""; ; {
		last, batchErrs, err := s.sweepBatch(ctx, gracePeriod, afterKey, stats)
		errs = append(errs, batchErrs...)

		if err != nil {
			errs = append(errs, err)

			break
		}

		notify()

		if last == "" {
			break
		}

		afterKey = last
	}

	if len(errs) > 0 {
		return stats, fmt.Errorf("%d sweep failures: %w", len(errs), errors.Join(errs...))
	}

	return stats, nil
}
