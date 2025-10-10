package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

const (
	DeletionBatchSize = 1000
)

func flushBatch(ctx context.Context, keys []string, operation func(context.Context, []string) error, s3Error *error) []string {
	if len(keys) == 0 {
		return keys
	}

	if err := operation(ctx, keys); err != nil {
		slog.Error("batch operation failed", "error", err)
		*s3Error = err
	}

	return keys[:0]
}

func getObjectsForDeletion(ctx context.Context,
	pool *pgxpool.Pool,
	objectCh chan<- minio.ObjectInfo,
	s3Error *error,
	queryErr *error,
) {
	defer close(objectCh)

	queries := pg.New(pool)

	for *s3Error == nil {
		objs, err := queries.MarkObjectsForDeletion(ctx, DeletionBatchSize)
		if err != nil {
			*queryErr = fmt.Errorf("failed to mark objects for deletion: %w", err)
			slog.Error("failed to mark objects for deletion", "error", err)

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

func (s *Service) removeS3Objects(ctx context.Context,
	pool *pgxpool.Pool,
	objectCh <-chan minio.ObjectInfo,
	s3Error *error,
) {
	opts := minio.RemoveObjectsOptions{GovernanceBypass: false}
	failedKeys := make([]string, 0, DeletionBatchSize)
	deletedKeys := make([]string, 0, DeletionBatchSize)

	queries := pg.New(pool)

	for result := range s.MinioClient.RemoveObjectsWithResult(ctx, s.Bucket, objectCh, opts) {
		// if the object was not found, we can ignore it
		if result.Err != nil {
			if minio.ToErrorResponse(result.Err).Code == "NoSuchKey" {
				continue
			}

			*s3Error = fmt.Errorf("failed to remove object '%s': %w", result.ObjectName, result.Err)
			slog.Error("failed to remove object", "object", result.ObjectName, "error", s3Error)
			failedKeys = append(failedKeys, result.ObjectName)

			if len(failedKeys) >= DeletionBatchSize {
				failedKeys = flushBatch(ctx, failedKeys, queries.MarkObjectsAsActive, s3Error)
			}

			continue
		}

		deletedKeys = append(deletedKeys, result.ObjectName)

		if len(deletedKeys) >= DeletionBatchSize {
			deletedKeys = flushBatch(ctx, deletedKeys, queries.DeleteObjects, s3Error)
		}
	}

	flushBatch(ctx, failedKeys, queries.MarkObjectsAsActive, s3Error)
	flushBatch(ctx, deletedKeys, queries.DeleteObjects, s3Error)
}

func (s *Service) cleanupOrphanObjects(ctx context.Context, pool *pgxpool.Pool) error {
	// limit channel size to 1000, as minio limits to 1000 in one request
	objectCh := make(chan minio.ObjectInfo, DeletionBatchSize)

	var queryErr error

	var s3Error error

	go getObjectsForDeletion(ctx, pool, objectCh, &s3Error, &queryErr)

	s.removeS3Objects(ctx, pool, objectCh, &s3Error)

	if queryErr != nil {
		return queryErr
	}

	if s3Error != nil {
		return s3Error
	}

	return nil
}
