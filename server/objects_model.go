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
				err := queries.MarkObjectsAsActive(ctx, failedKeys)
				if err != nil {
					slog.Error("failed to mark objects as active", "error", err)
					*s3Error = fmt.Errorf("failed to mark objects as active: %w", err)
				}

				failedKeys = failedKeys[:0]
			}

			continue
		}

		deletedKeys = append(deletedKeys, result.ObjectName)

		if len(deletedKeys) >= DeletionBatchSize {
			err := queries.DeleteObjects(ctx, deletedKeys)
			if err != nil {
				slog.Error("failed to mark objects as deleted", "error", err)
				*s3Error = fmt.Errorf("failed to mark objects as deleted: %w", err)
			}

			deletedKeys = deletedKeys[:0]
		}
	}

	if len(failedKeys) > 0 {
		err := queries.MarkObjectsAsActive(ctx, failedKeys)
		if err != nil {
			*s3Error = fmt.Errorf("failed to mark objects as active: %w", err)
		}
	}

	if len(deletedKeys) > 0 {
		err := queries.DeleteObjects(ctx, deletedKeys)
		if err != nil {
			*s3Error = fmt.Errorf("failed to mark objects as deleted: %w", err)
		}
	}
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
