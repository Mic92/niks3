package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

func (s *Service) cleanupPendingClosures(ctx context.Context, duration time.Duration) (int, error) {
	queries := pg.New(s.Pool)
	seconds := int32(duration.Seconds())
	coreClient := minio.Core{Client: s.MinioClient}

	// 1. Get old multipart uploads to abort
	uploads, err := queries.GetOldMultipartUploads(ctx, seconds)
	if err != nil {
		return 0, fmt.Errorf("get old uploads: %w", err)
	}

	// 2. Abort them in S3
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(s.S3Concurrency)

	for _, upload := range uploads {
		eg.Go(func() error {
			if err := s.S3RateLimiter.Wait(egCtx); err != nil {
				return err
			}

			if err := coreClient.AbortMultipartUpload(egCtx, s.Bucket, upload.ObjectKey, upload.UploadID); err != nil {
				if isRateLimitError(err) {
					s.S3RateLimiter.RecordThrottle()
				}

				if errResp := minio.ToErrorResponse(err); errResp.Code != minio.NoSuchUpload {
					slog.Warn("Failed to abort upload", "key", upload.ObjectKey, "error", err, "code", errResp.Code)
				} else if errors.Is(err, context.Canceled) {
					return err
				}
			} else {
				s.S3RateLimiter.RecordSuccess()
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return 0, fmt.Errorf("abort multipart uploads: %w", err)
	}

	slog.Info("Aborted multipart uploads", "count", len(uploads))

	// 3. Clean database (cascade deletes multipart_uploads rows)
	count, err := queries.CleanupPendingClosures(ctx, seconds)
	if err != nil {
		return 0, fmt.Errorf("cleanup pending closures: %w", err)
	}

	return int(count), nil
}
