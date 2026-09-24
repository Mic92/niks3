package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

func (s *Service) cleanupPendingClosures(ctx context.Context, duration time.Duration) (int, error) {
	queries := pg.New(s.Pool)
	coreClient := minio.Core{Client: s.MinioClient}

	// One cutoff for both queries: closures that aged past the cutoff between
	// listing the uploads and deleting the rows would otherwise be cascaded
	// out of the database with their multipart uploads still open in S3.
	cutoff := pgtype.Timestamp{Time: time.Now().UTC().Add(-duration), Valid: true}

	// 1. Get old multipart uploads to abort
	uploads, err := queries.GetOldMultipartUploads(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("get old uploads: %w", err)
	}

	// 2. Abort them in S3. A closure whose upload could not be aborted is kept:
	// the multipart_uploads row is the only handle on the upload, and dropping
	// it would leave the parts in S3 for good. The next run retries.
	var (
		keepMu sync.Mutex
		keep   []int64
	)

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(s.S3Concurrency)

	for _, upload := range uploads {
		eg.Go(func() error {
			if err := s.S3RateLimiter.Wait(egCtx); err != nil {
				return fmt.Errorf("rate limiter: %w", err)
			}

			err := coreClient.AbortMultipartUpload(egCtx, s.Bucket, upload.ObjectKey, upload.UploadID)
			if err == nil {
				s.S3RateLimiter.RecordSuccess()

				return nil
			}

			if isRateLimitError(err) {
				s.S3RateLimiter.RecordThrottle()
			}

			if errors.Is(err, context.Canceled) {
				return fmt.Errorf("abort upload %q: %w", upload.ObjectKey, err)
			}

			if errResp := minio.ToErrorResponse(err); errResp.Code != minio.NoSuchUpload {
				slog.Warn("Failed to abort upload, keeping its closure", "key", upload.ObjectKey, "error", err, "code", errResp.Code)

				keepMu.Lock()
				keep = append(keep, upload.PendingClosureID)
				keepMu.Unlock()
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return 0, fmt.Errorf("abort multipart uploads: %w", err)
	}

	slog.Info("Aborted multipart uploads", "count", len(uploads)-len(keep), "kept", len(keep))

	if keep == nil {
		keep = []int64{}
	}

	// 3. Clean database (cascade deletes multipart_uploads rows)
	count, err := queries.CleanupPendingClosures(ctx, pg.CleanupPendingClosuresParams{Cutoff: cutoff, Keep: keep})
	if err != nil {
		return 0, fmt.Errorf("cleanup pending closures: %w", err)
	}

	return int(count), nil
}
