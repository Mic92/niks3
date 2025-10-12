package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

func cleanupPendingClosures(ctx context.Context, pool *pgxpool.Pool, minioClient *minio.Client, bucket string, duration time.Duration) error {
	queries := pg.New(pool)
	seconds := int32(duration.Seconds())
	coreClient := minio.Core{Client: minioClient}

	// 1. Get old multipart uploads to abort
	uploads, err := queries.GetOldMultipartUploads(ctx, seconds)
	if err != nil {
		return fmt.Errorf("get old uploads: %w", err)
	}

	// 2. Abort them in S3
	for _, upload := range uploads {
		if err := coreClient.AbortMultipartUpload(ctx, bucket, upload.ObjectKey, upload.UploadID); err != nil {
			slog.Warn("Failed to abort upload", "key", upload.ObjectKey, "error", err)
		}
	}

	slog.Info("Aborted multipart uploads", "count", len(uploads))

	// 3. Clean database (cascade deletes multipart_uploads rows)
	if err := queries.CleanupPendingClosures(ctx, seconds); err != nil {
		return fmt.Errorf("cleanup pending closures: %w", err)
	}

	return nil
}
