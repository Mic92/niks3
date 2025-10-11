package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Mic92/niks3/server/pg"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

type ObjectType int

const (
	ObjectTypeNarinfo ObjectType = iota
	ObjectTypeListing
	ObjectTypeBuildLog
	ObjectTypeNAR
)

func getObjectType(objectKey string) ObjectType {
	if strings.HasSuffix(objectKey, ".narinfo") {
		return ObjectTypeNarinfo
	}

	if strings.HasSuffix(objectKey, ".ls") {
		return ObjectTypeListing
	}

	if strings.HasPrefix(objectKey, "log/") {
		return ObjectTypeBuildLog
	}

	// Default: assume it's a NAR file
	return ObjectTypeNAR
}

const (
	maxSignedURLDuration = time.Duration(5) * time.Hour
	multipartPartSize    = 5 * 1024 * 1024 // 5MB parts (S3 minimum)
)

type MultipartUploadInfo struct {
	UploadID string   `json:"upload_id"`
	PartURLs []string `json:"part_urls"`
}

type PendingObject struct {
	PresignedURL  string               `json:"presigned_url,omitempty"`  // For small files (narinfo)
	MultipartInfo *MultipartUploadInfo `json:"multipart_info,omitempty"` // For large files (NAR)
}

type PendingClosureResponse struct {
	ID             string                   `json:"id"`
	StartedAt      time.Time                `json:"started_at"`
	PendingObjects map[string]PendingObject `json:"pending_objects"`
}

type PendingClosure struct {
	id             int64
	startedAt      time.Time
	pendingObjects []pg.InsertPendingObjectsParams
	deletedObjects []string
}

func rollbackOnError(ctx context.Context, tx *pgx.Tx, err *error, committed *bool) {
	if p := recover(); p != nil && !*committed {
		if err := (*tx).Rollback(ctx); err != nil {
			slog.Error("failed to rollback transaction", "error", err)
		}

		panic(p) // re-throw after Rollback
	} else if err != nil && !*committed {
		if err := (*tx).Rollback(ctx); err != nil {
			slog.Error("failed to rollback transaction", "error", err)
		}
	}
}

func waitForDeletion(ctx context.Context, pool *pgxpool.Pool, inflightPaths []string) (map[string]bool, error) {
	queries := pg.New(pool)

	missingObjects := make(map[string]bool, len(inflightPaths))
	for _, objectKey := range inflightPaths {
		missingObjects[objectKey] = true
	}

	for len(inflightPaths) > 0 {
		time.Sleep(time.Duration(1) * time.Second)

		existingObjects, err := queries.GetExistingObjects(ctx, inflightPaths)
		if err != nil {
			return nil, fmt.Errorf("failed to get existing objects: %w", err)
		}

		// reset inflightPaths
		inflightPaths = inflightPaths[:0]

		for _, existingObject := range existingObjects {
			deletedAt, ok := existingObject.DeletedAt.(pgtype.Interval)
			if !ok {
				return nil, fmt.Errorf("deleted_at is not set for object: %s", existingObject.Key)
			}

			if deletedAt.Months == 0 && deletedAt.Days == 0 && deletedAt.Microseconds < 1000*1000*30 {
				inflightPaths = append(inflightPaths, existingObject.Key)
			} else {
				delete(missingObjects, existingObject.Key)
			}
		}
	}

	return missingObjects, nil
}

func createPendingClosureInner(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	objectsWithRefs map[string][]string,
) (*PendingClosure, error) {
	if !strings.HasSuffix(closureKey, ".narinfo") {
		return nil, fmt.Errorf("closure key must end with .narinfo: %s", closureKey)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	committed := false

	defer rollbackOnError(ctx, &tx, &err, &committed)

	queries := pg.New(tx)

	var pendingClosure pg.PendingClosure

	if pendingClosure, err = queries.InsertPendingClosure(ctx, closureKey); err != nil {
		return nil, fmt.Errorf("failed to insert pending closure: %w", err)
	}

	keys := make([]string, 0, len(objectsWithRefs))
	for k := range objectsWithRefs {
		keys = append(keys, k)
	}

	existingObjects, err := queries.GetExistingObjects(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing objects: %w", err)
	}

	deletedObjects := make([]string, 0, len(existingObjects))

	for _, existingObject := range existingObjects {
		if existingObject.DeletedAt != nil {
			deletedObjects = append(deletedObjects, existingObject.Key)
		} else {
			delete(objectsWithRefs, existingObject.Key)
		}
	}

	pendingObjects := make([]pg.InsertPendingObjectsParams, 0, len(objectsWithRefs))

	for objectKey, refs := range objectsWithRefs {
		pendingObjects = append(pendingObjects, pg.InsertPendingObjectsParams{
			PendingClosureID: pendingClosure.ID,
			Key:              objectKey,
			Refs:             refs,
		})
	}

	if _, err = queries.InsertPendingObjects(ctx, pendingObjects); err != nil {
		return nil, fmt.Errorf("failed to insert pending objects: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	committed = true

	return &PendingClosure{
		id:             pendingClosure.ID,
		startedAt:      pendingClosure.StartedAt.Time,
		pendingObjects: pendingObjects,
		deletedObjects: deletedObjects,
	}, nil
}

// estimatePartsNeeded estimates how many multipart parts we'll need based on NarSize.
// Assumes worst-case: no compression (1:1 ratio) plus buffer for overhead.
func estimatePartsNeeded(narSize uint64) int {
	const (
		minParts = 2
		maxParts = 100
	)

	if narSize == 0 {
		return 10 // Default if unknown
	}

	// Assume worst case: no compression, file stays same size
	estimatedSize := narSize

	// Calculate parts needed (5MB per part)
	partsU64 := (estimatedSize + multipartPartSize - 1) / multipartPartSize

	// Add 20% buffer for compression overhead/metadata
	partsU64 += (partsU64 / 5)

	// Cap at max before converting to int (ensures safe conversion)
	if partsU64 > maxParts {
		return maxParts
	}

	// Safe conversion now that we know it's <= maxParts
	parts := int(partsU64)

	// Apply minimum
	if parts < minParts {
		return minParts
	}

	return parts
}

func (s *Service) createPendingObjects(
	ctx context.Context,
	pendingClosureID int64,
	pendingObjectsParams []pg.InsertPendingObjectsParams,
	objectsWithNarSize map[string]uint64,
	result map[string]PendingObject,
) error {
	for _, pendingObject := range pendingObjectsParams {
		narSize := objectsWithNarSize[pendingObject.Key]

		po, err := s.makePendingObject(ctx, pendingClosureID, pendingObject.Key, narSize)
		if err != nil {
			return fmt.Errorf("failed to create pending object: %w", err)
		}

		result[pendingObject.Key] = po
	}

	return nil
}

func (s *Service) makePendingObject(ctx context.Context, pendingClosureID int64, objectKey string, narSize uint64) (PendingObject, error) {
	objectType := getObjectType(objectKey)

	// Small files use simple presigned URL
	switch objectType {
	case ObjectTypeNarinfo, ObjectTypeListing, ObjectTypeBuildLog:
		presignedURL, err := s.MinioClient.PresignedPutObject(ctx,
			s.Bucket,
			objectKey,
			maxSignedURLDuration)
		if err != nil {
			return PendingObject{}, fmt.Errorf("failed to create presigned URL: %w", err)
		}

		return PendingObject{
			PresignedURL: presignedURL.String(),
		}, nil

	case ObjectTypeNAR:
		// NAR files (large) use multipart upload
		return s.createMultipartUpload(ctx, pendingClosureID, objectKey, narSize)

	default:
		return PendingObject{}, fmt.Errorf("unknown object type for key: %s", objectKey)
	}
}

func (s *Service) createMultipartUpload(ctx context.Context, pendingClosureID int64, objectKey string, narSize uint64) (PendingObject, error) {
	numParts := estimatePartsNeeded(narSize)

	// Create Core client for multipart operations
	coreClient := minio.Core{Client: s.MinioClient}

	// Initiate multipart upload
	uploadID, err := coreClient.NewMultipartUpload(ctx, s.Bucket, objectKey, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return PendingObject{}, fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	// Store upload ID in database
	if err := pg.New(s.Pool).InsertMultipartUpload(ctx, pg.InsertMultipartUploadParams{
		PendingClosureID: pendingClosureID,
		ObjectKey:        objectKey,
		UploadID:         uploadID,
	}); err != nil {
		_ = coreClient.AbortMultipartUpload(ctx, s.Bucket, objectKey, uploadID)

		return PendingObject{}, fmt.Errorf("failed to store multipart upload: %w", err)
	}

	// Presign URLs for each part
	partURLs := make([]string, numParts)
	for i := range numParts {
		partNumber := i + 1 // Part numbers start at 1
		// Use Client.Presign with query parameters for multipart
		reqParams := make(url.Values)
		reqParams.Set("uploadId", uploadID)
		reqParams.Set("partNumber", strconv.Itoa(partNumber))

		presignedURL, err := s.MinioClient.Presign(ctx,
			"PUT",
			s.Bucket,
			objectKey,
			maxSignedURLDuration,
			reqParams)
		if err != nil {
			// Cleanup: abort multipart upload
			_ = coreClient.AbortMultipartUpload(ctx, s.Bucket, objectKey, uploadID)

			return PendingObject{}, fmt.Errorf("failed to presign part %d: %w", partNumber, err)
		}

		partURLs[i] = presignedURL.String()
	}

	return PendingObject{
		MultipartInfo: &MultipartUploadInfo{
			UploadID: uploadID,
			PartURLs: partURLs,
		},
	}, nil
}

func (s *Service) createPendingClosure(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	objectsWithRefs map[string][]string,
	objectsWithNarSize map[string]uint64,
) (*PendingClosureResponse, error) {
	pendingClosure, err := createPendingClosureInner(ctx, pool, closureKey, objectsWithRefs)
	if err != nil {
		return nil, err
	}

	pendingObjects := make(map[string]PendingObject, len(pendingClosure.pendingObjects)+len(pendingClosure.deletedObjects))

	if err := s.createPendingObjects(ctx, pendingClosure.id, pendingClosure.pendingObjects, objectsWithNarSize, pendingObjects); err != nil {
		return nil, err
	}

	if len(pendingClosure.deletedObjects) > 0 {
		slog.Info("Found objects not yet deleted. Waiting for deletion",
			"pending_objects", len(pendingClosure.deletedObjects))

		missingObjects, err := waitForDeletion(ctx, pool, pendingClosure.deletedObjects)
		if err != nil {
			return nil, err
		}

		pendingObjectsParams := make([]pg.InsertPendingObjectsParams, 0, len(missingObjects))
		for objectKey := range missingObjects {
			pendingObjectsParams = append(pendingObjectsParams, pg.InsertPendingObjectsParams{
				PendingClosureID: pendingClosure.id,
				Key:              objectKey,
				Refs:             objectsWithRefs[objectKey],
			})
		}

		queries := pg.New(pool)

		if _, err = queries.InsertPendingObjects(ctx, pendingObjectsParams); err != nil {
			return nil, fmt.Errorf("failed to insert pending objects: %w", err)
		}

		if err := s.createPendingObjects(ctx, pendingClosure.id, pendingObjectsParams, objectsWithNarSize, pendingObjects); err != nil {
			return nil, err
		}
	}

	return &PendingClosureResponse{
		ID:             strconv.FormatInt(pendingClosure.id, 10),
		StartedAt:      pendingClosure.startedAt,
		PendingObjects: pendingObjects,
	}, nil
}

var errPendingClosureNotFound = errors.New("not found")

func commitPendingClosure(ctx context.Context, pool *pgxpool.Pool, pendingClosureID int64) error {
	if err := pg.New(pool).CommitPendingClosure(ctx, pendingClosureID); err != nil {
		msg := "Closure does not exist:"

		var pgError *pgconn.PgError

		ok := errors.As(err, &pgError)
		if ok && strings.Contains(pgError.Message, msg) {
			return fmt.Errorf("failed to commit pending closure: %w", errPendingClosureNotFound)
		}

		return fmt.Errorf("failed to commit pending closure: %w", err)
	}

	return nil
}

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
