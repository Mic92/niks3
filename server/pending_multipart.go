package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"

	"github.com/Mic92/niks3/server/pg"
	pgx "github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
)

const (
	multipartPartSize = 10 * 1024 * 1024 // 10MB parts
)

// useSimpleUpload reports whether a NAR of the given uncompressed size should
// be uploaded with a single presigned PUT instead of a multipart upload.
// NARs that fit into one part gain nothing from multipart but cost extra S3
// API calls. Size 0 (unknown) uses multipart.
func useSimpleUpload(narSize uint64) bool {
	return narSize > 0 && narSize <= multipartPartSize
}

type MultipartUploadInfo struct {
	UploadID string   `json:"upload_id"`
	PartURLs []string `json:"part_urls"`
}

// requestPartsRequest is the request to request additional part URLs.
type requestPartsRequest struct {
	ObjectKey       string `json:"object_key"`
	UploadID        string `json:"upload_id"`
	StartPartNumber int    `json:"start_part_number"` // The first part number to generate URLs for
	NumParts        int    `json:"num_parts"`         // Number of parts to generate
}

// requestPartsResponse is the response with additional part URLs.
type requestPartsResponse struct {
	PartURLs        []string `json:"part_urls"`
	StartPartNumber int      `json:"start_part_number"`
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

	// Calculate parts needed (10MB per part)
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

func (s *Service) createMultipartUpload(ctx context.Context, pendingClosureID int64, objectKey string, narSize uint64) (PendingObject, error) {
	numParts := estimatePartsNeeded(narSize)

	queries := pg.New(s.Pool)
	coreClient := minio.Core{Client: s.MinioClient}

	// Reuse an in-flight S3 multipart upload if one is already registered for
	// this object key. Parallel CI workflows often share a NAR via multiple
	// pending_closures; without this check each closure would initiate its
	// own NewMultipartUpload, and only one of those gets completed by a
	// client — the rest become orphaned multipart uploads that bloat the S3
	// backend's metadata store. Part URLs are always regenerated because
	// presigned URLs are time-bounded; reusing the upload ID is sufficient.
	uploadID, err := queries.GetActiveMultipartUploadByObjectKey(ctx, objectKey)
	freshUpload := false

	switch {
	case err == nil:
		slog.Debug("reusing existing multipart upload", "object_key", objectKey, "upload_id", uploadID)
	case errors.Is(err, pgx.ErrNoRows):
		uploadID, err = s.initiateMultipartUpload(ctx, coreClient, objectKey)
		if err != nil {
			return PendingObject{}, err
		}

		freshUpload = true
	default:
		return PendingObject{}, fmt.Errorf("failed to look up existing multipart upload: %w", err)
	}

	// Bookkeep this closure's reference to the upload. The primary key is
	// (pending_closure_id, object_key), so reusing an upload_id across two
	// closures yields two rows pointing at the same S3 mpu — both get
	// cleaned up via DeleteMultipartUpload on completion.
	if err := queries.InsertMultipartUpload(ctx, pg.InsertMultipartUploadParams{
		PendingClosureID: pendingClosureID,
		ObjectKey:        objectKey,
		UploadID:         uploadID,
	}); err != nil {
		if freshUpload {
			_ = coreClient.AbortMultipartUpload(ctx, s.Bucket, objectKey, uploadID)
		}

		return PendingObject{}, fmt.Errorf("failed to store multipart upload: %w", err)
	}

	// Generate presigned URLs for each part (starting from part 1)
	partURLs, err := s.generatePartURLs(ctx, objectKey, uploadID, 1, numParts)
	if err != nil {
		// Only abort if we just opened this upload; we must not abort an
		// upload that another in-flight closure is relying on.
		if freshUpload {
			_ = coreClient.AbortMultipartUpload(ctx, s.Bucket, objectKey, uploadID)
		}

		return PendingObject{}, err
	}

	return PendingObject{
		MultipartInfo: &MultipartUploadInfo{
			UploadID: uploadID,
			PartURLs: partURLs,
		},
	}, nil
}

// initiateMultipartUpload calls S3 CreateMultipartUpload and returns the new
// upload ID, honoring the adaptive rate limiter.
func (s *Service) initiateMultipartUpload(ctx context.Context, coreClient minio.Core, objectKey string) (string, error) {
	if err := s.S3RateLimiter.Wait(ctx); err != nil {
		return "", err
	}

	uploadID, err := coreClient.NewMultipartUpload(ctx, s.Bucket, objectKey, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		if isRateLimitError(err) {
			s.S3RateLimiter.RecordThrottle()
		}

		return "", fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	s.S3RateLimiter.RecordSuccess()

	return uploadID, nil
}

// generatePartURLs generates presigned URLs for multipart upload parts.
func (s *Service) generatePartURLs(ctx context.Context, objectKey, uploadID string, startPartNumber, numParts int) ([]string, error) {
	partURLs := make([]string, numParts)

	for i := range numParts {
		partNumber := startPartNumber + i

		// Wait for rate limiter
		if err := s.S3RateLimiter.Wait(ctx); err != nil {
			return nil, err
		}

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
			if isRateLimitError(err) {
				s.S3RateLimiter.RecordThrottle()
			}

			return nil, fmt.Errorf("failed to presign part %d: %w", partNumber, err)
		}

		s.S3RateLimiter.RecordSuccess()
		partURLs[i] = presignedURL.String()
	}

	return partURLs, nil
}
