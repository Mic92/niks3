package server

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/Mic92/niks3/server/pg"
	"github.com/minio/minio-go/v7"
)

const (
	multipartPartSize = 10 * 1024 * 1024 // 10MB parts
)

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

	// Create Core client for multipart operations
	coreClient := minio.Core{Client: s.MinioClient}

	// Wait for rate limiter
	if err := s.S3RateLimiter.Wait(ctx); err != nil {
		return PendingObject{}, err
	}

	// Initiate multipart upload
	uploadID, err := coreClient.NewMultipartUpload(ctx, s.Bucket, objectKey, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		if isRateLimitError(err) {
			s.S3RateLimiter.RecordThrottle()
		}

		return PendingObject{}, fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	s.S3RateLimiter.RecordSuccess()

	// Store upload ID in database
	if err := pg.New(s.Pool).InsertMultipartUpload(ctx, pg.InsertMultipartUploadParams{
		PendingClosureID: pendingClosureID,
		ObjectKey:        objectKey,
		UploadID:         uploadID,
	}); err != nil {
		_ = coreClient.AbortMultipartUpload(ctx, s.Bucket, objectKey, uploadID)

		return PendingObject{}, fmt.Errorf("failed to store multipart upload: %w", err)
	}

	// Generate presigned URLs for each part (starting from part 1)
	partURLs, err := s.generatePartURLs(ctx, objectKey, uploadID, 1, numParts)
	if err != nil {
		// Cleanup: abort multipart upload
		_ = coreClient.AbortMultipartUpload(ctx, s.Bucket, objectKey, uploadID)

		return PendingObject{}, err
	}

	return PendingObject{
		MultipartInfo: &MultipartUploadInfo{
			UploadID: uploadID,
			PartURLs: partURLs,
		},
	}, nil
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
