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
	multipartPartSize = 5 * 1024 * 1024 // 5MB parts (S3 minimum)
)

type MultipartUploadInfo struct {
	UploadID string   `json:"upload_id"`
	PartURLs []string `json:"part_urls"`
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
