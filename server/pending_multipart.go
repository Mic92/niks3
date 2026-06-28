package server

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/Mic92/niks3/server/pg"
	pgx "github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
)

const (
	multipartPartSize = 10 * 1024 * 1024 // 10MB parts

	// multipartLockWindow bounds how long a registered multipart upload is
	// treated as in-flight for the exclusive-lock check in
	// createMultipartUpload. Within this window a second pending_closure
	// targeting the same NAR gets HTTP 409 and is expected to retry; past
	// it the previous upload is assumed dead (crashed client) and a fresh
	// one is opened, leaving the stale row to cleanupPendingClosures.
	// Sized to cover a max-parts NAR (~1 GiB) over a slow home-network
	// upload while keeping a crashed-client lockout short enough that a
	// retrying CI job finishes within ordinary timeouts.
	multipartLockWindow = 10 * time.Minute
)

// errMultipartUploadInProgress signals that another pending_closure is still
// uploading the same object key. Surfaced as HTTP 409 by the handler so the
// client can back off and retry instead of opening a second multipart upload
// (compression output is not byte-stable across uploaders).
var errMultipartUploadInProgress = errors.New("multipart upload already in progress for this object")

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

	// Refuse to open a second multipart upload while another pending_closure
	// is plausibly still uploading the same NAR. Parallel CI workflows often
	// race on the same dependency closure; without this check each closure
	// opened its own NewMultipartUpload and only one ever got completed,
	// leaving the rest as orphans that bloated the S3 backend's metadata
	// store. We must not share the upload_id across clients either:
	// compression output is not byte-stable across multi-threaded
	// uploaders, so concatenating parts from two clients produces a corrupt
	// archive. So we treat a recent registration as an exclusive lock and
	// expect the client to back off; rows older than multipartLockWindow
	// are ignored here and cleaned up by cleanupPendingClosures.
	_, err := queries.GetActiveMultipartUploadByObjectKey(ctx, pg.GetActiveMultipartUploadByObjectKeyParams{
		ObjectKey:     objectKey,
		MaxAgeSeconds: int32(multipartLockWindow.Seconds()),
	})
	if err == nil {
		return PendingObject{}, errMultipartUploadInProgress
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return PendingObject{}, fmt.Errorf("failed to look up existing multipart upload: %w", err)
	}

	uploadID, err := s.initiateMultipartUpload(ctx, coreClient, objectKey)
	if err != nil {
		return PendingObject{}, err
	}

	if err := queries.InsertMultipartUpload(ctx, pg.InsertMultipartUploadParams{
		PendingClosureID: pendingClosureID,
		ObjectKey:        objectKey,
		UploadID:         uploadID,
	}); err != nil {
		_ = coreClient.AbortMultipartUpload(ctx, s.Bucket, objectKey, uploadID)

		return PendingObject{}, fmt.Errorf("failed to store multipart upload: %w", err)
	}

	partURLs, err := s.generatePartURLs(ctx, objectKey, uploadID, 1, numParts)
	if err != nil {
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
