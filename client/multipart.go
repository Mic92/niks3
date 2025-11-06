package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	multipartPartSize = 10 * 1024 * 1024 // 10MB parts for balance between overhead and throughput
)

// formatBytes formats bytes in human-readable form (KB/MB/GB).
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}

	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// uploadBufferPool pools 10MB buffers for multipart uploads to reduce memory allocations.
var uploadBufferPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool should be global
	New: func() any {
		buf := make([]byte, multipartPartSize)

		return &buf
	},
}

// MultipartUploadInfo contains multipart upload information.
type MultipartUploadInfo struct {
	UploadID string   `json:"upload_id"`
	PartURLs []string `json:"part_urls"`
}

// CompletedPart represents a completed multipart part.
type CompletedPart struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
}

// completeMultipartRequest is the request to complete a multipart upload.
type completeMultipartRequest struct {
	ObjectKey string          `json:"object_key"`
	UploadID  string          `json:"upload_id"`
	Parts     []CompletedPart `json:"parts"`
}

// requestMorePartsRequest is the request to request additional part URLs.
type requestMorePartsRequest struct {
	ObjectKey       string `json:"object_key"`
	UploadID        string `json:"upload_id"`
	StartPartNumber int    `json:"start_part_number"`
	NumParts        int    `json:"num_parts"`
}

// requestMorePartsResponse is the response with additional part URLs.
type requestMorePartsResponse struct {
	PartURLs        []string `json:"part_urls"`
	StartPartNumber int      `json:"start_part_number"`
}

// RequestMoreParts requests additional part URLs for an existing multipart upload.
func (c *Client) RequestMoreParts(ctx context.Context, objectKey, uploadID string, startPartNumber, numParts int) ([]string, error) {
	reqURL := c.baseURL.JoinPath("api/multipart/request-parts")

	reqBody := requestMorePartsRequest{
		ObjectKey:       objectKey,
		UploadID:        uploadID,
		StartPartNumber: startPartNumber,
		NumParts:        numParts,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.authToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.DoWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK); err != nil {
		return nil, err
	}

	var respBody requestMorePartsResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return respBody.PartURLs, nil
}

// CompleteMultipartUpload completes a multipart upload.
func (c *Client) CompleteMultipartUpload(ctx context.Context, objectKey, uploadID string, parts []CompletedPart) error {
	reqURL := c.baseURL.JoinPath("api/multipart/complete")

	reqBody := completeMultipartRequest{
		ObjectKey: objectKey,
		UploadID:  uploadID,
		Parts:     parts,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.authToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.DoWithRetry(ctx, req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}

	return nil
}

// uploadMultipart uploads a stream in parts using presigned URLs (sequential).
func (c *Client) uploadMultipart(ctx context.Context, r io.Reader, multipartInfo *MultipartUploadInfo, objectKey string) (*CompressedFileInfo, error) {
	slog.Debug("Uploading", "object_key", objectKey)

	var completedParts []CompletedPart

	hasher := sha256.New()

	var totalSize atomic.Uint64

	// Get buffer from pool
	bufferPtr, ok := uploadBufferPool.Get().(*[]byte)
	if !ok {
		return nil, errors.New("failed to get buffer from pool")
	}

	defer uploadBufferPool.Put(bufferPtr)

	buffer := *bufferPtr

	partNumber := 1
	partURLs := multipartInfo.PartURLs

	var reachedEOF bool

	for {
		// Check if we need more part URLs
		if partNumber > len(partURLs) {
			// Request more parts (batch of 100)
			const additionalParts = 100
			slog.Info("Requesting additional part URLs",
				"object_key", objectKey,
				"current_part", partNumber,
				"requesting", additionalParts)

			newPartURLs, err := c.RequestMoreParts(ctx, objectKey, multipartInfo.UploadID, partNumber, additionalParts)
			if err != nil {
				return nil, fmt.Errorf("requesting more parts at part %d: %w", partNumber, err)
			}

			partURLs = append(partURLs, newPartURLs...)
			slog.Info("Received additional part URLs", "count", len(newPartURLs), "total_parts", len(partURLs))
		}

		// Read up to multipartPartSize for this part
		n, readErr := io.ReadFull(r, buffer)
		if errors.Is(readErr, io.EOF) {
			// Done reading
			reachedEOF = true

			break
		}

		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("reading part %d: %w", partNumber, readErr)
		}

		partData := buffer[:n]

		// Update hash and size
		hasher.Write(partData)
		//nolint:gosec // n is from io.ReadFull which returns valid int
		totalSize.Add(uint64(n))

		// Upload this part
		partURL := partURLs[partNumber-1]

		etag, err := c.uploadPart(ctx, partURL, partData)
		if err != nil {
			return nil, fmt.Errorf("uploading part %d: %w", partNumber, err)
		}

		completedParts = append(completedParts, CompletedPart{
			PartNumber: partNumber,
			ETag:       etag,
		})

		partNumber++

		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			// Short read indicates end of stream
			reachedEOF = true

			break
		}
	}

	if !reachedEOF {
		return nil, errors.New("unexpected end of upload loop without reaching EOF")
	}

	// Complete the multipart upload
	err := c.CompleteMultipartUpload(ctx, objectKey, multipartInfo.UploadID, completedParts)
	if err != nil {
		return nil, fmt.Errorf("completing multipart upload: %w", err)
	}

	// Log completion with final size
	slog.Debug("Completed upload", "size", formatBytes(totalSize.Load()), "parts", len(completedParts))

	// Compute final hash
	hashBytes := hasher.Sum(nil)
	hash := "sha256:" + EncodeNixBase32(hashBytes)

	return &CompressedFileInfo{
		Size: totalSize.Load(),
		Hash: hash,
	}, nil
}

// uploadPart uploads a single part and returns the ETag.
func (c *Client) uploadPart(ctx context.Context, partURL string, data []byte) (string, error) {
	resp, err := c.putBytes(ctx, partURL, data)
	if err != nil {
		return "", err
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusNoContent); err != nil {
		return "", err
	}

	// Get ETag from response
	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", errors.New("no ETag in response")
	}

	// Remove quotes from ETag if present
	etag = strings.Trim(etag, "\"")

	return etag, nil
}
