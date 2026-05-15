package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
)

const (
	multipartPartSize = 10 * 1024 * 1024       // default/minimum part size
	maxPartSize       = 5 * 1024 * 1024 * 1024 // S3 max part size
	// S3 allows at most 10000 parts. Aim for 9000: zstd can emit a bit more
	// than the input on incompressible data, and rounding to a 16 MiB step
	// gives no slack right at a boundary. The headroom only costs a
	// slightly bigger part size.
	targetMaxParts = 9000
)

// partSizeForNAR returns a part size that keeps the compressed upload under
// S3's 10000-part limit. We don't know the compressed size up front, but the
// uncompressed NAR size is a safe upper bound. Rounded up to 16 MiB steps
// (same as minio-go), clamped to the default 10 MiB and S3's 5 GiB max.
func partSizeForNAR(narSize uint64) int {
	const step = 16 << 20

	size := (narSize + targetMaxParts - 1) / targetMaxParts
	if size <= multipartPartSize {
		return multipartPartSize
	}

	size = (size + step - 1) / step * step
	if size > maxPartSize {
		return maxPartSize
	}

	return int(size)
}

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

// uploadBufferPool pools default-sized part buffers to reduce allocations.
// Larger NARs get a one-off buffer (rare, not worth pooling).
var uploadBufferPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool should be global
	New: func() any {
		buf := make([]byte, multipartPartSize)

		return &buf
	},
}

func getPartBuffer(partSize int) ([]byte, func()) {
	if partSize > multipartPartSize {
		return make([]byte, partSize), func() {}
	}

	ptr, ok := uploadBufferPool.Get().(*[]byte)
	if !ok {
		return make([]byte, multipartPartSize), func() {}
	}

	return *ptr, func() { uploadBufferPool.Put(ptr) }
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

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.DoServerRequest(ctx, req)
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

	// Validate that the server returned the correct start part number
	if respBody.StartPartNumber != startPartNumber {
		return nil, fmt.Errorf("server returned start part %d but requested %d", respBody.StartPartNumber, startPartNumber)
	}

	// Validate that the server returned at least one part URL
	if len(respBody.PartURLs) == 0 {
		return nil, fmt.Errorf("server returned empty part URLs list (requested %d parts starting at %d)", numParts, startPartNumber)
	}

	// Validate that none of the URLs are empty strings
	for i, url := range respBody.PartURLs {
		if url == "" {
			return nil, fmt.Errorf("server returned empty URL at index %d (out of %d URLs)", i, len(respBody.PartURLs))
		}
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

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.DoServerRequest(ctx, req)
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
func (c *Client) uploadMultipart(ctx context.Context, r io.Reader, multipartInfo *MultipartUploadInfo, objectKey string, partSize int) error {
	slog.Debug("Uploading", "object_key", objectKey, "part_size", partSize)

	var completedParts []CompletedPart

	buffer, release := getPartBuffer(partSize)
	defer release()

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
				return fmt.Errorf("requesting more parts at part %d: %w", partNumber, err)
			}

			partURLs = append(partURLs, newPartURLs...)
			slog.Info("Received additional part URLs", "count", len(newPartURLs), "total_parts", len(partURLs))
		}

		// Read up to partSize for this part
		n, readErr := io.ReadFull(r, buffer)
		if errors.Is(readErr, io.EOF) {
			// Done reading
			reachedEOF = true

			break
		}

		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return fmt.Errorf("reading part %d: %w", partNumber, readErr)
		}

		partData := buffer[:n]

		// Upload this part
		partURL := partURLs[partNumber-1]

		etag, err := c.uploadPart(ctx, partURL, partData)
		if err != nil {
			return fmt.Errorf("uploading part %d: %w", partNumber, err)
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
		return errors.New("unexpected end of upload loop without reaching EOF")
	}

	// Complete the multipart upload
	err := c.CompleteMultipartUpload(ctx, objectKey, multipartInfo.UploadID, completedParts)
	if err != nil {
		return fmt.Errorf("completing multipart upload: %w", err)
	}

	slog.Debug("Completed upload", "parts", len(completedParts))

	return nil
}

// uploadPart uploads a single part and returns the ETag.
func (c *Client) uploadPart(ctx context.Context, partURL string, data []byte) (string, error) {
	resp, err := c.putBytes(ctx, partURL, data)
	if err != nil {
		return "", err
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusCreated, http.StatusNoContent); err != nil {
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
