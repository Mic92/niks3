package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
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

// ErrUploadSuperseded means a concurrent closure already finished the same NAR
// and our upload was aborted server-side. The blob exists, so callers skip it.
var ErrUploadSuperseded = errors.New("multipart upload superseded by a concurrent upload")

// supersededByPeer reports whether a 404 from the upload means the NAR is
// already present. A 404 is ambiguous (peer abort vs expired upload), so we
// confirm the object exists in S3 before skipping to avoid losing data.
func (c *Client) supersededByPeer(ctx context.Context, objectKey string, err error) bool {
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		return false
	}

	exists, existsErr := c.ObjectExists(ctx, objectKey)
	if existsErr != nil {
		slog.Warn("Failed to confirm object after upload 404", "object_key", objectKey, "error", existsErr)

		return false
	}

	return exists
}

// ObjectExists reports whether objectKey is already present in S3.
func (c *Client) ObjectExists(ctx context.Context, objectKey string) (bool, error) {
	reqURL := c.baseURL.JoinPath("api/objects", objectKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, reqURL.String(), nil)
	if err != nil {
		return false, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.DoServerRequest(ctx, req)
	if err != nil {
		return false, fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	switch resp.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, &HTTPStatusError{StatusCode: resp.StatusCode}
	}
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

	var respBody requestMorePartsResponse
	if err := c.doJSONRequest(ctx, http.MethodPost, reqURL.String(), reqBody, &respBody, http.StatusOK); err != nil {
		return nil, err
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

	return c.doJSONRequest(ctx, http.MethodPost, reqURL.String(), reqBody, nil, http.StatusOK, http.StatusNoContent)
}

// partsInFlight bounds concurrent part PUTs (and part-sized buffers) per NAR.
const partsInFlight = 4

// uploadMultipart uploads a stream in parts using presigned URLs, up to
// partsInFlight parts at once.
func (c *Client) uploadMultipart(ctx context.Context, r io.Reader, multipartInfo *MultipartUploadInfo, objectKey string, partSize int) error {
	slog.Debug("Uploading", "object_key", objectKey, "part_size", partSize)

	var (
		mu             sync.Mutex
		completedParts []CompletedPart
	)

	g, gctx := errgroup.WithContext(ctx)
	// Taken before reading a part so buffers are bounded too, not just PUTs.
	slots := make(chan struct{}, partsInFlight)
	partURLs := multipartInfo.PartURLs

	readErr := func() error {
		for partNumber := 1; ; partNumber++ {
			if partNumber > len(partURLs) {
				const additionalParts = 100

				newPartURLs, err := c.RequestMoreParts(gctx, objectKey, multipartInfo.UploadID, partNumber, additionalParts)
				if err != nil {
					return fmt.Errorf("requesting more parts at part %d: %w", partNumber, err)
				}

				partURLs = append(partURLs, newPartURLs...)
				slog.Info("Received additional part URLs", "count", len(newPartURLs), "total_parts", len(partURLs))
			}

			select {
			case slots <- struct{}{}:
			case <-gctx.Done():
				return nil
			}

			buffer, release := getPartBuffer(partSize)

			n, err := io.ReadFull(r, buffer)
			if n == 0 {
				release()
				<-slots

				if errors.Is(err, io.EOF) {
					return nil
				}

				return fmt.Errorf("reading part %d: %w", partNumber, err)
			}

			last := errors.Is(err, io.ErrUnexpectedEOF)
			if err != nil && !last {
				release()
				<-slots

				return fmt.Errorf("reading part %d: %w", partNumber, err)
			}

			partURL, partData := partURLs[partNumber-1], buffer[:n]

			g.Go(func() error {
				defer func() { <-slots }()

				etag, err := c.uploadPart(gctx, partURL, partData)
				if err != nil {
					// Not released: on an early error response the transport
					// may still be copying the request body from this buffer
					// after Do has returned, and the next part read into a
					// pooled buffer would race it. Let the GC have this one.
					return fmt.Errorf("uploading part %d: %w", partNumber, err)
				}

				// A 2xx means S3 read the whole part, so the transport is
				// done with the buffer.
				release()

				mu.Lock()
				defer mu.Unlock()

				completedParts = append(completedParts, CompletedPart{PartNumber: partNumber, ETag: etag})

				return nil
			})

			if last {
				return nil
			}
		}
	}()

	if err := g.Wait(); err != nil {
		if c.supersededByPeer(ctx, objectKey, err) {
			return ErrUploadSuperseded
		}

		return err //nolint:wrapcheck // wrapped with its part number above
	}

	if readErr != nil {
		return readErr
	}

	slices.SortFunc(completedParts, func(a, b CompletedPart) int { return a.PartNumber - b.PartNumber })

	// Complete the multipart upload
	err := c.CompleteMultipartUpload(ctx, objectKey, multipartInfo.UploadID, completedParts)
	if err != nil {
		if c.supersededByPeer(ctx, objectKey, err) {
			return ErrUploadSuperseded
		}

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
