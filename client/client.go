package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

const (
	multipartPartSize = 10 * 1024 * 1024 // 10MB parts for balance between overhead and throughput
)

// uploadBufferPool pools 10MB buffers for multipart uploads to reduce memory allocations.
var uploadBufferPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool should be global
	New: func() interface{} {
		buf := make([]byte, multipartPartSize)

		return &buf
	},
}

// zstdEncoderPool pools zstd encoders to reduce memory allocations.
// Each encoder maintains compression history buffers (~60-80MB) that can be reused.
var zstdEncoderPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool should be global
	New: func() interface{} {
		// Create encoder with nil writer (will use Reset() to set the actual writer)
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			// This should never happen with nil writer
			panic(fmt.Sprintf("failed to create zstd encoder: %v", err))
		}

		return encoder
	},
}

// Client handles uploads to the niks3 server.
type Client struct {
	baseURL                 *url.URL
	authToken               string
	httpClient              *http.Client
	MaxConcurrentNARUploads int // Maximum number of concurrent NAR uploads (0 = unlimited)
}

// ObjectWithRefs represents an object with its dependencies.
type ObjectWithRefs struct {
	Key     string   `json:"key"`
	Refs    []string `json:"refs"`
	NarSize *uint64  `json:"nar_size,omitempty"` // For estimating multipart parts
}

// createPendingClosureRequest is the request to create a pending closure.
type createPendingClosureRequest struct {
	Closure string           `json:"closure"`
	Objects []ObjectWithRefs `json:"objects"`
}

// MultipartUploadInfo contains multipart upload information.
type MultipartUploadInfo struct {
	UploadID string   `json:"upload_id"`
	PartURLs []string `json:"part_urls"`
}

// PendingObject contains upload information for an object.
type PendingObject struct {
	PresignedURL string `json:"presigned_url,omitempty"` // For small files

	MultipartInfo *MultipartUploadInfo `json:"multipart_info,omitempty"` // For large files
}

// CreatePendingClosureResponse is the response from creating a pending closure.
type CreatePendingClosureResponse struct {
	ID             string                   `json:"id"`
	StartedAt      string                   `json:"started_at"`
	PendingObjects map[string]PendingObject `json:"pending_objects"`
}

// CompressedFileInfo contains information about a compressed file.
type CompressedFileInfo struct {
	Size uint64
	Hash string
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

// NewClient creates a new upload client.
// The default MaxConcurrentNARUploads is set to 16, optimized for I/O-bound upload workloads.
// This is comparable to browser HTTP/2 connection limits and Cachix's default of 8.
//
// TODO: Test this value in various network setups (local network, high-latency WAN,
// rate-limited connections) to determine optimal defaults for different scenarios.
func NewClient(serverURL, authToken string) (*Client, error) {
	baseURL, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("parsing server URL: %w", err)
	}

	return &Client{
		baseURL:   baseURL,
		authToken: authToken,
		httpClient: &http.Client{
			Timeout: 0, // No timeout for streaming uploads
		},
		MaxConcurrentNARUploads: 16,
	}, nil
}

func deferCloseBody(resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		slog.Error("Failed to close response body", "error", err)
	}
}

func checkResponse(resp *http.Response, acceptedStatuses ...int) error {
	for _, status := range acceptedStatuses {
		if resp.StatusCode == status {
			return nil
		}
	}

	body, _ := io.ReadAll(resp.Body)

	return fmt.Errorf("server returned %d: %s", resp.StatusCode, body)
}

// CreatePendingClosure creates a pending closure and returns upload URLs.
func (c *Client) CreatePendingClosure(ctx context.Context, closure string, objects []ObjectWithRefs) (*CreatePendingClosureResponse, error) {
	reqURL := c.baseURL.JoinPath("api/pending_closures")

	reqBody := createPendingClosureRequest{
		Closure: closure,
		Objects: objects,
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

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusCreated); err != nil {
		return nil, err
	}

	var result CreatePendingClosureResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	slog.Info("Created pending closure", "id", result.ID, "pending_objects", len(result.PendingObjects))

	return &result, nil
}

// CompletePendingClosure marks a closure as complete.
func (c *Client) CompletePendingClosure(ctx context.Context, closureID string) error {
	reqURL := c.baseURL.JoinPath("api/pending_closures", closureID, "complete")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.authToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}

	slog.Info("Completed pending closure", "id", closureID)

	return nil
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

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	defer deferCloseBody(resp)

	if err := checkResponse(resp, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}

	slog.Info("Completed multipart upload", "object_key", objectKey)

	return nil
}

// UploadBytesToPresignedURL uploads bytes to a presigned URL.
func (c *Client) UploadBytesToPresignedURL(ctx context.Context, presignedURL string, data []byte) error {
	resp, err := c.putBytes(ctx, presignedURL, data)
	if err != nil {
		return err
	}

	defer deferCloseBody(resp)

	return checkResponse(resp, http.StatusOK, http.StatusNoContent)
}

// CompressAndUploadNAR compresses a NAR and uploads it using multipart upload.
func (c *Client) CompressAndUploadNAR(ctx context.Context, storePath string, pendingObj PendingObject, objectKey string) (*CompressedFileInfo, error) {
	slog.Info("Compressing and uploading NAR", "store_path", storePath)

	// Create a pipe for streaming: NAR serialization -> zstd compression -> hash/size tracking
	pr, pw := io.Pipe()

	// Channel to receive errors from the compression goroutine
	errChan := make(chan error, 1)

	// Start compression in goroutine
	go func() {
		defer func() {
			if err := pw.Close(); err != nil {
				slog.Error("Failed to close pipe writer", "error", err)
			}
		}()

		// Get encoder from pool and reset it to write to pipe
		encoder, ok := zstdEncoderPool.Get().(*zstd.Encoder)
		if !ok {
			pw.CloseWithError(errors.New("failed to get zstd encoder from pool"))

			errChan <- errors.New("failed to get zstd encoder from pool")

			return
		}
		defer zstdEncoderPool.Put(encoder)

		encoder.Reset(pw)

		defer func() {
			if err := encoder.Close(); err != nil {
				slog.Error("Failed to close zstd encoder", "error", err)
			}
		}()

		// Serialize NAR directly to the compressed stream
		if err := DumpPath(encoder, storePath); err != nil {
			pw.CloseWithError(fmt.Errorf("serializing NAR: %w", err))

			errChan <- err

			return
		}

		errChan <- nil
	}()

	var info *CompressedFileInfo

	var err error

	switch {
	case pendingObj.MultipartInfo != nil:
		// Upload using multipart
		info, err = c.uploadMultipart(ctx, pr, pendingObj.MultipartInfo, objectKey)
	case pendingObj.PresignedURL != "":
		// Single-part upload (shouldn't happen for NARs, but just in case)
		return nil, errors.New("NAR files should use multipart upload")
	default:
		return nil, errors.New("no upload method provided")
	}

	// Check for compression errors
	if compressErr := <-errChan; compressErr != nil {
		return nil, compressErr
	}

	if err != nil {
		return nil, err
	}

	slog.Info("Uploaded NAR", "object_key", objectKey, "size", info.Size, "hash", info.Hash)

	return info, nil
}

func (c *Client) putBytes(ctx context.Context, url string, data []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(data)))
	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("uploading: %w", err)
	}

	return resp, nil
}

// uploadMultipart uploads a stream in parts using presigned URLs (sequential).
func (c *Client) uploadMultipart(ctx context.Context, r io.Reader, multipartInfo *MultipartUploadInfo, objectKey string) (*CompressedFileInfo, error) {
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

	for partNumber <= len(multipartInfo.PartURLs) {
		// Read up to multipartPartSize for this part
		n, err := io.ReadFull(r, buffer)
		if errors.Is(err, io.EOF) {
			// Done reading
			break
		}

		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("reading part %d: %w", partNumber, err)
		}

		partData := buffer[:n]

		// Update hash and size
		hasher.Write(partData)
		//nolint:gosec // n is from io.ReadFull which returns valid int
		totalSize.Add(uint64(n))

		// Upload this part
		partURL := multipartInfo.PartURLs[partNumber-1]

		etag, err := c.uploadPart(ctx, partURL, partData)
		if err != nil {
			return nil, fmt.Errorf("uploading part %d: %w", partNumber, err)
		}

		completedParts = append(completedParts, CompletedPart{
			PartNumber: partNumber,
			ETag:       etag,
		})

		slog.Info("Uploaded part", "part_number", partNumber, "total_parts", len(multipartInfo.PartURLs), "bytes", n, "object_key", objectKey)

		partNumber++

		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
	}

	// Complete the multipart upload
	err := c.CompleteMultipartUpload(ctx, objectKey, multipartInfo.UploadID, completedParts)
	if err != nil {
		return nil, fmt.Errorf("completing multipart upload: %w", err)
	}

	slog.Info("Completed multipart upload", "object_key", objectKey)

	// Compute final hash
	hashBytes := hasher.Sum(nil)
	hash := "sha256:" + base64.StdEncoding.EncodeToString(hashBytes)

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

// CreateNarinfo generates a narinfo file content.
func CreateNarinfo(pathInfo *PathInfo, narFilename string, compressedSize uint64, fileHash string) string {
	var sb strings.Builder

	// StorePath
	fmt.Fprintf(&sb, "StorePath: %s\n", pathInfo.Path)

	// URL to the NAR file
	fmt.Fprintf(&sb, "URL: nar/%s\n", narFilename)

	// Compression
	fmt.Fprintf(&sb, "Compression: zstd\n")

	// NAR hash and size (uncompressed)
	fmt.Fprintf(&sb, "NarHash: %s\n", pathInfo.NarHash)
	fmt.Fprintf(&sb, "NarSize: %d\n", pathInfo.NarSize)

	// FileHash and FileSize for compressed file
	fmt.Fprintf(&sb, "FileHash: %s\n", fileHash)
	fmt.Fprintf(&sb, "FileSize: %d\n", compressedSize)

	// References (must have space after colon, even if empty)
	fmt.Fprint(&sb, "References:")

	for _, ref := range pathInfo.References {
		// Remove /nix/store/ prefix
		refName := strings.TrimPrefix(ref, "/nix/store/")
		fmt.Fprintf(&sb, " %s", refName)
	}

	// Always add a space after "References:" even if empty
	if len(pathInfo.References) == 0 {
		fmt.Fprint(&sb, " ")
	}

	fmt.Fprint(&sb, "\n")

	// Deriver (optional)
	if pathInfo.Deriver != nil {
		deriverName := strings.TrimPrefix(*pathInfo.Deriver, "/nix/store/")
		fmt.Fprintf(&sb, "Deriver: %s\n", deriverName)
	}

	// Signatures (optional)
	if len(pathInfo.Signatures) > 0 {
		for _, sig := range pathInfo.Signatures {
			fmt.Fprintf(&sb, "Sig: %s\n", sig)
		}
	}

	// CA (optional)
	if pathInfo.CA != nil {
		fmt.Fprintf(&sb, "CA: %s\n", *pathInfo.CA)
	}

	return sb.String()
}
