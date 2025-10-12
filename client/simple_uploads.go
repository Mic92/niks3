package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

// UploadBytesToPresignedURL uploads bytes to a presigned URL.
func (c *Client) UploadBytesToPresignedURL(ctx context.Context, presignedURL string, data []byte) error {
	return c.UploadBytesToPresignedURLWithHeaders(ctx, presignedURL, data, nil)
}

// UploadBytesToPresignedURLWithHeaders uploads bytes to a presigned URL with optional custom headers.
func (c *Client) UploadBytesToPresignedURLWithHeaders(ctx context.Context, presignedURL string, data []byte, headers map[string]string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")

	// Add custom headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("uploading: %w", err)
	}

	defer deferCloseBody(resp)

	return checkResponse(resp, http.StatusOK, http.StatusNoContent)
}

// UploadNarinfoToPresignedURL compresses a narinfo file with zstd and uploads it with Content-Encoding header.
// This follows Nix's convention for compressed narinfo files.
func (c *Client) UploadNarinfoToPresignedURL(ctx context.Context, presignedURL string, narinfoContent []byte) error {
	// Compress narinfo content with zstd using pooled encoder
	var compressed bytes.Buffer

	encoder, ok := zstdEncoderPool.Get().(*zstd.Encoder)
	if !ok {
		return errors.New("failed to get zstd encoder from pool")
	}
	defer zstdEncoderPool.Put(encoder)

	encoder.Reset(&compressed)

	if _, err := encoder.Write(narinfoContent); err != nil {
		return fmt.Errorf("compressing narinfo: %w", err)
	}

	if err := encoder.Close(); err != nil {
		return fmt.Errorf("closing zstd encoder: %w", err)
	}

	// Upload with Content-Encoding header
	headers := map[string]string{
		"Content-Encoding": "zstd",
	}

	return c.UploadBytesToPresignedURLWithHeaders(ctx, presignedURL, compressed.Bytes(), headers)
}

// UploadListingToPresignedURL compresses a NAR listing with brotli and uploads it with Content-Encoding header.
// The listing is stored as a .ls file, compatible with Nix's lazy NAR accessor format.
func (c *Client) UploadListingToPresignedURL(ctx context.Context, presignedURL string, listing *NarListing) error {
	// Compress listing with brotli
	compressed, err := CompressListingWithBrotli(listing)
	if err != nil {
		return fmt.Errorf("compressing listing: %w", err)
	}

	// Upload with Content-Encoding header
	headers := map[string]string{
		"Content-Encoding": "br",
	}

	return c.UploadBytesToPresignedURLWithHeaders(ctx, presignedURL, compressed, headers)
}

// UploadBuildLogToPresignedURL uploads a compressed build log with Content-Encoding header.
// This follows Nix's convention for compressed build logs stored at log/<drvPath>.
// The compressedInfo must point to a temporary file created by CompressBuildLog.
func (c *Client) UploadBuildLogToPresignedURL(ctx context.Context, presignedURL string, compressedInfo *CompressedBuildLogInfo) error {
	// Open file for mmap
	file, err := os.Open(compressedInfo.TempFile)
	if err != nil {
		return fmt.Errorf("opening compressed log: %w", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("Failed to close file", "error", err)
		}
	}()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	fileSize := stat.Size()

	var reader *bytes.Reader

	var mmapData []byte // Hold reference for defer

	if fileSize == 0 {
		// Empty file - can't mmap, just use empty reader
		reader = bytes.NewReader([]byte{})
	} else {
		// Memory-map the file (kernel handles paging, efficient for large files)
		var err error

		mmapData, err = unix.Mmap(int(file.Fd()), 0, int(fileSize), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			return fmt.Errorf("mmap file: %w", err)
		}
		// Wrap mmap'd data in bytes.Reader so Go's HTTP client properly sets Content-Length
		reader = bytes.NewReader(mmapData)
	}

	// Ensure munmap happens after HTTP request completes
	defer func() {
		if mmapData != nil {
			if err := unix.Munmap(mmapData); err != nil {
				slog.Error("Failed to unmap file", "error", err)
			}
		}
	}()

	// Create upload request (Go automatically sets ContentLength for bytes.Reader)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, reader)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Content-Encoding", "zstd")

	// Upload
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("uploading: %w", err)
	}
	defer deferCloseBody(resp)

	return checkResponse(resp, http.StatusOK, http.StatusNoContent)
}
