package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
)

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

	resp, err := c.DoS3Request(ctx, req)
	if err != nil {
		return fmt.Errorf("uploading: %w", err)
	}

	defer deferCloseBody(resp)

	return checkResponse(resp, http.StatusOK, http.StatusNoContent)
}

// UploadListingToPresignedURL compresses a NAR listing with zstd and uploads it with Content-Encoding header.
// The listing is stored as a .ls file, compatible with Nix's lazy NAR accessor format.
func (c *Client) UploadListingToPresignedURL(ctx context.Context, presignedURL string, listing *NarListing) error {
	// Compress listing with zstd
	compressed, err := CompressListingWithZstd(listing)
	if err != nil {
		return fmt.Errorf("compressing listing: %w", err)
	}

	// Upload with Content-Encoding header
	headers := map[string]string{
		"Content-Encoding": compressionZstd,
	}

	return c.UploadBytesToPresignedURLWithHeaders(ctx, presignedURL, compressed, headers)
}

// UploadBuildLogToPresignedURL uploads a compressed build log with Content-Encoding header.
// This follows Nix's convention for compressed build logs stored at log/<drvPath>.
// The compressedInfo must point to a temporary file created by CompressBuildLog.
func (c *Client) UploadBuildLogToPresignedURL(ctx context.Context, presignedURL string, compressedInfo *CompressedBuildLogInfo) error {
	stat, err := os.Stat(compressedInfo.TempFile)
	if err != nil {
		return fmt.Errorf("stat compressed log: %w", err)
	}

	fileSize := stat.Size()

	// The body is the file itself, reopened for every attempt, and the
	// transport closes it. An earlier version mmap'd the file and unmapped it
	// as soon as Do returned, but the transport may still be writing the body
	// from another goroutine when the server answers before the upload is
	// finished (an early 403 or 503), and a read from an unmapped page kills
	// the process. A read from a closed file is at worst an error.
	open := func() (io.ReadCloser, error) {
		if fileSize == 0 {
			return http.NoBody, nil
		}

		file, err := os.Open(compressedInfo.TempFile)
		if err != nil {
			return nil, fmt.Errorf("opening compressed log: %w", err)
		}

		return file, nil
	}

	body, err := open()
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, body)
	if err != nil {
		_ = body.Close()

		return fmt.Errorf("creating request: %w", err)
	}

	req.ContentLength = fileSize
	req.GetBody = open

	// Set headers
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Content-Encoding", compressionZstd)

	// Upload
	resp, err := c.DoS3Request(ctx, req)
	if err != nil {
		return fmt.Errorf("uploading: %w", err)
	}
	defer deferCloseBody(resp)

	return checkResponse(resp, http.StatusOK, http.StatusCreated, http.StatusNoContent)
}
