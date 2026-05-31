package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstdEncoderPool pools zstd encoders to reduce memory allocations.
// Each encoder maintains compression history buffers (~60-80MB) that can be reused.
var zstdEncoderPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool should be global
	New: func() any {
		// Create encoder with nil writer (will use Reset() to set the actual writer)
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			// This should never happen with nil writer
			panic(fmt.Sprintf("failed to create zstd encoder: %v", err))
		}

		return encoder
	},
}

// compressAndSimpleUploadNAR uploads a small NAR with a single presigned PUT.
// The compressed NAR is stored as opaque bytes with no Content-Encoding (like multipart part upload);
// nix-daemon decompresses it per the narinfo Compression field.
func (c *Client) compressAndSimpleUploadNAR(ctx context.Context, storePath, presignedURL, objectKey string) (*NarListing, error) {
	encoder, ok := zstdEncoderPool.Get().(*zstd.Encoder)
	if !ok {
		return nil, errors.New("failed to get zstd encoder from pool")
	}
	defer zstdEncoderPool.Put(encoder)

	var buf bytes.Buffer

	encoder.Reset(&buf)

	listing, err := DumpPathWithListing(encoder, storePath)
	if err != nil {
		return nil, fmt.Errorf("serializing NAR: %w", err)
	}

	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("closing zstd encoder: %w", err)
	}

	if err := c.UploadBytesToPresignedURLWithHeaders(ctx, presignedURL, buf.Bytes(), nil); err != nil {
		return nil, fmt.Errorf("uploading NAR %s: %w", objectKey, err)
	}

	return listing, nil
}

// CompressAndUploadNAR compresses a NAR and uploads it.
// Small NARs are sent with a single presigned PUT, larger ones via multipart upload.
// It also generates a directory listing during serialization.
func (c *Client) CompressAndUploadNAR(ctx context.Context, storePath string, narSize uint64, obj PendingObject, objectKey string) (*NarListing, error) {
	name := filepath.Base(storePath)
	slog.Info(fmt.Sprintf("Uploading %s (%s)", name, formatBytes(narSize)))

	var (
		listing *NarListing
		err     error
	)

	if obj.MultipartInfo != nil {
		listing, err = c.compressAndMultipartUploadNAR(ctx, storePath, narSize, obj.MultipartInfo, objectKey)
	} else {
		listing, err = c.compressAndSimpleUploadNAR(ctx, storePath, obj.PresignedURL, objectKey)
	}

	if err != nil {
		return nil, err
	}

	slog.Debug("Uploaded NAR", "object_key", objectKey)

	return listing, nil
}

// compressAndMultipartUploadNAR streams a compressed NAR through a multipart upload.
func (c *Client) compressAndMultipartUploadNAR(ctx context.Context, storePath string, narSize uint64, multipartInfo *MultipartUploadInfo, objectKey string) (*NarListing, error) {
	// Create a pipe for streaming: NAR serialization -> zstd compression -> hash/size tracking
	pr, pw := io.Pipe()

	// Channels to receive errors and listing from the compression goroutine
	errChan := make(chan error, 1)
	listingChan := make(chan *NarListing, 1)

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

			listingChan <- nil

			return
		}
		defer zstdEncoderPool.Put(encoder)

		encoder.Reset(pw)

		defer func() {
			if err := encoder.Close(); err != nil {
				slog.Error("Failed to close zstd encoder", "error", err)
			}
		}()

		// Serialize NAR with listing directly to the compressed stream
		listing, err := DumpPathWithListing(encoder, storePath)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("serializing NAR: %w", err))

			errChan <- err

			listingChan <- nil

			return
		}

		errChan <- nil

		listingChan <- listing
	}()

	err := c.uploadMultipart(ctx, pr, multipartInfo, objectKey, partSizeForNAR(narSize))
	// If upload failed, signal compressor to stop and wait for it to exit
	if err != nil {
		_ = pw.CloseWithError(err)

		<-errChan // drain to prevent goroutine leak

		return nil, err
	}

	// Check for compression errors
	if compressErr := <-errChan; compressErr != nil {
		return nil, compressErr
	}

	return <-listingChan, nil
}
