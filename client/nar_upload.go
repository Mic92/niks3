package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/klauspost/compress/zstd"
)

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

// CompressedFileInfo contains information about a compressed file.
type CompressedFileInfo struct {
	Size    uint64
	Hash    string
	Listing *NarListing // Directory listing (if generated)
}

// CompressAndUploadNAR compresses a NAR and uploads it using multipart upload.
// It also generates a directory listing during serialization.
func (c *Client) CompressAndUploadNAR(ctx context.Context, storePath string, pendingObj PendingObject, objectKey string) (*CompressedFileInfo, error) {
	slog.Info("Compressing and uploading NAR", "store_path", storePath)

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

	// Get the listing
	listing := <-listingChan

	// Add listing to info
	if info != nil {
		info.Listing = listing
	}

	slog.Info("Uploaded NAR", "object_key", objectKey, "size", info.Size, "hash", info.Hash)

	return info, nil
}
