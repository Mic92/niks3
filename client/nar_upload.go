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
func (c *Client) compressAndSimpleUploadNAR(ctx context.Context, storePath, presignedURL, objectKey string, onListing func(*NarListing)) error {
	encoder, ok := zstdEncoderPool.Get().(*zstd.Encoder)
	if !ok {
		return errors.New("failed to get zstd encoder from pool")
	}
	defer zstdEncoderPool.Put(encoder)

	var buf bytes.Buffer

	encoder.Reset(&buf)

	listing, err := DumpPathWithListing(encoder, storePath)
	if err != nil {
		return fmt.Errorf("serializing NAR: %w", err)
	}

	if err := encoder.Close(); err != nil {
		return fmt.Errorf("closing zstd encoder: %w", err)
	}

	onListing(listing)

	if err := c.UploadBytesToPresignedURLWithHeaders(ctx, presignedURL, buf.Bytes(), nil); err != nil {
		return fmt.Errorf("uploading NAR %s: %w", objectKey, err)
	}

	return nil
}

// CompressAndUploadNAR compresses a NAR and uploads it.
// Small NARs are sent with a single presigned PUT, larger ones via multipart upload.
// onListing gets the directory listing as soon as serialization finishes,
// while the NAR is still uploading; it must not block.
func (c *Client) CompressAndUploadNAR(ctx context.Context, storePath string, narSize uint64, obj PendingObject, objectKey string, onListing func(*NarListing)) error {
	name := filepath.Base(storePath)
	slog.Info(fmt.Sprintf("Uploading %s (%s)", name, formatBytes(narSize)))

	var err error

	if obj.MultipartInfo != nil {
		// The server records the object when the multipart upload completes.
		err = c.compressAndMultipartUploadNAR(ctx, storePath, narSize, obj.MultipartInfo, objectKey, onListing)
	} else {
		err = c.compressAndSimpleUploadNAR(ctx, storePath, obj.PresignedURL, objectKey, onListing)
		if err == nil {
			c.RegisterUploadedObject(ctx, objectKey)
		}
	}

	if err != nil {
		return err
	}

	slog.Debug("Uploaded NAR", "object_key", objectKey)

	return nil
}

// compressAndMultipartUploadNAR streams a compressed NAR through a multipart upload.
func (c *Client) compressAndMultipartUploadNAR(ctx context.Context, storePath string, narSize uint64, multipartInfo *MultipartUploadInfo, objectKey string, onListing func(*NarListing)) error {
	produce := func(w io.Writer) error {
		encoder, ok := zstdEncoderPool.Get().(*zstd.Encoder)
		if !ok {
			return errors.New("failed to get zstd encoder from pool")
		}
		defer zstdEncoderPool.Put(encoder)

		encoder.Reset(w)

		listing, err := DumpPathWithListing(encoder, storePath)
		if err != nil {
			_ = encoder.Close()

			return fmt.Errorf("serializing NAR: %w", err)
		}

		onListing(listing)

		if err := encoder.Close(); err != nil {
			return fmt.Errorf("closing zstd encoder: %w", err)
		}

		return nil
	}

	return c.uploadMultipart(ctx, produce, multipartInfo, objectKey, partSizeForNAR(narSize))
}
