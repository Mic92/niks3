package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/klauspost/compress/zstd"
)

// compressWithZstd compresses data using zstd compression.
// Uses the pooled encoder from nar_upload.go for efficiency.
func compressWithZstd(data []byte) ([]byte, error) {
	var compressed bytes.Buffer

	encoder, ok := zstdEncoderPool.Get().(*zstd.Encoder)
	if !ok {
		return nil, errors.New("failed to get zstd encoder from pool")
	}
	defer zstdEncoderPool.Put(encoder)

	encoder.Reset(&compressed)

	if _, err := encoder.Write(data); err != nil {
		return nil, fmt.Errorf("compressing data: %w", err)
	}

	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("closing zstd encoder: %w", err)
	}

	return compressed.Bytes(), nil
}

// uploadLog uploads a build log.
func (c *Client) uploadLog(ctx context.Context, task uploadTask, logPathsByKey map[string]string) error {
	// Get the local log path
	logPath, ok := logPathsByKey[task.key]
	if !ok {
		// Log was requested by server but not found locally - this shouldn't happen
		// but we'll log a warning and continue rather than failing the entire upload
		slog.Warn("Build log not found", "key", task.key)

		return nil // Don't fail the entire upload
	}

	// Compress the log to a temporary file
	compressedInfo, err := CompressBuildLog(logPath)
	if err != nil {
		slog.Warn("Failed to compress build log", "key", task.key, "log_path", logPath, "error", err)

		return nil // Don't fail the entire upload
	}

	defer func() {
		if cleanupErr := compressedInfo.Cleanup(); cleanupErr != nil {
			slog.Warn("Failed to cleanup compressed build log", "key", task.key, "error", cleanupErr)
		}
	}()

	// Upload the compressed log
	if err := c.UploadBuildLogToPresignedURL(ctx, task.obj.PresignedURL, compressedInfo); err != nil {
		return fmt.Errorf("uploading build log %s: %w", task.key, err)
	}

	slog.Debug("Uploaded build log", "key", task.key)

	return nil
}

// uploadRealisation uploads a realisation (.doi) file for CA derivations.
func (c *Client) uploadRealisation(ctx context.Context, task uploadTask, realisationsByKey map[string]*RealisationInfo) error {
	// Get the realisation info
	realisationInfo, ok := realisationsByKey[task.key]
	if !ok {
		// Realisation was requested by server but not found locally - this shouldn't happen
		// but we'll log a warning and continue rather than failing the entire upload
		slog.Warn("Realisation not found", "key", task.key)

		return nil // Don't fail the entire upload
	}

	// Marshal realisation to JSON
	jsonData, err := json.Marshal(realisationInfo)
	if err != nil {
		return fmt.Errorf("marshaling realisation %s: %w", task.key, err)
	}

	// Compress with zstd (like narinfo)
	compressed, err := compressWithZstd(jsonData)
	if err != nil {
		return fmt.Errorf("compressing realisation %s: %w", task.key, err)
	}

	// Upload with Content-Encoding header
	headers := map[string]string{
		"Content-Encoding": "zstd",
	}

	if err := c.UploadBytesToPresignedURLWithHeaders(ctx, task.obj.PresignedURL, compressed, headers); err != nil {
		return fmt.Errorf("uploading realisation %s: %w", task.key, err)
	}

	slog.Debug("Uploaded realisation", "key", task.key)

	return nil
}
