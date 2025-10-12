package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// uploadNARWithListing uploads a NAR and its listing.
// Successfully uploaded NARs are stored in compressedInfo for later narinfo uploads.
func (c *Client) uploadNARWithListing(
	ctx context.Context,
	task genericUploadTask,
	pendingByHash map[string]struct {
		narTask     *uploadTask
		lsTask      *uploadTask
		narinfoTask *uploadTask
	},
	pathInfoByHash map[string]*PathInfo,
	compressedInfo map[string]*CompressedFileInfo,
	compressedInfoMu *sync.Mutex,
) error {
	// Upload NAR
	info, err := c.CompressAndUploadNAR(ctx, pathInfoByHash[task.hash].Path, task.task.obj, task.task.key)
	if err != nil {
		return fmt.Errorf("uploading NAR %s: %w", task.task.key, err)
	}

	// Store compressed info for narinfo phase (protected by mutex for concurrent writes)
	compressedInfoMu.Lock()

	compressedInfo[task.hash] = info

	compressedInfoMu.Unlock()

	// Upload listing immediately in same goroutine
	entry := pendingByHash[task.hash]
	if entry.lsTask != nil {
		if err := c.uploadListing(ctx, *entry.lsTask, info); err != nil {
			return err
		}
	}

	return nil
}

// uploadListing uploads a listing file.
func (c *Client) uploadListing(ctx context.Context, task uploadTask, info *CompressedFileInfo) error {
	if info.Listing == nil {
		return fmt.Errorf("listing not found for hash %s", task.hash)
	}

	// Upload listing with brotli compression
	if err := c.UploadListingToPresignedURL(ctx, task.obj.PresignedURL, info.Listing); err != nil {
		return fmt.Errorf("uploading listing %s: %w", task.key, err)
	}

	slog.Info("Uploaded listing", "key", task.key)

	return nil
}
