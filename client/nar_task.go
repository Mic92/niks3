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
	task uploadTask,
	pendingByHash pendingObjectsByHash,
	pathInfoByHash map[string]*PathInfo,
	compressedInfo map[string]struct{},
	compressedInfoMu *sync.Mutex,
) error {
	// Upload NAR
	pathInfo, ok := pathInfoByHash[task.hash]
	if !ok || pathInfo == nil {
		return fmt.Errorf("missing PathInfo for hash %s", task.hash)
	}

	listing, err := c.CompressAndUploadNAR(ctx, pathInfo.Path, pathInfo.NarSize, task.obj, task.key)
	if err != nil {
		return fmt.Errorf("uploading NAR %s: %w", task.key, err)
	}

	// Upload listing immediately in same goroutine
	entry := pendingByHash[task.hash]
	if entry.lsTask != nil {
		if err := c.uploadListing(ctx, *entry.lsTask, listing); err != nil {
			return err
		}
	}

	// Mark hash as uploaded for phase 2 narinfo metadata collection
	compressedInfoMu.Lock()
	compressedInfo[task.hash] = struct{}{}
	compressedInfoMu.Unlock()

	return nil
}

// uploadListing uploads a listing file.
func (c *Client) uploadListing(ctx context.Context, task uploadTask, listing *NarListing) error {
	if listing == nil {
		return fmt.Errorf("listing not found for hash %s", task.hash)
	}

	// Upload listing with brotli compression
	if err := c.UploadListingToPresignedURL(ctx, task.obj.PresignedURL, listing); err != nil {
		return fmt.Errorf("uploading listing %s: %w", task.key, err)
	}

	slog.Debug("Uploaded listing", "key", task.key)

	return nil
}
