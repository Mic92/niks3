package client

import (
	"context"
	"fmt"
	"log/slog"
)

// uploadNARWithListing uploads a NAR and its listing.
// Successfully uploaded NARs are stored in compressedInfo for later narinfo uploads.
func (c *Client) uploadNARWithListing(
	ctx context.Context,
	narTask uploadTask,
	lsTask *uploadTask,
	pathInfo *PathInfo,
) error {
	if pathInfo == nil {
		return fmt.Errorf("missing PathInfo for hash %s", narTask.hash)
	}

	listing, err := c.CompressAndUploadNAR(ctx, pathInfo.Path, pathInfo.NarSize, narTask.obj, narTask.key)
	if err != nil {
		return fmt.Errorf("uploading NAR %s: %w", narTask.key, err)
	}

	// Upload listing immediately in same goroutine
	if lsTask != nil {
		if err := c.uploadListing(ctx, *lsTask, listing); err != nil {
			return err
		}
	}

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
