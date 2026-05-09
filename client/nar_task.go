package client

import (
	"context"
	"fmt"
	"log/slog"
)

// uploadNARWithListing uploads a NAR and its listing.
func (c *Client) uploadNARWithListing(
	ctx context.Context,
	narTask uploadTask,
	lsTask *uploadTask,
	pathInfo *PathInfo,
) error {
	if pathInfo == nil {
		return fmt.Errorf("missing PathInfo for NAR %s", narTask.key)
	}

	listing, err := c.CompressAndUploadNAR(ctx, pathInfo.Path, pathInfo.NarSize, narTask.obj.MultipartInfo, narTask.key)
	if err != nil {
		return fmt.Errorf("uploading NAR %s: %w", narTask.key, err)
	}

	// Upload listing immediately in same goroutine
	if lsTask != nil && listing != nil {
		if err := c.UploadListingToPresignedURL(ctx, lsTask.obj.PresignedURL, listing); err != nil {
			return fmt.Errorf("uploading listing %s: %w", lsTask.key, err)
		}

		slog.Debug("Uploaded listing", "key", lsTask.key)
	}

	return nil
}
