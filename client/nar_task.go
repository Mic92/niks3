package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"
)

// uploadNARWithListing uploads a NAR and, in parallel, its listing.
func (c *Client) uploadNARWithListing(
	ctx context.Context,
	narTask uploadTask,
	lsTask *uploadTask,
	pathInfo *PathInfo,
) error {
	if pathInfo == nil {
		return fmt.Errorf("missing PathInfo for NAR %s", narTask.key)
	}

	var listingUpload errgroup.Group

	onListing := func(listing *NarListing) {
		if lsTask == nil || listing == nil {
			return
		}

		listingUpload.Go(func() error {
			if err := c.UploadListingToPresignedURL(ctx, lsTask.obj.PresignedURL, listing); err != nil {
				return fmt.Errorf("uploading listing %s: %w", lsTask.key, err)
			}

			c.RegisterUploadedObject(ctx, lsTask.key)
			slog.Debug("Uploaded listing", "key", lsTask.key)

			return nil
		})
	}

	err := c.CompressAndUploadNAR(ctx, pathInfo.Path, pathInfo.NarSize, narTask.obj, narTask.key, onListing)
	listingErr := listingUpload.Wait()

	if err != nil {
		if errors.Is(err, ErrUploadSuperseded) {
			// A peer already uploaded this NAR (and its listing); nothing to do.
			slog.Debug("Skipping NAR superseded by concurrent upload", "key", narTask.key)

			return nil
		}

		return fmt.Errorf("uploading NAR %s: %w", narTask.key, err)
	}

	return listingErr //nolint:wrapcheck // wrapped in the task
}
