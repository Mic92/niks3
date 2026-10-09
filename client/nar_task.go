package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

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

	var (
		listingUpload  errgroup.Group
		listingStarted atomic.Bool
	)

	onListing := func(listing *NarListing) {
		if lsTask == nil || listing == nil {
			return
		}

		listingStarted.Store(true)

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
		if !errors.Is(err, ErrUploadSuperseded) {
			return fmt.Errorf("uploading NAR %s: %w", narTask.key, err)
		}

		slog.Debug("Skipping NAR superseded by concurrent upload", "key", narTask.key)

		// The peer uploaded the same NAR, but not necessarily this store path:
		// identical contents under different names share the NAR while each
		// path has its own listing key. The abort also cut the NAR dump short,
		// usually before it produced the listing, so generate it on its own.
		if lsTask != nil && !listingStarted.Load() {
			return c.uploadMetadataOnly(ctx, lsTask, pathInfo)
		}
	}

	return listingErr //nolint:wrapcheck // wrapped in the task
}
