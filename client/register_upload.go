package client

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// registrationTimeout bounds one registration request including retries.
const registrationTimeout = 2 * time.Minute

type completeUploadRequest struct {
	ObjectKey string `json:"object_key"`
}

// RegisterUploadedObject notifies the server that objectKey was uploaded via a
// presigned PUT, so the object is recorded even if the closure never commits.
// Best effort and off the upload path: the closure commit records the object
// too, so nothing waits on this except WaitRegistrations.
func (c *Client) RegisterUploadedObject(ctx context.Context, objectKey string) {
	reqURL := c.baseURL.JoinPath("api/uploads/complete")

	c.registrations.Go(func() error {
		// Survives cancellation of the push, but not indefinitely: a server
		// that accepts connections and never answers would otherwise hold a
		// slot of the registrations group for as long as the kernel keeps the
		// connection, block upload workers once the group is full, and keep
		// WaitRegistrations from returning after Ctrl-C.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), registrationTimeout)
		defer cancel()

		err := c.doJSONRequest(ctx, http.MethodPost, reqURL.String(),
			completeUploadRequest{ObjectKey: objectKey}, nil, http.StatusOK, http.StatusNoContent)
		if err != nil {
			slog.Warn("Failed to register uploaded object", "key", objectKey, "error", err)
		}

		return nil
	})
}

// WaitRegistrations blocks until all RegisterUploadedObject calls have finished.
func (c *Client) WaitRegistrations() {
	_ = c.registrations.Wait()
}
