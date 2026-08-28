package client

import (
	"context"
	"log/slog"
	"net/http"
)

type completeUploadRequest struct {
	ClosureID string `json:"closure_id"`
	ObjectKey string `json:"object_key"`
}

// RegisterUploadedObject notifies the server that objectKey was uploaded via a
// presigned PUT, so the object is recorded even if the closure never commits.
// Best effort: failures are logged, closure commit still records the object.
func (c *Client) RegisterUploadedObject(ctx context.Context, closureID, objectKey string) {
	reqURL := c.baseURL.JoinPath("api/uploads/complete")

	err := c.doJSONRequest(ctx, http.MethodPost, reqURL.String(),
		completeUploadRequest{ClosureID: closureID, ObjectKey: objectKey}, nil, http.StatusOK, http.StatusNoContent)
	if err != nil {
		slog.Warn("Failed to register uploaded object", "key", objectKey, "error", err)
	}
}
