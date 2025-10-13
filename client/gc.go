package client

import (
	"context"
	"fmt"
	"net/http"
)

// RunGarbageCollection triggers garbage collection on the server for closures older than the specified duration.
// If force is true, objects will be deleted immediately without a grace period (may be dangerous).
func (c *Client) RunGarbageCollection(ctx context.Context, olderThan string, force bool) error {
	// Build the URL with query parameters
	gcURL := c.baseURL.JoinPath("/api/closures")
	query := gcURL.Query()
	query.Set("older-than", olderThan)

	if force {
		query.Set("force", "true")
	}

	gcURL.RawQuery = query.Encode()

	// Create DELETE request
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, gcURL.String(), nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	// Add authentication
	req.Header.Set("Authorization", "Bearer "+c.authToken)

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing request: %w", err)
	}
	defer deferCloseBody(resp)

	// Check response
	if err := checkResponse(resp, http.StatusNoContent); err != nil {
		return fmt.Errorf("garbage collection failed: %w", err)
	}

	return nil
}
