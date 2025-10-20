package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Mic92/niks3/api"
)

// RunGarbageCollection triggers garbage collection on the server for closures older than the specified duration.
// If force is true, objects will be deleted immediately without a grace period (may be dangerous).
// failedUploadsOlderThan specifies how old failed uploads must be before cleanup (server defaults to "6h" if empty).
// Returns statistics about the garbage collection run.
func (c *Client) RunGarbageCollection(ctx context.Context, olderThan string, failedUploadsOlderThan string, force bool) (*api.GCStats, error) {
	// Build the URL with query parameters
	gcURL := c.baseURL.JoinPath("/api/closures")
	query := gcURL.Query()
	query.Set("older-than", olderThan)

	if failedUploadsOlderThan != "" {
		query.Set("failed-uploads-older-than", failedUploadsOlderThan)
	}

	if force {
		query.Set("force", "true")
	}

	gcURL.RawQuery = query.Encode()

	// Create DELETE request
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, gcURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	// Add authentication
	req.Header.Set("Authorization", "Bearer "+c.authToken)

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer deferCloseBody(resp)

	// Check response
	if err := checkResponse(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("garbage collection failed: %w", err)
	}

	// Parse statistics from response
	var stats api.GCStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return &stats, nil
}
