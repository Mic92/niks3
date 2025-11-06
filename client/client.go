package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
)

// Client handles uploads to the niks3 server.
type Client struct {
	baseURL                 *url.URL
	authToken               string
	httpClient              *http.Client
	MaxConcurrentNARUploads int         // Maximum number of concurrent uploads (0 = unlimited)
	NixEnv                  []string    // Optional environment variables for nix commands (for testing)
	Retry                   RetryConfig // Retry configuration for HTTP requests
	storeDir                string      // Cached Nix store directory (e.g., "/nix/store"), lazily initialized
}

// ObjectType classifies cache objects by their purpose and upload strategy.
type ObjectType string

const (
	ObjectTypeNarinfo     ObjectType = "narinfo"
	ObjectTypeListing     ObjectType = "listing"
	ObjectTypeBuildLog    ObjectType = "build_log"
	ObjectTypeNAR         ObjectType = "nar"
	ObjectTypeRealisation ObjectType = "realisation"
)

// ObjectWithRefs represents an object with its dependencies.
type ObjectWithRefs struct {
	Key     string     `json:"key"`
	Type    ObjectType `json:"type"`
	Refs    []string   `json:"refs"`
	NarSize *uint64    `json:"nar_size,omitempty"` // For estimating multipart parts
}

// NewClient creates a new upload client.
// The default MaxConcurrentNARUploads is set to 16, optimized for I/O-bound upload workloads.
// This is comparable to browser HTTP/2 connection limits and Cachix's default of 8.
//
// TODO: Test this value in various network setups (local network, high-latency WAN,
// rate-limited connections) to determine optimal defaults for different scenarios.
func NewClient(serverURL, authToken string) (*Client, error) {
	baseURL, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("parsing server URL: %w", err)
	}

	// Get the Nix store directory at startup
	ctx := context.Background()
	storeDir, err := GetStoreDir(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("getting store directory: %w", err)
	}

	return &Client{
		baseURL:   baseURL,
		authToken: authToken,
		httpClient: &http.Client{
			Timeout: 0, // No timeout for streaming uploads
		},
		MaxConcurrentNARUploads: 16,
		Retry:                   DefaultRetryConfig(),
		storeDir:                storeDir,
	}, nil
}

func deferCloseBody(resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		slog.Error("Failed to close response body", "error", err)
	}
}

func checkResponse(resp *http.Response, acceptedStatuses ...int) error {
	for _, status := range acceptedStatuses {
		if resp.StatusCode == status {
			return nil
		}
	}

	body, _ := io.ReadAll(resp.Body)

	return fmt.Errorf("server returned %d: %s", resp.StatusCode, body)
}

func (c *Client) putBytes(ctx context.Context, url string, data []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.DoWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("uploading: %w", err)
	}

	return resp, nil
}
