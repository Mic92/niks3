package client

import "net/http"

// NewTestClient creates a Client for testing with a custom HTTP client and retry config.
func NewTestClient(httpClient *http.Client, retry RetryConfig) *Client {
	return &Client{
		httpClient: httpClient,
		Retry:      retry,
	}
}
