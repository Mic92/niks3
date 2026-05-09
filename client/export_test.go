package client

import (
	"net/http"

	"github.com/Mic92/niks3/ratelimit"
)

// ParsePathInfoJSON exports parsePathInfoJSON for testing.
var ParsePathInfoJSON = parsePathInfoJSON

// NewTestClient creates a Client for testing with a custom HTTP client and retry config.
func NewTestClient(httpClient *http.Client, retry RetryConfig) *Client {
	return &Client{
		httpClient:        httpClient,
		Retry:             retry,
		S3RateLimiter:     ratelimit.NewAdaptiveRateLimiter(0, "s3-test"),
		ServerRateLimiter: ratelimit.NewAdaptiveRateLimiter(0, "server-test"),
	}
}
