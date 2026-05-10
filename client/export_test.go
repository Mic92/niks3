package client

import (
	"net/http"

	"github.com/Mic92/niks3/ratelimit"
)

// ParsePathInfoJSON exports parsePathInfoJSON for testing.
var ParsePathInfoJSON = parsePathInfoJSON //nolint:gochecknoglobals // test-only re-export

// ShellSplit re-exports shellSplit for the external test package.
var ShellSplit = shellSplit //nolint:gochecknoglobals // test-only re-export

// ScriptTokenWithClock builds a ScriptToken with an injected clock for tests.
var ScriptTokenWithClock = scriptToken //nolint:gochecknoglobals // test-only re-export

// HTTPClient exposes the underlying http.Client for testing.
func (c *Client) HTTPClient() *http.Client {
	return c.httpClient
}

// NewTestClient creates a Client for testing with a custom HTTP client and retry config.
func NewTestClient(httpClient *http.Client, retry RetryConfig) *Client {
	return NewTestClientWithToken(httpClient, retry, StaticToken(""))
}

// NewTestClientWithToken is like NewTestClient but with an explicit TokenSource.
func NewTestClientWithToken(httpClient *http.Client, retry RetryConfig, ts TokenSource) *Client {
	return &Client{
		httpClient:        httpClient,
		tokenSource:       ts,
		Retry:             retry,
		S3RateLimiter:     ratelimit.NewAdaptiveRateLimiter(0, "s3-test"),
		ServerRateLimiter: ratelimit.NewAdaptiveRateLimiter(0, "server-test"),
	}
}
