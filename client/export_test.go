package client

import (
	"net/http"
	"net/url"

	"github.com/Mic92/niks3/ratelimit"
)

// ParsePathInfoJSON exports parsePathInfoJSON for testing.
var ParsePathInfoJSON = parsePathInfoJSON //nolint:gochecknoglobals // test-only re-export

// ShellSplit re-exports shellSplit for the external test package.
var ShellSplit = shellSplit //nolint:gochecknoglobals // test-only re-export

// PartSizeForNAR re-exports partSizeForNAR for the external test package.
var PartSizeForNAR = partSizeForNAR //nolint:gochecknoglobals // test-only re-export

// MultipartPartSize re-exports the default part size for tests.
const MultipartPartSize = multipartPartSize

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
		ConflictRetry:     DefaultPendingClosureConflictRetry(),
		S3RateLimiter:     ratelimit.NewAdaptiveRateLimiter(0, "s3-test"),
		ServerRateLimiter: ratelimit.NewAdaptiveRateLimiter(0, "server-test"),
	}
}

// NewTestClientWithStoreDir creates a Client with only storeDir set, for path resolution tests.
func NewTestClientWithStoreDir(storeDir string) *Client {
	return &Client{storeDir: storeDir}
}

// NewTestClientForServer returns a Client wired to talk to a test HTTP server
// at serverURL. conflictRetry sets the 409 backoff used by CreatePendingClosure.
func NewTestClientForServer(serverURL string, conflictRetry RetryConfig) (*Client, error) {
	baseURL, err := url.Parse(serverURL)
	if err != nil {
		return nil, err //nolint:wrapcheck // test helper
	}

	return &Client{
		baseURL:           baseURL,
		httpClient:        &http.Client{},
		tokenSource:       StaticToken(""),
		Retry:             DefaultRetryConfig(),
		ConflictRetry:     conflictRetry,
		S3RateLimiter:     ratelimit.NewAdaptiveRateLimiter(0, "s3-test"),
		ServerRateLimiter: ratelimit.NewAdaptiveRateLimiter(0, "server-test"),
	}, nil
}
