package client

import (
	"context"
	"net/http"
	"net/url"
	"sync"

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

// NewStubClient builds a Client whose server requests target baseURL and
// whose rate limiters are unbounded — suitable for httptest-backed tests
// that drive internal upload paths directly.
func NewStubClient(baseURL *url.URL) *Client {
	return &Client{
		baseURL:           baseURL,
		authToken:         "test-token",
		httpClient:        &http.Client{},
		Retry:             RetryConfig{MaxRetries: 0},
		S3RateLimiter:     ratelimit.NewAdaptiveRateLimiter(0, "s3-test"),
		ServerRateLimiter: ratelimit.NewAdaptiveRateLimiter(0, "server-test"),
	}
}

// UploadNARAndListingForTest wraps the unexported uploadNARWithListing so
// tests in client_test can drive a single NAR + .ls upload end-to-end.
func (c *Client) UploadNARAndListingForTest(
	ctx context.Context,
	storeDir, hash, narKey, lsKey, lsURL string,
	narSize uint64,
	multipart *MultipartUploadInfo,
	compressedInfo map[string]*CompressedFileInfo,
	mu *sync.Mutex,
) error {
	pathInfoByHash := map[string]*PathInfo{
		hash: {Path: storeDir, NarSize: narSize},
	}

	pendingByHash := pendingObjectsByHash{
		hash: {
			narTask: &uploadTask{
				key:  narKey,
				obj:  PendingObject{Type: "nar", MultipartInfo: multipart},
				hash: hash,
			},
			lsTask: &uploadTask{
				key:  lsKey,
				obj:  PendingObject{Type: "listing", PresignedURL: lsURL},
				hash: hash,
			},
		},
	}

	task := genericUploadTask{
		taskType: "nar",
		task:     *pendingByHash[hash].narTask,
		hash:     hash,
	}

	return c.uploadNARWithListing(ctx, task, pendingByHash, pathInfoByHash, compressedInfo, mu)
}
