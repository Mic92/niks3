package client

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Mic92/niks3/ratelimit"
)

// ParsePathInfoJSON exports parsePathInfoJSON for testing.
var ParsePathInfoJSON = parsePathInfoJSON //nolint:gochecknoglobals // test-only re-export

// ShellSplit re-exports shellSplit for the external test package.
var ShellSplit = shellSplit //nolint:gochecknoglobals // test-only re-export

// PartSizeForNAR re-exports partSizeForNAR for the external test package.
var PartSizeForNAR = partSizeForNAR //nolint:gochecknoglobals // test-only re-export

// FilterOversizedClosures re-exports filterOversizedClosures for the external test package.
var FilterOversizedClosures = filterOversizedClosures //nolint:gochecknoglobals // test-only re-export

// MultipartPartSize re-exports the default part size for tests.
const MultipartPartSize = multipartPartSize

// ScriptTokenWithClock builds a ScriptToken with an injected clock for tests.
var ScriptTokenWithClock = scriptToken //nolint:gochecknoglobals // test-only re-export

// SetUseCaseHack forces macOS case-hack handling for tests and returns the old value.
func SetUseCaseHack(on bool) bool {
	old := useCaseHack
	useCaseHack = on

	return old
}

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
	if httpClient.Transport == nil {
		httpClient.Transport = newTransport()
	}

	c := &Client{
		httpClient:        httpClient,
		tokenSource:       ts,
		Retry:             retry,
		S3RateLimiter:     ratelimit.NewAdaptiveRateLimiter(0, "s3-test"),
		ServerRateLimiter: ratelimit.NewAdaptiveRateLimiter(0, "server-test"),
	}
	c.registrations.SetLimit(maxConnsPerHost)

	return c
}

// NewTestClientWithStoreDir creates a Client with only storeDir set, for path resolution tests.
func NewTestClientWithStoreDir(storeDir string) *Client {
	return &Client{storeDir: storeDir}
}

// NewTestClientForServer returns a Client whose server requests target serverURL.
func NewTestClientForServer(serverURL string) (*Client, error) {
	baseURL, err := url.Parse(serverURL)
	if err != nil {
		return nil, err //nolint:wrapcheck // test helper
	}

	c := NewTestClient(&http.Client{}, DefaultRetryConfig())
	c.baseURL = baseURL

	return c, nil
}

// UploadMultipart re-exports uploadMultipart for the external test package.
func (c *Client) UploadMultipart(ctx context.Context, r io.Reader, info *MultipartUploadInfo, objectKey string, partSize int) error {
	return c.uploadMultipart(ctx, r, info, objectKey, partSize)
}

// RecordSignatures re-exports (*Client).recordSignatures for tests.
func (c *Client) RecordSignatures(narinfos map[string]NarinfoMetadata, signatures map[string][]string) {
	c.recordSignatures(narinfos, signatures)
}

// UploadNARWithListing re-exports uploadNARWithListing for the external test package.
func (c *Client) UploadNARWithListing(ctx context.Context, narKey string, narObj PendingObject, lsKey string, lsObj PendingObject, pathInfo *PathInfo) error {
	return c.uploadNARWithListing(ctx, uploadTask{key: narKey, obj: narObj}, &uploadTask{key: lsKey, obj: lsObj}, pathInfo)
}

// lifoPartBuffers hands back the buffer returned most recently, so a test
// can rely on a released part buffer being the next one taken; sync.Pool
// makes no such promise (and under -race drops some Puts on purpose).
type lifoPartBuffers struct {
	mu   sync.Mutex
	free []*[]byte
}

func (p *lifoPartBuffers) Get() any {
	p.mu.Lock()
	defer p.mu.Unlock()

	if n := len(p.free); n > 0 {
		buf := p.free[n-1]
		p.free = p.free[:n-1]

		return buf
	}

	buf := make([]byte, multipartPartSize)

	return &buf
}

func (p *lifoPartBuffers) Put(x any) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if buf, ok := x.(*[]byte); ok {
		p.free = append(p.free, buf)
	}
}

// SetRegistrationTimeout shortens the bound on one upload registration.
func (c *Client) SetRegistrationTimeout(d time.Duration) {
	c.registrationTimeout = d
}

// UseLIFOPartBuffers makes the client reuse a released part buffer for the
// very next part.
func (c *Client) UseLIFOPartBuffers() {
	c.partBuffers = &lifoPartBuffers{}
}
