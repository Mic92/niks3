package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
)

// throttlingProxy creates a reverse proxy that returns S3 SlowDown errors
// for specific operations.
type throttlingProxy struct {
	target *url.URL

	mu                   sync.Mutex
	throttleOperations   map[string]bool // operations to throttle (e.g., "CompleteMultipartUpload")
	requestCount         atomic.Int64
	throttledCount       atomic.Int64
	completeMultipartCnt atomic.Int64
}

func newThrottlingProxy(target string) (*throttlingProxy, error) {
	targetURL, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("failed to parse target URL: %w", err)
	}

	return &throttlingProxy{
		target:             targetURL,
		throttleOperations: make(map[string]bool),
	}, nil
}

// ThrottleOperation enables throttling for a specific S3 operation.
func (p *throttlingProxy) ThrottleOperation(op string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.throttleOperations[op] = true
}

func (p *throttlingProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.requestCount.Add(1)

	// Check if this is a CompleteMultipartUpload request and should be throttled
	// S3 CompleteMultipartUpload is a POST with uploadId query param
	if r.Method == http.MethodPost && r.URL.Query().Has("uploadId") {
		p.completeMultipartCnt.Add(1)

		p.mu.Lock()
		shouldThrottle := p.throttleOperations["CompleteMultipartUpload"]
		p.mu.Unlock()

		if shouldThrottle {
			p.throttledCount.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusServiceUnavailable)
			// S3 SlowDown error response
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>SlowDown</Code>
    <Message>Please reduce your request rate.</Message>
    <RequestId>test-request-id</RequestId>
</Error>`))

			return
		}
	}

	// Forward to actual S3
	proxy := httputil.NewSingleHostReverseProxy(p.target)
	proxy.ServeHTTP(w, r)
}

// TestCompleteMultipartUploadHandler_RateLimitTriggersThrottle verifies that when
// CompleteMultipartUploadHandler receives an S3 SlowDown error, the server's rate
// limiter is properly triggered via handleS3Error().
//
// This is a regression test for the bug where handleS3Error() wasn't calling
// RecordThrottle(), so the rate limiter wouldn't adapt when errors came through
// HTTP handlers.
func TestCompleteMultipartUploadHandler_RateLimitTriggersThrottle(t *testing.T) {
	t.Parallel()

	if testRustfsServer == nil {
		t.Skip("rustfs server not started")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Create a throttling proxy in front of RustFS
	rustfsURL := fmt.Sprintf("http://localhost:%d", testRustfsServer.port)

	proxy, err := newThrottlingProxy(rustfsURL)
	ok(t, err)

	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	// Create a service that uses the throttling proxy
	service := createTestServiceWithThrottlingProxy(t, proxyServer.URL)
	defer service.Close()

	// Verify rate limiter starts disabled
	if service.S3RateLimiter.IsEnabled() {
		t.Fatal("rate limiter should start disabled")
	}

	// Step 1: Create a pending closure to get multipart upload info
	closureHash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb01"
	narinfoKey := closureHash + ".narinfo"
	narKey := "nar/" + closureHash + ".nar.zst"

	body, err := json.Marshal(map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 10 * 1024 * 1024}, // 10MB to trigger multipart
		},
	})
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/pending_closures",
		body:    body,
		handler: service.CreatePendingClosureHandler,
	})

	var pendingClosureResponse server.PendingClosureResponse

	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse)
	ok(t, err)

	// Find the multipart upload
	var narPending server.PendingObject

	var foundMultipart bool

	for key, obj := range pendingClosureResponse.PendingObjects {
		if strings.HasPrefix(key, "nar/") && obj.MultipartInfo != nil {
			narPending = obj
			foundMultipart = true

			break
		}
	}

	if !foundMultipart {
		t.Fatal("expected multipart upload for NAR file")
	}

	// Step 2: Upload parts to S3 (required before we can complete)
	httpClient := &http.Client{}
	minPartSize := 5 * 1024 * 1024 // 5MB minimum part size
	dummyData := make([]byte, minPartSize)

	completedParts := make([]map[string]any, 0, len(narPending.MultipartInfo.PartURLs))

	for i, partURL := range narPending.MultipartInfo.PartURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, partURL, bytes.NewReader(dummyData))
		ok(t, err)

		resp, err := httpClient.Do(req)
		ok(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("failed to upload part %d: status %d", i+1, resp.StatusCode)
		}

		etag := strings.Trim(resp.Header.Get("ETag"), "\"")
		completedParts = append(completedParts, map[string]any{
			"part_number": i + 1,
			"etag":        etag,
		})

		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}

	// Step 3: Now enable throttling for CompleteMultipartUpload
	proxy.ThrottleOperation("CompleteMultipartUpload")

	// Verify rate limiter is still disabled (no throttle events yet)
	if service.S3RateLimiter.IsEnabled() {
		t.Fatal("rate limiter should still be disabled before CompleteMultipartUpload")
	}

	// Step 4: Call CompleteMultipartUploadHandler - this should trigger handleS3Error
	completeReq := map[string]any{
		"object_key": narKey,
		"upload_id":  narPending.MultipartInfo.UploadID,
		"parts":      completedParts,
	}

	completeBody, err := json.Marshal(completeReq)
	ok(t, err)

	check429 := checkStatusCode(http.StatusTooManyRequests)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/multipart/complete",
		body:          completeBody,
		handler:       service.CompleteMultipartUploadHandler,
		checkResponse: &check429,
	})

	// Step 5: Verify the rate limiter was enabled via handleS3Error -> RecordThrottle
	// THIS IS THE KEY ASSERTION - it tests that handleS3Error calls RecordThrottle
	if !service.S3RateLimiter.IsEnabled() {
		t.Error("rate limiter should be enabled after handleS3Error received SlowDown error")
	}

	t.Logf("Proxy stats: total=%d, throttled=%d, completeMultipart=%d",
		proxy.requestCount.Load(), proxy.throttledCount.Load(), proxy.completeMultipartCnt.Load())
	t.Logf("Rate limiter: enabled=%v, rate=%.2f",
		service.S3RateLimiter.IsEnabled(), service.S3RateLimiter.CurrentRate())
}

// createTestServiceWithThrottlingProxy creates a test service that connects
// to S3 via a throttling proxy instead of directly to RustFS.
func createTestServiceWithThrottlingProxy(tb testing.TB, proxyURL string) *server.Service {
	tb.Helper()

	// Get the base service for DB setup
	baseService := createTestService(tb)

	// Parse proxy URL to create a new minio client pointing to it
	proxyURLParsed, err := url.Parse(proxyURL)
	ok(tb, err)

	// Create minio client pointing to the proxy
	minioClient := testRustfsServer.ClientWithEndpoint(tb, proxyURLParsed.Host)

	// Replace the minio client with one pointing to the proxy
	baseService.MinioClient = minioClient

	return baseService
}
