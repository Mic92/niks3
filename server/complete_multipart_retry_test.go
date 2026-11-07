package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/minio/minio-go/v7"
)

// TestRetryableCompletionError pins which completion failures are worth
// another attempt. Getting this wrong is expensive in both directions: a
// retried 4xx burns the whole backoff budget on a verdict that will not
// change, and a non-retried 5xx strands a fully-uploaded NAR.
func TestRetryableCompletionError(t *testing.T) {
	t.Parallel()

	s3Err := func(code string, status int) error {
		return minio.ErrorResponse{Code: code, StatusCode: status}
	}

	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nil":               {nil, false},
		"degraded quorum":   {s3Err("InternalError", http.StatusInternalServerError), true},
		"bad gateway":       {s3Err("", http.StatusBadGateway), true},
		"transport failure": {errors.New("connection reset by peer"), true},
		"no such upload":    {s3Err(minio.NoSuchUpload, http.StatusNotFound), false},
		"invalid part":      {s3Err("InvalidPart", http.StatusBadRequest), false},
		"access denied":     {s3Err("AccessDenied", http.StatusForbidden), false},
		"throttled":         {s3Err("SlowDown", http.StatusServiceUnavailable), false},
		"context canceled":  {context.Canceled, false},
		"deadline exceeded": {context.DeadlineExceeded, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := server.RetryableCompletionError(tc.err); got != tc.want {
				t.Errorf("RetryableCompletionError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// flakyCompleteProxy fails the first `failures` CompleteMultipartUpload
// requests the way a store that reads at less than full quorum does — the
// parts are durable, but the replica answering the completion cannot see them
// yet — and forwards everything else untouched.
type flakyCompleteProxy struct {
	target   *url.URL
	failures int64

	completeAttempts atomic.Int64
}

func newFlakyCompleteProxy(target string, failures int64) (*flakyCompleteProxy, error) {
	targetURL, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("failed to parse target URL: %w", err)
	}

	return &flakyCompleteProxy{target: targetURL, failures: failures}, nil
}

func (p *flakyCompleteProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CompleteMultipartUpload is a POST carrying uploadId; a part upload is a
	// PUT, so the method test is enough to leave the part writes alone.
	if r.Method == http.MethodPost && r.URL.Query().Has("uploadId") {
		if p.completeAttempts.Add(1) <= p.failures {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>InternalError</Code>
    <Message>No data was uploaded. Refusing to create empty object.</Message>
    <RequestId>test-request-id</RequestId>
</Error>`))

			return
		}
	}

	httputil.NewSingleHostReverseProxy(p.target).ServeHTTP(w, r)
}

// TestCompleteMultipartUploadRetriesTransientFailure pushes a NAR whose
// completion fails before succeeding. Every part is already durable, so
// failing the push would strand a fully-uploaded NAR — and because a later
// push skips objects niks3 believes are cached, nothing would heal it.
func TestCompleteMultipartUploadRetriesTransientFailure(t *testing.T) {
	t.Parallel()

	if testRustfsServer == nil {
		t.Skip("rustfs server not started")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	// minio-go retries 5xx itself, so one proxy-level failure is already more
	// than one round trip. The handler's own loop is what carries the push
	// past a store that stays behind for longer than the SDK will wait.
	proxy, err := newFlakyCompleteProxy(fmt.Sprintf("http://localhost:%d", testRustfsServer.port), 1)
	ok(t, err)

	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	service := createTestServiceWithThrottlingProxy(t, proxyServer.URL)
	defer service.Close()

	narKey := narKeyFor("cccccccccccccccccccccccccccccc01")
	narinfoKey := "cccccccccccccccccccccccccccccc10.narinfo"

	pendingClosureResponse := createPendingClosure(t, service, map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 20 * 1024 * 1024},
		},
	})

	narPending, found := pendingClosureResponse.PendingObjects[narKey]
	if !found || narPending.MultipartInfo == nil {
		t.Fatalf("expected multipart upload for %s", narKey)
	}

	httpClient := &http.Client{}
	dummyData := make([]byte, 5*1024*1024)
	completedParts := make([]map[string]any, 0, len(narPending.MultipartInfo.PartURLs))

	for i, partURL := range narPending.MultipartInfo.PartURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, partURL, bytes.NewReader(dummyData))
		ok(t, err)

		resp, err := httpClient.Do(req)
		ok(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("failed to upload part %d: status %d", i+1, resp.StatusCode)
		}

		completedParts = append(completedParts, map[string]any{
			"part_number": i + 1,
			"etag":        strings.Trim(resp.Header.Get("ETag"), "\""),
		})

		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}

	completeBody, err := json.Marshal(map[string]any{
		"object_key": narKey,
		"upload_id":  narPending.MultipartInfo.UploadID,
		"parts":      completedParts,
	})
	ok(t, err)

	success := checkStatusCode(http.StatusNoContent)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/multipart/complete",
		body:          completeBody,
		handler:       service.CompleteMultipartUploadHandler,
		checkResponse: &success,
	})

	if attempts := proxy.completeAttempts.Load(); attempts < 2 {
		t.Errorf("completion was not retried: %d attempt(s)", attempts)
	}

	// The retry must produce a real object, not merely a success status.
	info, err := service.MinioClient.StatObject(ctx, service.Bucket, narKey, minio.StatObjectOptions{})
	ok(t, err)

	if info.Size == 0 {
		t.Errorf("completed object %s is empty", narKey)
	}
}
