package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func TestMultipartCleanup(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Create a pending closure with multipart upload
	closureHash := "dadb44fdadb44fdadb44fdadb44f0000"
	closureKey := closureHash + ".narinfo"
	narKey := narKeyFor(closureHash)

	// Use a large NarSize to ensure multipart upload is used
	largeNarSize := uint64(100 * 1024 * 1024) // 100MB

	objects := []map[string]any{
		{"key": closureKey, "type": "narinfo", "refs": []string{narKey}},
		{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": largeNarSize},
	}

	// Create pending closure (this initiates multipart upload)
	pendingClosureResponse := createPendingClosure(t, service, map[string]any{
		"closure": closureKey,
		"objects": objects,
	})

	// Verify that we got a multipart upload
	narPendingObject, exists := pendingClosureResponse.PendingObjects[narKey]
	if !exists {
		t.Fatalf("NAR object not found in pending objects")
	}

	if narPendingObject.MultipartInfo == nil {
		t.Fatalf("Expected multipart upload for NAR file, got presigned URL instead")
	}

	uploadID := narPendingObject.MultipartInfo.UploadID
	if uploadID == "" {
		t.Fatalf("Upload ID is empty")
	}

	// Verify the upload exists in S3
	coreClient := minio.Core{Client: service.MinioClient}
	_, err := coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10)
	ok(t, err) // Should not error if upload exists

	// Don't complete the upload - simulate an abandoned upload

	// Wait a bit to ensure the cleanup will catch it
	time.Sleep(100 * time.Millisecond)

	// Call cleanup with 0 duration (cleans everything)
	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	// Verify the upload was aborted in S3
	_, err = coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10)
	if err == nil {
		t.Error("Expected error when listing parts of aborted upload, but got none")
	}
}


// If the request is cancelled after S3 opened the upload but before the row is
// stored, the upload must still be aborted. Nothing else can find it.
func TestMultipartUploadAbortedWhenCancelledBeforeRecorded(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	reqCtx, cancelReq := context.WithCancel(t.Context())
	defer cancelReq()

	var aborts atomic.Int32

	service.MinioClient = interceptedS3Client(t, func(r *http.Request, next http.RoundTripper) (*http.Response, error) {
		if r.Method == http.MethodDelete && r.URL.Query().Has("uploadId") {
			aborts.Add(1)
		}

		resp, err := next.RoundTrip(r)
		if err != nil || r.Method != http.MethodPost || !r.URL.Query().Has("uploads") {
			return resp, err //nolint:wrapcheck // transparent
		}

		// Cancel after S3 opened the upload and before the server records it.
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if err != nil {
			return nil, err //nolint:wrapcheck // transparent
		}

		resp.Body = io.NopCloser(bytes.NewReader(body))

		cancelReq()

		return resp, nil
	})

	hash := strings.Repeat("d", 32)
	narKey := narKeyFor(hash)

	w := httptest.NewRecorder()
	service.CreatePendingClosureHandler(w, httptest.NewRequestWithContext(reqCtx, http.MethodPost, "/api/pending_closures",
		strings.NewReader(`{"closure":"`+hash+`.narinfo","objects":[`+
			`{"key":"`+hash+`.narinfo","type":"narinfo","refs":["`+narKey+`"]},`+
			`{"key":"`+narKey+`","type":"nar","refs":[],"nar_size":104857600}]}`)))

	if w.Code == http.StatusOK {
		t.Fatalf("request succeeded although it was cancelled: %s", w.Body.String())
	}

	if n := aborts.Load(); n != 1 {
		t.Errorf("%d abort request(s) reached S3, want 1", n)
	}

	for upload := range testRustfsServer.Client(t).ListIncompleteUploads(t.Context(), service.Bucket, narKey, true) {
		ok(t, upload.Err)
		t.Errorf("multipart upload %s left open in S3 with no row to find it by", upload.UploadID)
	}
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// interceptedS3Client returns a client whose requests go through intercept,
// which may observe, delay or fail them. Retries are off.
func interceptedS3Client(
	t *testing.T,
	intercept func(r *http.Request, next http.RoundTripper) (*http.Response, error),
) *minio.Client {
	t.Helper()

	next, isTransport := http.DefaultTransport.(*http.Transport)
	if !isTransport {
		t.Fatal("http.DefaultTransport is not an *http.Transport")
	}

	next = next.Clone()

	c, err := minio.New(fmt.Sprintf("localhost:%d", testRustfsServer.port), &minio.Options{
		Creds:      testRustfsServer.Creds(),
		Transport:  roundTripFunc(func(r *http.Request) (*http.Response, error) { return intercept(r, next) }),
		MaxRetries: 1,
	})
	ok(t, err)

	return c
}

