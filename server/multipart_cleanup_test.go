package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

	// While S3 is unreachable the abort fails. The closure and its tracking
	// row must survive the cleanup: the row is the only handle on the upload,
	// and without it the parts would sit in S3 for good.
	good := service.MinioClient
	service.MinioClient = brokenS3Client(t)

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	service.MinioClient = good

	var rows int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM multipart_uploads WHERE upload_id = $1", uploadID).Scan(&rows))

	if rows != 1 {
		t.Fatalf("multipart upload row dropped although the abort failed (rows=%d)", rows)
	}

	_, err = coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10)
	ok(t, err) // still live: nothing aborted it

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

	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM pending_closures").Scan(&rows))

	if rows != 0 {
		t.Errorf("%d pending closure(s) left after a successful cleanup", rows)
	}
}

// A multipart upload is opened in S3 before it is recorded. When the request
// is cancelled in between (a sibling in the errgroup failing does the same),
// the insert fails and the upload must be aborted on a context that survives
// the cancellation: without a row nothing can ever find it again.
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

		// S3 has opened the upload. Hand minio the complete answer, then
		// cancel the request before the server records the upload.
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

// Cleanup lists the uploads to abort and deletes the closures against one
// cutoff. With a cutoff per query, a closure that ages past it while the
// aborts run is cascaded out of the database with its upload still open.
func TestPendingCleanupUsesOneCutoff(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	push := func(hash string) (string, string) {
		t.Helper()

		narKey := narKeyFor(hash)
		resp := createPendingClosure(t, service, map[string]any{
			"closure": hash + ".narinfo",
			"objects": []map[string]any{
				{"key": hash + ".narinfo", "type": "narinfo", "refs": []string{narKey}},
				{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 100 * 1024 * 1024},
			},
		})

		return narKey, resp.PendingObjects[narKey].MultipartInfo.UploadID
	}

	oldHash, youngHash := strings.Repeat("k", 32), strings.Repeat("m", 32)
	oldNar, oldUpload := push(oldHash)
	youngNar, youngUpload := push(youngHash)

	// The old closure is well past the one-hour cutoff; the young one
	// crosses it four seconds from now, while the old upload's abort is
	// still running.
	const abortDelay = 6 * time.Second

	_, err := service.Pool.Exec(ctx, `UPDATE pending_closures SET started_at = CASE key
		WHEN $1 THEN timezone('UTC', now()) - interval '2 hours'
		ELSE timezone('UTC', now()) - interval '1 hour' + interval '4 seconds' END`, oldHash+".narinfo")
	ok(t, err)

	good := service.MinioClient
	service.MinioClient = interceptedS3Client(t, func(r *http.Request, next http.RoundTripper) (*http.Response, error) {
		if r.Method == http.MethodDelete && r.URL.Query().Has("uploadId") {
			select {
			case <-time.After(abortDelay):
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}

		return next.RoundTrip(r)
	})

	cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	w := httptest.NewRecorder()
	service.CleanupPendingClosuresHandler(w,
		httptest.NewRequestWithContext(cleanupCtx, http.MethodDelete, "/api/pending_closures?older-than=1h", nil))
	httpOkDepth(t, w)

	service.MinioClient = good

	uploadRows := func(uploadID string) int {
		var n int
		ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM multipart_uploads WHERE upload_id = $1", uploadID).Scan(&n))

		return n
	}

	coreClient := minio.Core{Client: service.MinioClient}

	if _, err := coreClient.ListObjectParts(ctx, service.Bucket, oldNar, oldUpload, 0, 10); err == nil {
		t.Error("the old closure's upload was not aborted")
	}

	if n := uploadRows(oldUpload); n != 0 {
		t.Errorf("the old closure's upload row survived its abort (rows=%d)", n)
	}

	// The young closure was not selected for abort, so it must not have
	// been deleted either.
	if n := uploadRows(youngUpload); n != 1 {
		t.Fatalf("the young closure's upload row was dropped although its upload was never aborted (rows=%d)", n)
	}

	// Once it has aged out for real, cleanup reaps it like any other.
	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	if _, err := coreClient.ListObjectParts(ctx, service.Bucket, youngNar, youngUpload, 0, 10); err == nil {
		t.Error("the young closure's upload was not aborted by the later cleanup")
	}
}

// brokenS3Client returns a client whose every request fails: it points at a
// closed port.
func brokenS3Client(t *testing.T) *minio.Client {
	t.Helper()

	bad, err := minio.New("127.0.0.1:1", &minio.Options{Creds: credentials.NewStaticV4("a", "b", ""), MaxRetries: 1})
	ok(t, err)

	return bad
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// interceptedS3Client returns a client for the test S3 whose requests go
// through intercept, which may observe, delay or fail them before or after
// handing them to next. Retries are off so a failure injected once is final.
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

// createUploadProxy forwards requests to S3, except multipart upload
// creation for two keys: failKey's is refused once holdKey's has been
// created in S3, and the response to holdKey's creation is held until the
// caller gives up on it or half a second has passed.
type createUploadProxy struct {
	target           *url.URL
	holdKey, failKey string
	created          chan struct{}
	once             sync.Once
}

func (p *createUploadProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(p.target)

	if r.Method != http.MethodPost || !r.URL.Query().Has("uploads") {
		proxy.ServeHTTP(w, r)

		return
	}

	switch {
	case strings.HasSuffix(r.URL.Path, "/"+p.holdKey):
		rec := httptest.NewRecorder()
		proxy.ServeHTTP(rec, r)
		p.once.Do(func() { close(p.created) })

		select {
		case <-r.Context().Done():
			return
		case <-time.After(500 * time.Millisecond):
		}

		maps.Copy(w.Header(), rec.Header())
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	case strings.HasSuffix(r.URL.Path, "/"+p.failKey):
		select {
		case <-p.created:
		case <-time.After(5 * time.Second):
		}

		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` +
			`<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
	default:
		proxy.ServeHTTP(w, r)
	}
}

// A pending-closure request that fails while creating its multipart uploads
// must leave no upload in S3 without a row to find it by. Here one NAR's
// upload cannot be created while S3 is creating the other's, and the failure
// cancels the sibling request after S3 has carried it out.
func TestPendingClosureFailureTracksEveryUpload(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	hash := strings.Repeat("r", 32)
	holdKey := "nar/" + strings.Repeat("s", 52) + ".nar.zst"
	failKey := "nar/" + strings.Repeat("v", 52) + ".nar.zst"

	target, err := url.Parse(fmt.Sprintf("http://localhost:%d", testRustfsServer.port))
	ok(t, err)

	proxyServer := httptest.NewServer(&createUploadProxy{
		target: target, holdKey: holdKey, failKey: failKey, created: make(chan struct{}),
	})
	defer proxyServer.Close()

	proxyURL, err := url.Parse(proxyServer.URL)
	ok(t, err)

	direct := service.MinioClient
	service.MinioClient = testRustfsServer.ClientWithEndpoint(t, proxyURL.Host)

	w := postPendingClosureJSON(t, service, `{"closure":"`+hash+`.narinfo","objects":[`+
		`{"key":"`+hash+`.narinfo","type":"narinfo","refs":["`+holdKey+`","`+failKey+`"]},`+
		`{"key":"`+holdKey+`","type":"nar","refs":[],"nar_size":104857600},`+
		`{"key":"`+failKey+`","type":"nar","refs":[],"nar_size":104857600}]}`)

	service.MinioClient = direct

	if w.Code == http.StatusOK {
		t.Fatalf("pending closure created although an upload could not be: %s", w.Body.String())
	}

	for upload := range direct.ListIncompleteUploads(ctx, service.Bucket, "nar/", true) {
		ok(t, upload.Err)

		var rows int
		ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM multipart_uploads WHERE upload_id = $1", upload.UploadID).Scan(&rows))

		if rows == 0 {
			t.Errorf("multipart upload %s of %s is open in S3 with no row to find it by", upload.UploadID, upload.Key)
		}
	}
}
