package client_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

// The build log body is the compressed file, sent with a known length and
// replayed from the file on retry.
func TestUploadBuildLog_FileBodyReplayedOnRetry(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "build.log")
	if err := os.WriteFile(logPath, bytes.Repeat([]byte("building...\n"), 4096), 0o600); err != nil {
		t.Fatal(err)
	}

	info, err := client.CompressBuildLog(logPath)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = info.Cleanup() }()

	want, err := os.ReadFile(info.TempFile)
	if err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)

		if r.ContentLength != int64(len(want)) {
			t.Errorf("attempt %d: Content-Length %d, want %d", n, r.ContentLength, len(want))
		}

		got, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("attempt %d: body mismatch (err=%v, %d bytes, want %d)", n, err, len(got), len(want))
		}

		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := client.NewTestClient(srv.Client(), client.RetryConfig{
		MaxRetries:     2,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     1.0,
		Jitter:         0,
	})

	if err := c.UploadBuildLogToPresignedURL(context.Background(), srv.URL, info); err != nil {
		t.Fatalf("upload failed: %v", err)
	}

	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 attempts, got %d", got)
	}
}
