package client_test

import (
	"bytes"
	"context"
	"errors"
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

// fileBodyRefusingTransport fails an *os.File body like darwin's Nix sandbox
// does for sendfile(2).
type fileBodyRefusingTransport struct{ next http.RoundTripper }

func (t fileBodyRefusingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if _, ok := r.Body.(*os.File); ok {
		_ = r.Body.Close()

		return nil, errors.New("request body is an *os.File, which net/http sends with sendfile")
	}

	return t.next.RoundTrip(r) //nolint:wrapcheck // transparent
}

// The body is the file, replayed on retry and not handed to sendfile.
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

	httpClient := srv.Client()
	httpClient.Transport = fileBodyRefusingTransport{next: httpClient.Transport}

	c := client.NewTestClient(httpClient, client.RetryConfig{
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
