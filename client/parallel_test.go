package client_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Mic92/niks3/client"
)

// lockedBuffer collects log output written from several goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p) //nolint:wrapcheck // bytes.Buffer never fails
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// Once an upload has failed, or the push was cancelled, no further task may
// start: a NAR task serializes and compresses its whole store path before its
// first request notices the cancelled context, so a Ctrl-C during a large push
// would otherwise keep reading and compressing every remaining path (and the
// signal context swallows a second Ctrl-C meanwhile).
//
// Not parallel: it counts the "Uploading" line each NAR task logs as it
// starts, through the default logger.
//
//nolint:paralleltest // swaps the global slog default
func TestUploadPendingObjectsStopsStartingAfterFailure(t *testing.T) {
	var logs lockedBuffer

	oldLogger := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(oldLogger) })

	var puts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}

		// An expired presigned URL: not retried, fails the push.
		http.Error(w, "request has expired", http.StatusForbidden)
	}))
	defer srv.Close()

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	// One slot: each task starts only after the previous one has finished,
	// so everything after the first starts once the push has failed.
	c.MaxConcurrentNARUploads = 1

	const paths = 20

	uc := &client.UploadContext{
		PendingObjects: make(map[string]client.PendingObject),
		PathInfoByHash: make(map[string]*client.PathInfo),
		NARKeyToHash:   make(map[string]string),
	}

	storeDir := t.TempDir()

	for i := range paths {
		hash := fmt.Sprintf("%032d", i)
		storePath := filepath.Join(storeDir, fmt.Sprintf("%s-stop-probe-%d", hash, i))

		if err := os.MkdirAll(storePath, 0o755); err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(filepath.Join(storePath, "file"), bytes.Repeat([]byte{'x'}, 1024), 0o600); err != nil {
			t.Fatal(err)
		}

		narKey := fmt.Sprintf("nar/%d.nar.zst", i)
		uc.PendingObjects[narKey] = client.PendingObject{Type: "nar", PresignedURL: srv.URL + "/" + narKey}
		uc.NARKeyToHash[narKey] = hash
		uc.PathInfoByHash[hash] = &client.PathInfo{Path: storePath, NarSize: 2048}
	}

	if _, err := c.UploadPendingObjects(context.Background(), uc); err == nil {
		t.Fatal("upload against a server refusing every PUT succeeded")
	}

	if n := puts.Load(); n != 1 {
		t.Errorf("%d NAR PUTs, want 1", n)
	}

	if started := strings.Count(logs.String(), "-stop-probe-"); started != 1 {
		t.Errorf("%d NAR tasks started, want 1: none may start after the push failed", started)
	}
}
