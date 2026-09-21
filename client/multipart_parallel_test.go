package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

func TestUploadMultipart_PartsInParallel(t *testing.T) {
	t.Parallel()

	const parts = 6

	var (
		inFlight, maxInFlight atomic.Int32
		mu                    sync.Mutex
		completed             []client.CompletedPart
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/part/"):
			n := inFlight.Add(1)
			defer inFlight.Add(-1)

			for m := maxInFlight.Load(); n > m; m = maxInFlight.Load() {
				maxInFlight.CompareAndSwap(m, n)
			}

			time.Sleep(50 * time.Millisecond)
			w.Header().Set("ETag", `"etag-`+strings.TrimPrefix(r.URL.Path, "/part/")+`"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/api/multipart/complete":
			var req struct {
				Parts []client.CompletedPart `json:"parts"`
			}

			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)

				return
			}

			mu.Lock()
			completed = req.Parts
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	urls := make([]string, parts)
	for i := range urls {
		urls[i] = fmt.Sprintf("%s/part/%d", srv.URL, i+1)
	}

	info := &client.MultipartUploadInfo{UploadID: "upload-1", PartURLs: urls}
	payload := bytes.Repeat([]byte{'x'}, (parts-1)*client.MultipartPartSize+1)

	err = c.UploadMultipart(context.Background(), bytes.NewReader(payload),
		info, "nar/abc.nar.zst", client.MultipartPartSize)
	if err != nil {
		t.Fatal(err)
	}

	if got := maxInFlight.Load(); got < 2 {
		t.Fatalf("parts were uploaded one at a time (max in flight %d)", got)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(completed) != parts {
		t.Fatalf("completed %d parts, want %d", len(completed), parts)
	}

	for i, p := range completed {
		if p.PartNumber != i+1 || p.ETag != fmt.Sprintf("etag-%d", i+1) {
			t.Fatalf("part %d completed as %+v", i+1, p)
		}
	}
}
