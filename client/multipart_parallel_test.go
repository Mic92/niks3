package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

// Only the stream's own EOF ends an upload. The NAR dump reports a store file
// that reads short with an error wrapping io.ErrUnexpectedEOF, or io.EOF when
// nothing was left to read, and the pipe hands it to the reader as is;
// completing on it would register a truncated NAR.
func TestUploadMultipart_ProducerErrorIsNotEOF(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		written int
		cause   error
	}{
		{name: "short read inside a part", written: client.MultipartPartSize * 3 / 2, cause: io.ErrUnexpectedEOF},
		{name: "empty read on a part boundary", written: client.MultipartPartSize, cause: io.EOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var completes atomic.Int32

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/part/"):
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("ETag", `"etag"`)
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPost && r.URL.Path == "/api/multipart/complete":
					completes.Add(1)
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

			info := &client.MultipartUploadInfo{
				UploadID: "upload-1",
				PartURLs: []string{srv.URL + "/part/1", srv.URL + "/part/2", srv.URL + "/part/3"},
			}

			// The shape of compressAndMultipartUploadNAR's pipe on a dump error.
			pr, pw := io.Pipe()

			go func() {
				_, _ = pw.Write(bytes.Repeat([]byte{'x'}, tc.written))
				pw.CloseWithError(fmt.Errorf("serializing NAR: reading file blob: expected 42 more bytes: %w", tc.cause))
			}()

			err = c.UploadMultipart(context.Background(), pr, info, "nar/abc.nar.zst", client.MultipartPartSize)
			if !errors.Is(err, tc.cause) {
				t.Errorf("got %v, want the producer's error", err)
			}

			if n := completes.Load(); n != 0 {
				t.Errorf("multipart upload completed %d times on a producer error", n)
			}
		})
	}
}

// S3 may answer a part before reading its body (SlowDown, an expired
// presigned URL) and go on reading it. The transport's write loop then keeps
// copying from the part's buffer after the upload has returned: net/http
// waits at most 50ms for the body write before it lets the caller go, and
// nothing orders the write loop's later reads before the caller's next use
// of the buffer. A failed part's buffer must therefore not go back to the
// pool, or the reader fills it for the next part while the write loop is
// still copying from it.
//
// The pool here always hands out the buffer released last, so a released
// buffer is taken by the very next part, and the race detector reports the
// overlap. Without -race the test cannot tell the difference.
func TestUploadMultipart_FailedPartBufferNotReused(t *testing.T) {
	t.Parallel()

	const parts = 6

	// Every other part is held, so the slots stay taken and the reader is
	// waiting for one when part 1 fails.
	var (
		hold    = make(chan struct{})
		arrived = make(chan struct{}, parts)
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/part/1" {
			arrived <- struct{}{}

			select {
			case <-hold:
			case <-r.Context().Done():
			}

			http.Error(w, "held", http.StatusBadRequest)

			return
		}

		rc := http.NewResponseController(w)
		if err := rc.EnableFullDuplex(); err != nil {
			t.Errorf("full duplex: %v", err)
		}

		// Parts 2 to 4 arriving means the reader has filled every buffer
		// its slots allow.
		for range 3 {
			select {
			case <-arrived:
			case <-time.After(5 * time.Second):
				t.Error("parts 2 to 4 never arrived")
			}
		}

		// Answer before reading anything, then take the body slowly for
		// longer than the client waits for it.
		const msg = "SlowDown"

		w.Header().Set("Content-Length", strconv.Itoa(len(msg)))
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, msg)
		_ = rc.Flush()

		buf := make([]byte, 64<<10)
		for end := time.Now().Add(300 * time.Millisecond); time.Now().Before(end); {
			if _, err := r.Body.Read(buf); err != nil {
				return
			}

			time.Sleep(time.Millisecond)
		}
	}))
	defer srv.Close()
	defer close(hold)

	c := client.NewTestClient(srv.Client(), client.RetryConfig{})
	c.UseLIFOPartBuffers()

	urls := make([]string, parts)
	for i := range urls {
		urls[i] = fmt.Sprintf("%s/part/%d", srv.URL, i+1)
	}

	payload := bytes.Repeat([]byte{'x'}, (parts-1)*client.MultipartPartSize+1)

	err := c.UploadMultipart(context.Background(), bytes.NewReader(payload),
		&client.MultipartUploadInfo{UploadID: "upload-1", PartURLs: urls}, "nar/abc.nar.zst", client.MultipartPartSize)

	var statusErr *client.HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("err = %v, want the 503 of part 1", err)
	}
}
