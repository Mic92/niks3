package client_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

// When a peer's completion aborts our multipart upload of a NAR, the listing
// still has to be uploaded: the peer pushed the same NAR but possibly a
// different store path, and the listing is keyed by store path. The abort
// also cuts the NAR dump short before it produces the listing, unless the
// NAR is small enough to be dumped whole before the first part goes out. And
// a listing upload that did start and failed is still an error.
func TestSupersededNARStillUploadsListing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// Size of the store path's one file; random, so it does not compress.
		size          int
		listingStatus int
		wantErr       bool
	}{
		{name: "small NAR", size: 11, listingStatus: http.StatusOK},
		// Several parts' worth: the dump is still running when the first
		// part is refused, and the abort ends it before the listing.
		{name: "dump cut short", size: 64 << 20, listingStatus: http.StatusOK},
		{name: "listing upload fails", size: 11, listingStatus: http.StatusForbidden, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var listingUploads atomic.Int32

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/part/"):
					// The presigned part upload: a peer already aborted it.
					http.Error(w, "no such upload", http.StatusNotFound)
				case r.Method == http.MethodPut && r.URL.Path == "/listing":
					listingUploads.Add(1)
					w.WriteHeader(tc.listingStatus)
				case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/api/objects/"):
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodPost && r.URL.Path == "/api/uploads/complete":
					w.WriteHeader(http.StatusNoContent)
				default:
					http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusBadRequest)
				}
			}))
			defer srv.Close()

			c, err := client.NewTestClientForServer(srv.URL)
			if err != nil {
				t.Fatal(err)
			}

			storePath := filepath.Join(t.TempDir(), "abc-hello")
			if err := os.MkdirAll(storePath, 0o755); err != nil {
				t.Fatal(err)
			}

			content := make([]byte, tc.size)
			_, _ = rand.Read(content)

			if err := os.WriteFile(filepath.Join(storePath, "hello"), content, 0o600); err != nil {
				t.Fatal(err)
			}

			partURLs := make([]string, 16)
			for i := range partURLs {
				partURLs[i] = fmt.Sprintf("%s/part/%d", srv.URL, i+1)
			}

			narObj := client.PendingObject{
				Type:          "nar",
				MultipartInfo: &client.MultipartUploadInfo{UploadID: "upload-1", PartURLs: partURLs},
			}
			lsObj := client.PendingObject{Type: "listing", PresignedURL: srv.URL + "/listing"}

			err = c.UploadNARWithListing(context.Background(), "nar/abc.nar.zst", narObj, "abc.ls", lsObj,
				&client.PathInfo{Path: storePath, NarSize: 1 << 30})

			c.WaitRegistrations()

			switch {
			case tc.wantErr && err == nil:
				t.Fatal("superseded upload whose listing failed reported success")
			case !tc.wantErr && err != nil:
				t.Fatalf("superseded upload must succeed, got %v", err)
			}

			if n := listingUploads.Load(); n != 1 {
				t.Fatalf("listing uploaded %d times, want 1", n)
			}
		})
	}
}

// A store file that turns out shorter than its size at walk time makes the
// NAR dump fail with an error wrapping io.ErrUnexpectedEOF, and that error
// reaches the multipart reader through the pipe. It must fail the upload: a
// completed multipart upload is registered as the NAR on the spot, so taking
// the error for the end of the stream would publish a truncated NAR under the
// content hash of the full one, where no later push replaces it.
func TestTruncatedNARDumpIsNotCompleted(t *testing.T) {
	t.Parallel()

	// More than partsInFlight parts, so the dump is still reading the file
	// when the parts stall.
	const size = 60 << 20

	storePath := filepath.Join(t.TempDir(), "abc-blob")
	if err := os.MkdirAll(storePath, 0o755); err != nil {
		t.Fatal(err)
	}

	blob := filepath.Join(storePath, "blob")

	// Incompressible, so compressed parts keep pace with the file offset.
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(blob, data, 0o600); err != nil {
		t.Fatal(err)
	}

	var (
		completes atomic.Int32
		arrived   = make(chan struct{}, 16)
		release   = make(chan struct{})
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/part/"):
			arrived <- struct{}{}

			select {
			case <-release:
			case <-r.Context().Done():
				return
			}

			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/api/multipart/complete":
			completes.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/api/objects/"):
			w.WriteHeader(http.StatusNotFound)
		default:
			http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	partURLs := make([]string, 10)
	for i := range partURLs {
		partURLs[i] = fmt.Sprintf("%s/part/%d", srv.URL, i+1)
	}

	narObj := client.PendingObject{
		Type:          "nar",
		MultipartInfo: &client.MultipartUploadInfo{UploadID: "upload-1", PartURLs: partURLs},
	}
	lsObj := client.PendingObject{Type: "listing", PresignedURL: srv.URL + "/listing"}

	done := make(chan error, 1)

	go func() {
		done <- c.UploadNARWithListing(context.Background(), "nar/abc.nar.zst", narObj, "abc.ls", lsObj,
			&client.PathInfo{Path: storePath, NarSize: size})
	}()

	// Every part slot is taken, so the dump is blocked on the pipe well
	// before the end of the file. Cut its last byte, then let the parts go.
	for range 4 {
		<-arrived
	}

	if err := os.Truncate(blob, size-1); err != nil {
		t.Fatal(err)
	}

	close(release)

	select {
	case err := <-done:
		if err == nil {
			t.Error("upload of a truncated dump succeeded")
		}
	case <-time.After(time.Minute):
		t.Fatal("upload did not return")
	}

	if n := completes.Load(); n != 0 {
		t.Errorf("multipart upload completed %d times with a truncated NAR", n)
	}
}
