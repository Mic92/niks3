package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Mic92/niks3/client"
)

// When a peer's completion aborts our multipart upload of a NAR, the listing
// still has to be uploaded: the peer pushed the same NAR but possibly a
// different store path, and the listing is keyed by store path. The abort
// also cuts the NAR dump short before it produces the listing.
func TestSupersededNARStillUploadsListing(t *testing.T) {
	t.Parallel()

	var listingUploads atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/part/"):
			// The presigned part upload: a peer already aborted it.
			http.Error(w, "no such upload", http.StatusNotFound)
		case r.Method == http.MethodPut && r.URL.Path == "/listing":
			listingUploads.Add(1)
			w.WriteHeader(http.StatusOK)
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

	if err := os.WriteFile(filepath.Join(storePath, "hello"), []byte("hello world"), 0o600); err != nil {
		t.Fatal(err)
	}

	narObj := client.PendingObject{
		Type:          "nar",
		MultipartInfo: &client.MultipartUploadInfo{UploadID: "upload-1", PartURLs: []string{srv.URL + "/part/1"}},
	}
	lsObj := client.PendingObject{Type: "listing", PresignedURL: srv.URL + "/listing"}

	err = c.UploadNARWithListing(context.Background(), "nar/abc.nar.zst", narObj, "abc.ls", lsObj,
		&client.PathInfo{Path: storePath, NarSize: 1 << 30})
	if err != nil {
		t.Fatalf("superseded upload must succeed, got %v", err)
	}

	c.WaitRegistrations()

	if n := listingUploads.Load(); n != 1 {
		t.Fatalf("listing uploaded %d times, want 1", n)
	}
}
