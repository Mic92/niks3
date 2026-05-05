package client_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Mic92/niks3/client"
)

// stubServer accepts the HTTP traffic that uploadNARWithListing produces:
// PUTs (NAR parts and the .ls file) get a fake ETag, multipart-complete
// responds cleanly. Returns the server and a helper for building
// MultipartUploadInfo with N pre-signed PUT URLs that point at the stub.
func stubServer(t *testing.T) (*httptest.Server, func(numParts int) *client.MultipartUploadInfo) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		switch {
		case r.Method == http.MethodPut:
			w.Header().Set("ETag", "\"deadbeef\"")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/api/multipart/complete":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request: "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
		}
	}))

	makeMultipart := func(numParts int) *client.MultipartUploadInfo {
		urls := make([]string, numParts)
		for i := range numParts {
			urls[i] = fmt.Sprintf("%s/part/%d", srv.URL, i+1)
		}

		return &client.MultipartUploadInfo{UploadID: "test-upload", PartURLs: urls}
	}

	return srv, makeMultipart
}

// makeStoreDir creates a temp directory containing numFiles regular files so
// the resulting NarListing has real entries.
func makeStoreDir(t *testing.T, numFiles int) string {
	t.Helper()

	dir := t.TempDir()

	for i := range numFiles {
		fp := filepath.Join(dir, fmt.Sprintf("file-%05d.dat", i))
		if err := os.WriteFile(fp, fmt.Appendf(nil, "content-%030d", i), 0o600); err != nil {
			t.Fatalf("writing file: %v", err)
		}
	}

	return dir
}

func newStubClient(t *testing.T, srvURL string) *client.Client {
	t.Helper()

	base, err := url.Parse(srvURL)
	if err != nil {
		t.Fatalf("parsing url: %v", err)
	}

	return client.NewStubClient(base)
}

// TestListingNotRetainedAfterLsUpload verifies that compressedInfo[hash].Listing
// is released once the .ls file has been uploaded. Phase 2 of the upload
// pipeline only uses compressedInfo for presence checks, so retaining the
// recursive directory tree past the .ls upload is pure waste.
func TestListingNotRetainedAfterLsUpload(t *testing.T) {
	t.Parallel()

	storeDir := makeStoreDir(t, 200)

	srv, makeMultipart := stubServer(t)
	defer srv.Close()

	c := newStubClient(t, srv.URL)
	hash := "0000000000000000000000000000000000"
	compressedInfo := map[string]*client.CompressedFileInfo{}

	var mu sync.Mutex

	err := c.UploadNARAndListingForTest(
		t.Context(),
		storeDir, hash,
		"nar/test.nar.zst", hash+".ls", srv.URL+"/ls",
		1024,
		makeMultipart(8),
		compressedInfo, &mu,
	)
	if err != nil {
		t.Fatalf("UploadNARAndListingForTest: %v", err)
	}

	info := compressedInfo[hash]
	if info == nil {
		t.Fatalf("compressedInfo[%s] missing after upload", hash)
	}

	if info.Listing != nil {
		t.Fatalf("Listing was not released after .ls upload (root has %d entries)", len(info.Listing.Root.Entries))
	}
}
