package client_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Mic92/niks3/client"
)

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
