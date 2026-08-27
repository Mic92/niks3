package client_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mic92/niks3/client"
)

// TestUploadMultipart_SupersededByPeer verifies that when a part upload fails
// with 404 (a peer aborted our multipart upload), the client only skips the NAR
// after confirming the object exists. A 404 plus a missing object is a real
// error, guarding against silent data loss.
func TestUploadMultipart_SupersededByPeer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		failAt       string
		objectExists bool
		wantErr      error
	}{
		{name: "exists", failAt: "part", objectExists: true, wantErr: client.ErrUploadSuperseded},
		{name: "missing", failAt: "part", objectExists: false, wantErr: nil},
		{name: "request-parts", failAt: "request-parts", objectExists: true, wantErr: client.ErrUploadSuperseded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPut && tc.failAt == "part":
					// The presigned part upload: peer already aborted it.
					http.Error(w, "no such upload", http.StatusNotFound)
				case r.Method == http.MethodPut:
					w.Header().Set("ETag", `"x"`)
				case r.Method == http.MethodPost && r.URL.Path == "/api/multipart/request-parts":
					http.Error(w, "upload not found", http.StatusNotFound)
				case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/api/objects/"):
					if tc.objectExists {
						w.WriteHeader(http.StatusNoContent)
					} else {
						w.WriteHeader(http.StatusNotFound)
					}
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
				PartURLs: []string{srv.URL + "/part/1"},
			}

			// Two parts with one URL: the second needs request-parts.
			err = c.UploadMultipart(context.Background(), bytes.NewReader([]byte("payload!")),
				info, "nar/abc.nar.zst", 4)

			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected %v, got %v", tc.wantErr, err)
				}
			default:
				if err == nil {
					t.Fatal("expected a real error when the object is absent, got nil")
				}

				if errors.Is(err, client.ErrUploadSuperseded) {
					t.Fatalf("must not skip when object is absent: %v", err)
				}
			}
		})
	}
}
