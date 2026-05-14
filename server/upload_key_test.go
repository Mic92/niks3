package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
)

func TestIsValidUploadKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		objType string
		valid   bool
	}{
		// Valid uploads
		{"narinfo", "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", "narinfo", true},
		{"nar zst", "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.zst", "nar", true},
		{"nar xz", "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.xz", "nar", true},
		{"nar plain", "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar", "nar", true},
		{"listing", "26xbg1ndr7hbcncrlf9nhx5is2b25d13.ls", "listing", true},
		{"build log", "log/abcd1234-hello-1.0.drv", "build_log", true},
		{"build log home-manager file", "log/abcd1234-hm_..zlogout.drv", "build_log", true},
		{"realisation", "realisations/sha256:abc123!out.doi", "realisation", true},

		// Server-owned files: never client-writable
		{"nix-cache-info", "nix-cache-info", "narinfo", false},
		{"index.html", "index.html", "narinfo", false},

		// Type/key mismatch
		{"narinfo key, nar type", "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", "nar", false},
		{"nar key, narinfo type", "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.zst", "narinfo", false},
		{"listing key, narinfo type", "26xbg1ndr7hbcncrlf9nhx5is2b25d13.ls", "narinfo", false},

		// Path traversal / arbitrary keys
		{"traversal", "../etc/passwd", "narinfo", false},
		{"traversal nar", "nar/../../secrets", "nar", false},
		{"absolute", "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", "narinfo", false},
		{"empty key", "", "narinfo", false},
		{"unknown type", "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", "weird", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := server.IsValidUploadKey(tc.key, tc.objType)
			if got != tc.valid {
				t.Errorf("IsValidUploadKey(%q, %q) = %v, want %v", tc.key, tc.objType, got, tc.valid)
			}
		})
	}
}

// TestUploadHandlersRejectInvalidKeys ensures handlers actually enforce
// IsValidUploadKey and reject malicious keys before touching DB or S3.
// Validation runs on the request body alone, so a zero-value Service suffices.
func TestUploadHandlersRejectInvalidKeys(t *testing.T) {
	t.Parallel()

	svc := &server.Service{}

	post := func(t *testing.T, h http.HandlerFunc, body any) *httptest.ResponseRecorder {
		t.Helper()

		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(string(data)))
		h.ServeHTTP(rr, req)

		return rr
	}

	t.Run("create pending closure rejects nix-cache-info", func(t *testing.T) {
		t.Parallel()

		rr := post(t, svc.CreatePendingClosureHandler, map[string]any{
			"closure": "00000000000000000000000000000000.narinfo",
			"objects": []map[string]any{
				{"key": "nix-cache-info", "type": "narinfo", "refs": []string{}},
			},
		})
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("create pending closure rejects path traversal", func(t *testing.T) {
		t.Parallel()

		rr := post(t, svc.CreatePendingClosureHandler, map[string]any{
			"closure": "00000000000000000000000000000000.narinfo",
			"objects": []map[string]any{
				{"key": "nar/../../../etc/passwd", "type": "nar", "refs": []string{}},
			},
		})
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("complete multipart rejects non-NAR key", func(t *testing.T) {
		t.Parallel()

		rr := post(t, svc.CompleteMultipartUploadHandler, map[string]any{
			"object_key": "nix-cache-info",
			"upload_id":  "x",
			"parts":      []map[string]any{{"part_number": 1, "etag": "a"}},
		})
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("request more parts rejects non-NAR key", func(t *testing.T) {
		t.Parallel()

		rr := post(t, svc.RequestMorePartsHandler, map[string]any{
			"object_key":        "index.html",
			"upload_id":         "x",
			"start_part_number": 1,
			"num_parts":         1,
		})
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

// TestUploadHandlersRejectOversizedBody ensures handlers cap request body
// size before decoding, so a single request cannot exhaust server memory.
func TestUploadHandlersRejectOversizedBody(t *testing.T) {
	t.Parallel()

	svc := &server.Service{}

	// Slightly over the largest configured limit. Stream of 'a' bytes inside
	// a JSON string field so the decoder keeps reading until cut off.
	oversize := server.MaxClosureRequestBody + 1
	body := `{"closure":"` + strings.Repeat("a", oversize) + `"}`

	handlers := map[string]http.HandlerFunc{
		"create pending closure": svc.CreatePendingClosureHandler,
		"complete multipart":     svc.CompleteMultipartUploadHandler,
		"request more parts":     svc.RequestMorePartsHandler,
	}

	for name, h := range handlers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rr := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("expected 413, got %d: %s", rr.Code, rr.Body.String())
			}
		})
	}
}
