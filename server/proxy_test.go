package server_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/klauspost/compress/zstd"
	minio "github.com/minio/minio-go/v7"
)

func TestIsValidCachePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		path  string
		valid bool
	}{
		// Valid patterns
		{"narinfo", "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", true},
		{"narinfo all nix base32 chars", "0123456789abcdfghijklmnpqrsvwxyz.narinfo", true},
		{"nar zst", "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.zst", true},
		{"nar xz", "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.xz", true},
		{"nar bz2", "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.bz2", true},
		{"nar uncompressed", "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar", true},
		{"ls", "26xbg1ndr7hbcncrlf9nhx5is2b25d13.ls", true},
		{"log", "log/k3b2gg5n0p2q8r9t1v4w6x7y-my-package-1.0.drv", true},
		{"realisation", "realisations/sha256:abc123def456!out.doi", true},
		{"nix-cache-info", "nix-cache-info", true},
		{"index.html", "index.html", true},

		// Path traversal
		{"traversal parent", "../etc/passwd", false},
		{"traversal in middle", "nar/../../../etc/passwd", false},

		// Wrong nix base32 chars (e, t, u are not in nix base32)
		{"invalid char e", "26xbg1ndr7hbcncrlf9nhx5is2b25e13.narinfo", false},
		{"invalid char u", "26xbg1ndr7hbcncrlf9nhx5is2b25u13.narinfo", false},

		// Not in allowlist
		{"random path", "foo/bar/baz", false},
		{"empty", "", false},
		{"leading slash", "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", false},
		{"wrong extension", "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo.bak", false},
		{"short hash", "abc.narinfo", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := server.IsValidCachePath(tc.path)
			if got != tc.valid {
				t.Errorf("IsValidCachePath(%q) = %v, want %v", tc.path, got, tc.valid)
			}
		})
	}
}

// zstdCompress compresses data with zstd for test fixtures.
func zstdCompress(tb testing.TB, data []byte) []byte {
	tb.Helper()

	encoder, err := zstd.NewWriter(nil)
	ok(tb, err)

	return encoder.EncodeAll(data, nil)
}

// createProxyTestService creates a test service with read proxy enabled.
func createProxyTestService(tb testing.TB) *server.Service {
	tb.Helper()

	service := createTestService(tb)
	service.EnableReadProxy = true

	return service
}

// putTestObject uploads a test object to the service's S3 bucket.
func putTestObject(ctx context.Context, tb testing.TB, service *server.Service, key string, content []byte, contentType string) {
	tb.Helper()

	_, err := service.MinioClient.PutObject(ctx, service.Bucket, key,
		bytes.NewReader(content), int64(len(content)),
		minio.PutObjectOptions{ContentType: contentType})
	ok(tb, err)
}

// setupProxyServer creates an httptest.Server with proxy routes registered.
// Mirrors the route registration logic from runServer().
func setupProxyServer(tb testing.TB, service *server.Service) *httptest.Server {
	tb.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", service.HealthCheckHandler)

	if service.EnableReadProxy {
		// Register without method prefix so both GET and HEAD are handled.
		// Go's ServeMux would auto-add HEAD for a GET handler, but that
		// conflicts with the more-specific "GET /health" pattern.
		mux.HandleFunc("/{path...}", service.ReadProxyHandler)
	} else {
		mux.HandleFunc("GET /", service.RootRedirectHandler)
	}

	return httptest.NewServer(mux)
}

func TestReadProxyNarinfo(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	// Narinfos are stored zstd-compressed in S3 — the proxy must decompress.
	plainNarinfo := []byte("StorePath: /nix/store/abc123-hello\nURL: nar/abc.nar.zst\n")
	compressed := zstdCompress(t, plainNarinfo)
	putTestObject(ctx, t, service, "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", compressed, "application/x-nix-narinfo")

	ts := setupProxyServer(t, service)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo")
	ok(t, err)

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	ok(t, err)

	if !bytes.Equal(body, plainNarinfo) {
		t.Errorf("body mismatch: got %q, want %q", body, plainNarinfo)
	}

	if ct := resp.Header.Get("Content-Type"); ct != "text/x-nix-narinfo" {
		t.Errorf("Content-Type = %q, want text/x-nix-narinfo", ct)
	}

	if resp.Header.Get("ETag") == "" {
		t.Error("expected ETag header")
	}

	if resp.Header.Get("Last-Modified") == "" {
		t.Error("expected Last-Modified header")
	}
}

func TestReadProxyNarStreaming(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	// 64KB to exercise streaming (not just buffered in a single chunk)
	narContent := bytes.Repeat([]byte("x"), 1024*64)
	putTestObject(ctx, t, service, "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.zst", narContent, "application/x-nix-nar")

	ts := setupProxyServer(t, service)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.zst")
	ok(t, err)

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	ok(t, err)

	if len(body) != len(narContent) {
		t.Errorf("body length = %d, want %d", len(body), len(narContent))
	}
}

func TestReadProxy404(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ts := setupProxyServer(t, service)
	defer ts.Close()

	// Valid path but object doesn't exist in S3
	resp, err := http.Get(ts.URL + "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo")
	ok(t, err)

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for missing object, got %d", resp.StatusCode)
	}
}

func TestReadProxyInvalidPath(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ts := setupProxyServer(t, service)
	defer ts.Close()

	for _, path := range []string{
		"/../../etc/passwd",
		"/foo/bar/baz",
		"/some-random-file.txt",
	} {
		resp, err := http.Get(ts.URL + path)
		ok(t, err)

		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("path %q: expected 404, got %d", path, resp.StatusCode)
		}
	}
}

func TestReadProxyHead(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	compressed := zstdCompress(t, []byte("StorePath: /nix/store/abc123-hello\n"))
	putTestObject(ctx, t, service, "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", compressed, "application/x-nix-narinfo")

	ts := setupProxyServer(t, service)
	defer ts.Close()

	resp, err := http.Head(ts.URL + "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo")
	ok(t, err)

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	ok(t, err)

	if len(body) != 0 {
		t.Errorf("HEAD body should be empty, got %d bytes", len(body))
	}

	// Narinfo HEAD omits Content-Length (compressed size != decompressed size),
	// but must report the correct Content-Type.
	if ct := resp.Header.Get("Content-Type"); ct != "text/x-nix-narinfo" {
		t.Errorf("Content-Type = %q, want text/x-nix-narinfo", ct)
	}
}

func TestReadProxyConditionalGet(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	putTestObject(ctx, t, service, "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo",
		zstdCompress(t, []byte("StorePath: /nix/store/abc123-hello\n")), "application/x-nix-narinfo")

	ts := setupProxyServer(t, service)
	defer ts.Close()

	// First GET to capture ETag
	resp, err := http.Get(ts.URL + "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo")
	ok(t, err)

	etag := resp.Header.Get("ETag")

	if err := resp.Body.Close(); err != nil {
		t.Logf("Failed to close response body: %v", err)
	}

	if etag == "" {
		t.Fatal("expected ETag in first response")
	}

	// Conditional GET with matching ETag → 304
	ctx2, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx2, "GET", ts.URL+"/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", nil)
	ok(t, err)

	req.Header.Set("If-None-Match", etag)

	resp2, err := http.DefaultClient.Do(req)
	ok(t, err)

	defer func() {
		if err := resp2.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}()

	if resp2.StatusCode != http.StatusNotModified {
		t.Errorf("expected 304 with matching ETag, got %d", resp2.StatusCode)
	}
}

func TestReadProxyDisabled(t *testing.T) {
	t.Parallel()

	service := createTestService(t) // proxy NOT enabled
	defer service.Close()

	ts := setupProxyServer(t, service)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo")
	ok(t, err)

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 when proxy disabled, got %d", resp.StatusCode)
	}
}
