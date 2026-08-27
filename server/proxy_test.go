package server_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
func putTestObject(ctx context.Context, tb testing.TB, service *server.Service, key string, content []byte, opts minio.PutObjectOptions) {
	tb.Helper()

	_, err := service.MinioClient.PutObject(ctx, service.Bucket, key,
		bytes.NewReader(content), int64(len(content)), opts)
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

// proxyGet issues a GET against the proxy test server, asserts the response
// status and returns the response headers and body.
func proxyGet(t *testing.T, ts *httptest.Server, path string, wantStatus int) (http.Header, []byte) {
	t.Helper()

	resp, err := http.Get(ts.URL + path)
	ok(t, err)

	body, err := io.ReadAll(resp.Body)
	ok(t, err)

	if err := resp.Body.Close(); err != nil {
		t.Logf("Failed to close response body: %v", err)
	}

	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s: status = %d, want %d", path, resp.StatusCode, wantStatus)
	}

	return resp.Header, body
}

func TestReadProxyNarinfo(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	// Narinfos are stored zstd-compressed in S3 — the proxy must decompress.
	plainNarinfo := []byte("StorePath: /nix/store/abc123-hello\nURL: nar/abc.nar.zst\n")
	compressed := zstdCompress(t, plainNarinfo)
	putTestObject(ctx, t, service, "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", compressed,
		minio.PutObjectOptions{ContentType: "application/x-nix-narinfo", ContentEncoding: "zstd"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	header, body := proxyGet(t, ts, "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", http.StatusOK)

	if !bytes.Equal(body, plainNarinfo) {
		t.Errorf("body mismatch: got %q, want %q", body, plainNarinfo)
	}

	if ct := header.Get("Content-Type"); ct != "text/x-nix-narinfo" {
		t.Errorf("Content-Type = %q, want text/x-nix-narinfo", ct)
	}

	if header.Get("ETag") == "" {
		t.Error("expected ETag header")
	}

	if header.Get("Last-Modified") == "" {
		t.Error("expected Last-Modified header")
	}
}

// TestReadProxyNarinfoAlreadyDecompressed verifies that narinfos already
// decompressed by a transparent proxy (e.g. Cloudflare Tunnel) are served
// as-is when Content-Encoding is absent.
func TestReadProxyNarinfoAlreadyDecompressed(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	// Simulate a transparent proxy that already decompressed the narinfo:
	// plain text in S3 with no Content-Encoding header.
	plainNarinfo := []byte("StorePath: /nix/store/abc123-hello\nURL: nar/abc.nar.zst\n")
	putTestObject(ctx, t, service, "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", plainNarinfo,
		minio.PutObjectOptions{ContentType: "application/x-nix-narinfo"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	header, body := proxyGet(t, ts, "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", http.StatusOK)

	if !bytes.Equal(body, plainNarinfo) {
		t.Errorf("body mismatch: got %q, want %q", body, plainNarinfo)
	}

	if ct := header.Get("Content-Type"); ct != "text/x-nix-narinfo" {
		t.Errorf("Content-Type = %q, want text/x-nix-narinfo", ct)
	}
}

func TestReadProxyNarStreaming(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	// 64KB to exercise streaming (not just buffered in a single chunk)
	narContent := bytes.Repeat([]byte("x"), 1024*64)
	putTestObject(ctx, t, service, "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.zst", narContent,
		minio.PutObjectOptions{ContentType: "application/x-nix-nar"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	_, body := proxyGet(t, ts, "/nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.zst", http.StatusOK)

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
	proxyGet(t, ts, "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", http.StatusNotFound)
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
		proxyGet(t, ts, path, http.StatusNotFound)
	}
}

func TestReadProxyHead(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	compressed := zstdCompress(t, []byte("StorePath: /nix/store/abc123-hello\n"))
	putTestObject(ctx, t, service, "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", compressed,
		minio.PutObjectOptions{ContentType: "application/x-nix-narinfo", ContentEncoding: "zstd"})

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
		zstdCompress(t, []byte("StorePath: /nix/store/abc123-hello\n")),
		minio.PutObjectOptions{ContentType: "application/x-nix-narinfo", ContentEncoding: "zstd"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	// First GET to capture ETag
	header, _ := proxyGet(t, ts, "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", http.StatusOK)

	etag := header.Get("ETag")
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

func TestReadProxyRootRedirectsToIndexHTML(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	service.CacheURL = "https://cache.example.com"
	defer service.Close()

	ctx := t.Context()

	// Upload index.html so the redirect target exists
	putTestObject(ctx, t, service, "index.html",
		[]byte("<html><body>landing page</body></html>"),
		minio.PutObjectOptions{ContentType: "text/html"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	// Use a client that does NOT follow redirects so we can inspect the 301
	client := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Get(ts.URL + "/")
	ok(t, err)

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("expected 301, got %d", resp.StatusCode)
	}

	loc := resp.Header.Get("Location")
	if loc != "/index.html" {
		t.Errorf("Location = %q, want /index.html", loc)
	}
}

func TestReadProxyDisabled(t *testing.T) {
	t.Parallel()

	service := createTestService(t) // proxy NOT enabled
	defer service.Close()

	ts := setupProxyServer(t, service)
	defer ts.Close()

	proxyGet(t, ts, "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", http.StatusNotFound)
}

func TestReadRedirectNar(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	service.ReadRedirectTTL = time.Minute

	defer service.Close()

	ctx := t.Context()

	narContent := bytes.Repeat([]byte("x"), 1024*64)
	key := "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.zst"
	putTestObject(ctx, t, service, key, narContent,
		minio.PutObjectOptions{ContentType: "application/x-nix-nar"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	noFollow := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req, err := http.NewRequestWithContext(ctx, method, ts.URL+"/"+key, nil)
		ok(t, err)

		resp, err := noFollow.Do(req)
		ok(t, err)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusTemporaryRedirect {
			t.Fatalf("%s: status = %d, want 307", method, resp.StatusCode)
		}

		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("%s: Cache-Control = %q, want no-store", method, got)
		}

		location, err := url.Parse(resp.Header.Get("Location"))
		ok(t, err)

		if location.Query().Get("X-Amz-Signature") == "" {
			t.Errorf("%s: Location is not presigned: %s", method, location)
		}

		if location.Host == req.Host {
			t.Errorf("%s: Location points back at the proxy: %s", method, location)
		}
	}

	// Following the redirect proves S3 accepts the signature.
	_, body := proxyGet(t, ts, "/"+key, http.StatusOK)

	if !bytes.Equal(body, narContent) {
		t.Errorf("body mismatch: got %d bytes, want %d", len(body), len(narContent))
	}
}

// Narinfos are stored zstd-compressed and must still be decompressed by the proxy.
func TestReadRedirectKeepsNarinfoProxied(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	service.ReadRedirectTTL = time.Minute

	defer service.Close()

	ctx := t.Context()

	plainNarinfo := []byte("StorePath: /nix/store/abc123-hello\nURL: nar/abc.nar.zst\n")
	putTestObject(ctx, t, service, "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", zstdCompress(t, plainNarinfo),
		minio.PutObjectOptions{ContentType: "application/x-nix-narinfo", ContentEncoding: "zstd"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	header, body := proxyGet(t, ts, "/26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo", http.StatusOK)

	if !bytes.Equal(body, plainNarinfo) {
		t.Errorf("body mismatch: got %q, want %q", body, plainNarinfo)
	}

	if got := header.Get("Cache-Control"); got == "no-store" {
		t.Error("narinfo was redirected, want proxied")
	}
}

// TestReadProxyRangeRequest ensures the proxy honors Range headers so an
// interrupted NAR download can resume instead of restarting from zero.
func TestReadProxyRangeRequest(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	narContent := bytes.Repeat([]byte("0123456789"), 1024) // 10 KiB
	key := "nar/1ngi2dxw1f7khrrjamzkkdai393lwcm8s78gvs1ag8k3n82w7bvp.nar.zst"
	putTestObject(ctx, t, service, key, narContent,
		minio.PutObjectOptions{ContentType: "application/x-nix-nar"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	get := func(rangeHeader string) *http.Response {
		t.Helper()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/"+key, nil)
		ok(t, err)

		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}

		resp, err := http.DefaultClient.Do(req)
		ok(t, err)

		return resp
	}

	// HEAD advertises Accept-Ranges.
	headReq, err := http.NewRequestWithContext(ctx, http.MethodHead, ts.URL+"/"+key, nil)
	ok(t, err)

	headResp, err := http.DefaultClient.Do(headReq)
	ok(t, err)
	_ = headResp.Body.Close()

	if got := headResp.Header.Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("HEAD Accept-Ranges = %q, want bytes", got)
	}

	// Closed range -> 206.
	resp := get("bytes=100-199")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("partial: status = %d, want 206", resp.StatusCode)
	}

	if got := resp.Header.Get("Content-Range"); got != "bytes 100-199/10240" {
		t.Errorf("partial: Content-Range = %q, want bytes 100-199/10240", got)
	}

	body, err := io.ReadAll(resp.Body)
	ok(t, err)
	_ = resp.Body.Close()

	if !bytes.Equal(body, narContent[100:200]) {
		t.Errorf("partial: got %d bytes, want narContent[100:200]", len(body))
	}

	// Open-ended range -> 206.
	resp = get("bytes=10000-")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("open-ended: status = %d, want 206", resp.StatusCode)
	}

	body, err = io.ReadAll(resp.Body)
	ok(t, err)
	_ = resp.Body.Close()

	if len(body) != 240 {
		t.Errorf("open-ended: body length = %d, want 240", len(body))
	}

	// Range past EOF -> 416.
	resp = get("bytes=99999-")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("unsatisfiable: status = %d, want 416", resp.StatusCode)
	}
}

// Compressed non-narinfo objects must either carry Content-Encoding or be
// decoded. Headers are derived from the key, not from what the writer stored.
func TestReadProxyCompressedLog(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	ctx := t.Context()

	plain := []byte("building...\ndone\n")
	putTestObject(ctx, t, service, "log/abc-hello.drv", zstdCompress(t, plain),
		minio.PutObjectOptions{ContentType: "text/html", ContentEncoding: "zstd"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	header, body := proxyGet(t, ts, "/log/abc-hello.drv", http.StatusOK)

	if !bytes.Equal(body, plain) {
		t.Errorf("body = %q, want decoded %q", body, plain)
	}

	if ct := header.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}

	if header.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("missing nosniff")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/log/abc-hello.drv", nil)
	ok(t, err)
	req.Header.Set("Accept-Encoding", "gzip, zstd")

	resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(req)
	ok(t, err)

	defer func() { _ = resp.Body.Close() }()

	if ce := resp.Header.Get("Content-Encoding"); ce != "zstd" {
		t.Errorf("Content-Encoding = %q, want zstd passthrough", ce)
	}

	raw, err := io.ReadAll(resp.Body)
	ok(t, err)

	if !bytes.Equal(raw, zstdCompress(t, plain)) {
		t.Errorf("expected raw zstd bytes")
	}
}

func TestReadProxyPins(t *testing.T) {
	t.Parallel()

	service := createProxyTestService(t)
	defer service.Close()

	putTestObject(t.Context(), t, service, "pins/release-1.0", []byte("/nix/store/abc-hello\n"),
		minio.PutObjectOptions{ContentType: "text/plain"})

	ts := setupProxyServer(t, service)
	defer ts.Close()

	_, body := proxyGet(t, ts, "/pins/release-1.0", http.StatusOK)

	if string(body) != "/nix/store/abc-hello\n" {
		t.Errorf("body = %q", body)
	}
}
