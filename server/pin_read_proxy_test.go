package server_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	minio "github.com/minio/minio-go/v7"
)

// setupPinScopedProxyServer creates an httptest.Server with read proxy wrapped with RequireRead.
func setupPinScopedProxyServer(tb testing.TB, service *server.Service) *httptest.Server {
	tb.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", service.HealthCheckHandler)

	if service.EnableReadProxy {
		// Wrap ReadProxyHandler with RequireScope(Read, ...)
		mux.HandleFunc("/{path...}", service.RequireRead(service.ReadProxyHandler))
	} else {
		mux.HandleFunc("GET /", service.RootRedirectHandler)
	}

	return httptest.NewServer(mux)
}

// TestPinScopedReads verifies pin-scoped clients can only read objects reachable from their pins.
func TestPinScopedReads(t *testing.T) {
	t.Parallel()

	service := createTestServiceWithAuth(t, staticTokenForTests)
	service.EnableReadProxy = true
	service.MTLSProxyHeader = "X-Ssl-Client-Verify"
	service.MTLSSubjectHeader = "X-Ssl-Client-Dn"
	service.MTLSBoundSubjects = []string{"CN=writer"}
	service.MTLSPinReadRules = []server.PinReadRule{
		{Subject: "CN=*", Pin: "host-{cn}"},
	}

	defer service.Close()

	ctx := t.Context()

	// Create the closure graph from Task 1:
	// a.narinfo -> nar/a.nar.zst, b.narinfo (b references nar/b)
	// b.narinfo references -> nar/b.nar.zst
	// c.narinfo -> (unrelated, no closure)
	aKey := strings.Repeat("a", 32) + ".narinfo"
	bKey := strings.Repeat("b", 32) + ".narinfo"
	cKey := strings.Repeat("c", 32) + ".narinfo"
	narAKey := "nar/" + strings.Repeat("a", 32) + "00000000000000000000.nar.zst"
	narBKey := "nar/" + strings.Repeat("b", 32) + "00000000000000000000.nar.zst"

	setupGraphObjects(t, service, map[string][]string{
		aKey:    {narAKey, bKey},
		bKey:    {narBKey},
		cKey:    {},
		narAKey: {},
		narBKey: {},
	}, []string{aKey, bKey, cKey, narAKey, narBKey})

	// Create pins: host-web-01 -> a, host-db-01 -> c
	pin(t, service, "host-web-01", strings.Repeat("a", 32))
	pin(t, service, "host-db-01", strings.Repeat("c", 32))

	// Put all objects in S3
	putTestObject(ctx, t, service, aKey, []byte("narinfo a"), minio.PutObjectOptions{})
	putTestObject(ctx, t, service, bKey, []byte("narinfo b"), minio.PutObjectOptions{})
	putTestObject(ctx, t, service, cKey, []byte("narinfo c"), minio.PutObjectOptions{})
	putTestObject(ctx, t, service, narAKey, []byte("nar a content"), minio.PutObjectOptions{})
	putTestObject(ctx, t, service, narBKey, []byte("nar b content"), minio.PutObjectOptions{})
	putTestObject(ctx, t, service, "nix-cache-info", []byte("StoreDir: /nix/store"), minio.PutObjectOptions{})

	ts := setupPinScopedProxyServer(t, service)
	defer ts.Close()

	// Test CN=web-01 can read reachable objects

	// Test: GET /a.narinfo -> 200
	proxyGetWithHeaders(t, ts, "/"+aKey, http.StatusOK, map[string]string{
		"X-Ssl-Client-Verify": "SUCCESS",
		"X-Ssl-Client-Dn":     "CN=web-01",
	})

	// Test: GET /nar/a.nar.zst -> 200
	body := proxyGetWithHeaders(t, ts, "/"+narAKey, http.StatusOK, map[string]string{
		"X-Ssl-Client-Verify": "SUCCESS",
		"X-Ssl-Client-Dn":     "CN=web-01",
	})
	if !bytes.Equal(body, []byte("nar a content")) {
		t.Errorf("body mismatch for nar/a: got %q", body)
	}

	// Test: GET /b.narinfo -> 200 (reachable from a)
	proxyGetWithHeaders(t, ts, "/"+bKey, http.StatusOK, map[string]string{
		"X-Ssl-Client-Verify": "SUCCESS",
		"X-Ssl-Client-Dn":     "CN=web-01",
	})

	// Test: GET /c.narinfo -> 404 (not reachable from a)
	proxyGetWithHeaders(t, ts, "/"+cKey, http.StatusNotFound, map[string]string{
		"X-Ssl-Client-Verify": "SUCCESS",
		"X-Ssl-Client-Dn":     "CN=web-01",
	})

	// Test: HEAD /c.narinfo -> 404
	headWithHeaders(t, ts, "/"+cKey, http.StatusNotFound, map[string]string{
		"X-Ssl-Client-Verify": "SUCCESS",
		"X-Ssl-Client-Dn":     "CN=web-01",
	})

	// Test: GET /pins/host-web-01 -> 200
	proxyGetWithHeaders(t, ts, "/pins/host-web-01", http.StatusOK, map[string]string{
		"X-Ssl-Client-Verify": "SUCCESS",
		"X-Ssl-Client-Dn":     "CN=web-01",
	})

	// Test: GET /pins/host-db-01 -> 404 (web-01 can't read db-01's pins)
	proxyGetWithHeaders(t, ts, "/pins/host-db-01", http.StatusNotFound, map[string]string{
		"X-Ssl-Client-Verify": "SUCCESS",
		"X-Ssl-Client-Dn":     "CN=web-01",
	})

	// Test: GET /index.html -> 404 (pin-scoped can't read index.html per R4)
	proxyGetWithHeaders(t, ts, "/index.html", http.StatusNotFound, map[string]string{
		"X-Ssl-Client-Verify": "SUCCESS",
		"X-Ssl-Client-Dn":     "CN=web-01",
	})

	// Test: GET /nix-cache-info -> 200
	proxyGetWithHeaders(t, ts, "/nix-cache-info", http.StatusOK, map[string]string{
		"X-Ssl-Client-Verify": "SUCCESS",
		"X-Ssl-Client-Dn":     "CN=web-01",
	})
}

// TestPinScopedReadRedirect tests that read redirects work for pin-scoped clients
// and that no Location header is set on denied requests.
func TestPinScopedReadRedirect(t *testing.T) {
	t.Parallel()

	service := createTestServiceWithAuth(t, staticTokenForTests)
	service.EnableReadProxy = true
	service.ReadRedirectTTL = 15 * time.Minute
	service.MTLSProxyHeader = "X-Ssl-Client-Verify"
	service.MTLSBoundSubjects = []string{"CN=writer"}
	service.MTLSSubjectHeader = "X-Ssl-Client-Dn"
	service.MTLSPinReadRules = []server.PinReadRule{
		{Subject: "CN=*", Pin: "host-{cn}"},
	}

	defer service.Close()

	ctx := t.Context()

	// Create the closure graph
	aKey := strings.Repeat("a", 32) + ".narinfo"
	cKey := strings.Repeat("c", 32) + ".narinfo"
	narAKey := "nar/" + strings.Repeat("a", 32) + "00000000000000000000.nar.zst"
	narCKey := "nar/" + strings.Repeat("c", 32) + "00000000000000000000.nar.zst"

	setupGraphObjects(t, service, map[string][]string{
		aKey:    {narAKey},
		cKey:    {narCKey},
		narAKey: {},
		narCKey: {},
	}, []string{aKey, cKey, narAKey, narCKey})

	// Create pins
	pin(t, service, "host-web-01", strings.Repeat("a", 32))
	pin(t, service, "host-db-01", strings.Repeat("c", 32))

	// Put objects in S3
	putTestObject(ctx, t, service, narAKey, bytes.Repeat([]byte("x"), 1024*64), minio.PutObjectOptions{ContentType: "application/x-nix-nar"})
	putTestObject(ctx, t, service, narCKey, bytes.Repeat([]byte("y"), 1024*64), minio.PutObjectOptions{ContentType: "application/x-nix-nar"})
	ts := setupPinScopedProxyServer(t, service)
	defer ts.Close()

	noFollow := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	for nar, wantStatus := range map[string]int{narAKey: http.StatusTemporaryRedirect, narCKey: http.StatusNotFound} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/"+nar, nil)
		ok(t, err)
		req.Header.Set("X-Ssl-Client-Verify", "SUCCESS")
		req.Header.Set("X-Ssl-Client-Dn", "CN=web-01")

		resp, err := noFollow.Do(req)
		ok(t, err)

		_ = resp.Body.Close()

		redirected := resp.Header.Get("Location") != ""
		if resp.StatusCode != wantStatus || redirected != (wantStatus == http.StatusTemporaryRedirect) {
			t.Errorf("GET /%s: status %d, redirected %v", nar, resp.StatusCode, redirected)
		}
	}
}

// proxyGetWithHeaders issues a GET with custom headers against the proxy test server.
func proxyGetWithHeaders(t *testing.T, ts *httptest.Server, path string, wantStatus int, headers map[string]string) []byte {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+path, nil)
	ok(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	ok(t, err)

	body, err := io.ReadAll(resp.Body)
	ok(t, err)

	if err := resp.Body.Close(); err != nil {
		t.Logf("Failed to close response body: %v", err)
	}

	if resp.StatusCode != wantStatus {
		t.Errorf("GET %s: status = %d, want %d", path, resp.StatusCode, wantStatus)
	}

	return body
}

// headWithHeaders issues a HEAD with custom headers against the proxy test server.
func headWithHeaders(t *testing.T, ts *httptest.Server, path string, wantStatus int, headers map[string]string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodHead, ts.URL+path, nil)
	ok(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	ok(t, err)
	_ = resp.Body.Close()

	if resp.StatusCode != wantStatus {
		t.Errorf("HEAD %s: status = %d, want %d", path, resp.StatusCode, wantStatus)
	}
}
