package server_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestService_NativeMTLS exercises the direct-TLS auth path that reads
// r.TLS.PeerCertificates instead of trusting proxy headers.
func TestService_NativeMTLS(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()
	service.Pool.Close()

	service.APIToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	service.NativeMTLS = true
	service.MTLSBoundSubjects = []string{"CN=writer"}
	service.MTLSBoundSubjectsRead = []string{"CN=reader"}

	mkReq := func(cn string) *http.Request {
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
		if cn != "" {
			r.TLS = &tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{
					{Subject: pkix.Name{CommonName: cn}},
				},
			}
		} else {
			r.TLS = &tls.ConnectionState{} // anonymous TLS
		}

		return r
	}

	check := func(t *testing.T, h http.HandlerFunc, cn string, wantCode int) {
		t.Helper()

		w := httptest.NewRecorder()
		h(w, mkReq(cn))

		if w.Code != wantCode {
			t.Errorf("cn=%q: got %d, want %d", cn, w.Code, wantCode)
		}
	}

	write := service.AuthMiddleware(service.HealthCheckHandler)
	read := service.ReadAuthMiddleware(service.HealthCheckHandler)

	check(t, write, "writer", http.StatusOK)
	check(t, write, "reader", http.StatusUnauthorized)
	check(t, write, "", http.StatusUnauthorized)

	check(t, read, "reader", http.StatusOK)
	check(t, read, "writer", http.StatusUnauthorized)
	check(t, read, "", http.StatusUnauthorized)

	// Proxy headers ignored under native mTLS.
	r := mkReq("")
	r.Header.Set("X-Ssl-Client-Verify", "SUCCESS")
	r.Header.Set("X-Ssl-Client-Dn", "CN=writer")

	w := httptest.NewRecorder()
	write(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("native mTLS must ignore proxy headers, got %d", w.Code)
	}
}
