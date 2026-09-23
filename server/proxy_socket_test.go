package server_test

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/oidc"
)

func TestProxyHeadersOnlyTrustedOnSocket(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()
	service.Pool.Close()

	service.APIToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	service.MTLSProxyHeader = "X-SSL-Client-Verify"
	service.MTLSSubjectHeader = "X-SSL-Client-Dn"
	service.MTLSBoundSubjects = []string{"CN=worker-*"}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /write", service.RequireScope(oidc.ScopeWrite, service.HealthCheckHandler))

	lc := net.ListenConfig{}
	tcp, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	ok(t, err)

	sock := filepath.Join(t.TempDir(), "proxy.sock")
	unixLn, err := lc.Listen(context.Background(), "unix", sock)
	ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- server.ServeProxySocketForTest(ctx, mux, tcp, unixLn, "X-SSL-Client-Verify", "X-SSL-Client-Dn")
	}()

	tcpClient := &http.Client{Timeout: 5 * time.Second}
	unixClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sock)
			},
		},
	}

	get := func(c *http.Client, url, dn string) int {
		t.Helper()

		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		ok(t, err)

		if dn != "" {
			req.Header.Set("X-Ssl-Client-Verify", "SUCCESS")
			req.Header.Set("X-Ssl-Client-Dn", dn)
		}

		resp, err := c.Do(req)
		ok(t, err)

		defer func() { _ = resp.Body.Close() }()

		return resp.StatusCode
	}

	tcpURL := "http://" + tcp.Addr().String() + "/write"
	unixURL := "http://proxy/write"

	if got := get(unixClient, unixURL, "CN=worker-a"); got != http.StatusOK {
		t.Errorf("worker header on the proxy socket: got %d, want 200", got)
	}

	if got := get(unixClient, unixURL, "CN=someone"); got != http.StatusUnauthorized {
		t.Errorf("other subject on the proxy socket: got %d, want 401", got)
	}

	if got := get(tcpClient, tcpURL, "CN=worker-a"); got != http.StatusUnauthorized {
		t.Errorf("forged header on the TCP listener: got %d, want 401", got)
	}

	cancel()

	select {
	case err := <-done:
		ok(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("servers did not stop")
	}
}
