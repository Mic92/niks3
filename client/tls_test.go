package client_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
)

// genCert creates a self-signed certificate for the given CN and writes the
// PEM-encoded certificate and key to dir. It returns the parsed certificate
// and the file paths so tests can both load it from disk (client side) and
// add it to a CertPool (server side).
func genCert(t *testing.T, dir, cn string, isCA bool) (*x509.Certificate, string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  isCA,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing certificate: %v", err)
	}

	certPath := filepath.Join(dir, cn+".crt")
	keyPath := filepath.Join(dir, cn+".key")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("writing cert: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshalling key: %v", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("writing key: %v", err)
	}

	return cert, certPath, keyPath
}

func TestSetClientTLS(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	clientCert, clientCertPath, clientKeyPath := genCert(t, dir, "client", false)
	_, serverCertPath, serverKeyPath := genCert(t, dir, "server", true)

	clientPool := x509.NewCertPool()
	clientPool.AddCert(clientCert)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))

	serverTLSCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	if err != nil {
		t.Fatalf("loading server cert: %v", err)
	}

	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientPool,
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	t.Run("rejects connection without client cert", func(t *testing.T) {
		t.Parallel()

		c := client.NewTestClient(&http.Client{}, client.DefaultRetryConfig())

		resp, err := doGet(t, c, srv.URL)
		if err == nil {
			_ = resp.Body.Close()

			t.Fatal("expected handshake to fail without client certificate")
		}
	})

	t.Run("succeeds with client cert and CA", func(t *testing.T) {
		t.Parallel()

		c := client.NewTestClient(&http.Client{}, client.DefaultRetryConfig())
		if err := c.SetClientTLS(clientCertPath, clientKeyPath, serverCertPath); err != nil {
			t.Fatalf("SetClientTLS: %v", err)
		}

		resp, err := doGet(t, c, srv.URL)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status: %d", resp.StatusCode)
		}
	})

	t.Run("preserves debug logging transport", func(t *testing.T) {
		t.Parallel()

		c := client.NewTestClient(&http.Client{}, client.DefaultRetryConfig())
		c.SetDebugHTTP(true)

		if err := c.SetClientTLS(clientCertPath, clientKeyPath, serverCertPath); err != nil {
			t.Fatalf("SetClientTLS: %v", err)
		}

		resp, err := doGet(t, c, srv.URL)
		if err != nil {
			t.Fatalf("request through logging transport failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status: %d", resp.StatusCode)
		}
	})
}

// TestSetClientTLSDoesNotMutateDefaultTransport asserts SetClientTLS clones
// rather than reuses http.DefaultTransport. We compare against the client's
// own transport instead of snapshotting the global because httptest servers
// in other tests mutate http.DefaultTransport.TLSClientConfig.RootCAs.
func TestSetClientTLSDoesNotMutateDefaultTransport(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_, certPath, keyPath := genCert(t, dir, "client", false)

	c := client.NewTestClient(&http.Client{}, client.DefaultRetryConfig())
	if err := c.SetClientTLS(certPath, keyPath, ""); err != nil {
		t.Fatalf("SetClientTLS: %v", err)
	}

	if c.HTTPClient().Transport == http.DefaultTransport {
		t.Fatal("SetClientTLS reused http.DefaultTransport instead of cloning it")
	}

	dt, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatal("http.DefaultTransport is not *http.Transport")
	}

	if dt.TLSClientConfig != nil && len(dt.TLSClientConfig.Certificates) > 0 {
		t.Fatal("SetClientTLS leaked the client certificate into http.DefaultTransport")
	}
}

func doGet(t *testing.T, c *client.Client, url string) (*http.Response, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}

	resp, err := c.HTTPClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}

	return resp, nil
}

func TestSetClientTLSErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_, certPath, keyPath := genCert(t, dir, "client", false)

	bogus := filepath.Join(dir, "bogus.pem")
	if err := os.WriteFile(bogus, []byte("not pem"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		cert string
		key  string
		ca   string
	}{
		{"missing cert file", filepath.Join(dir, "nope.crt"), keyPath, ""},
		{"missing key file", certPath, filepath.Join(dir, "nope.key"), ""},
		{"missing ca file", certPath, keyPath, filepath.Join(dir, "nope.ca")},
		{"invalid ca file", certPath, keyPath, bogus},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := client.NewTestClient(&http.Client{}, client.DefaultRetryConfig())
			if err := c.SetClientTLS(tt.cert, tt.key, tt.ca); err == nil {
				t.Fatalf("expected error for %s", tt.name)
			}
		})
	}
}
