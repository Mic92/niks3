package oidc

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAudienceForIssuer(t *testing.T) {
	t.Parallel()

	v := &Validator{
		config: &Config{
			Providers: map[string]*ProviderConfig{
				"github": {
					Issuer:   "https://token.actions.githubusercontent.com",
					Audience: "https://cache.example.com",
				},
				"gitlab": {
					Issuer:   "https://gitlab.com",
					Audience: "niks3",
				},
			},
		},
	}

	tests := []struct {
		issuer  string
		wantAud string
		wantOK  bool
	}{
		{"https://token.actions.githubusercontent.com", "https://cache.example.com", true},
		{"https://gitlab.com", "niks3", true},
		{"https://unknown.example.com", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		aud, ok := v.AudienceForIssuer(tt.issuer)
		if aud != tt.wantAud || ok != tt.wantOK {
			t.Errorf("AudienceForIssuer(%q) = (%q, %v), want (%q, %v)",
				tt.issuer, aud, ok, tt.wantAud, tt.wantOK)
		}
	}
}

// Discovery and JWKS requests must be bounded. go-oidc refreshes keys in a
// detached goroutine and makes every verification of an unknown key id wait
// on that single refresh, so a stalled fetch would wedge them until restart.
func TestHTTPClientForHasTimeouts(t *testing.T) {
	t.Parallel()

	for _, cfg := range []*ProviderConfig{
		{Issuer: "https://plain.example"},
		{Issuer: "https://bearer.example", BearerTokenFile: writeTempFile(t, "token")},
	} {
		client, err := httpClientFor(cfg)
		if err != nil {
			t.Fatal(err)
		}

		if client.Timeout <= 0 {
			t.Errorf("%s: client has no overall timeout", cfg.Issuer)
		}

		if client == http.DefaultClient {
			t.Errorf("%s: shares http.DefaultClient, whose settings are global", cfg.Issuer)
		}
	}

	// A server that accepts the connection and never answers must not hang
	// the request for longer than the timeout.
	stalled := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer stalled.Close()

	client, err := httpClientFor(&ProviderConfig{Issuer: stalled.URL})
	if err != nil {
		t.Fatal(err)
	}

	client.Timeout = 200 * time.Millisecond

	start := time.Now()

	//nolint:noctx // the timeout under test is the client's own
	resp, err := client.Get(stalled.URL + "/.well-known/openid-configuration")
	if err == nil {
		_ = resp.Body.Close()

		t.Fatal("request to a stalled issuer returned without error")
	}

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("request took %s despite the timeout", elapsed)
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("err = %v, want a timeout", err)
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}
