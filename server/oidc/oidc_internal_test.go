package oidc

import (
	"context"
	"errors"
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

	// An issuer that accepts the connection and never answers.
	stalled := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer stalled.Close()

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

			continue
		}

		checkResponseHeaderTimeout(t, cfg.Issuer, client, stalled.URL)
	}
}

// checkResponseHeaderTimeout checks that the client's transport, on its own
// and without the client's overall timeout, gives up on a server that never
// sends a response header.
func checkResponseHeaderTimeout(t *testing.T, name string, client *http.Client, stalledURL string) {
	t.Helper()

	rt := client.Transport
	if bearer, ok := rt.(*bearerFileTransport); ok {
		rt = bearer.base
	}

	transport, ok := rt.(*http.Transport)
	if !ok || transport == http.DefaultTransport {
		t.Errorf("%s: transport %T is not the client's own *http.Transport", name, client.Transport)

		return
	}

	if transport.ResponseHeaderTimeout <= 0 {
		t.Errorf("%s: transport has no response header timeout", name)

		return
	}

	// Shorten only the bound under test so the stall shows in milliseconds.
	// The overall timeout stays longer than the deadline below, so nothing
	// else can end the request in time.
	transport.ResponseHeaderTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, stalledURL+"/.well-known/openid-configuration", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()

		t.Errorf("%s: request to a stalled issuer returned without error", name)

		return
	}

	if ctx.Err() != nil {
		t.Errorf("%s: request to a stalled issuer still pending after 5s: %v", name, err)

		return
	}

	// bearerFileTransport wraps the error, which hides it from url.Error's
	// Timeout, but net/http's timeout errors also match DeadlineExceeded.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("%s: err = %v, want a timeout", name, err)
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
