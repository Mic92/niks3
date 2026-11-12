package oidc

import (
	"errors"
	"net/http"
	"net/url"
	"testing"
	"testing/synctest"
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

type silentIssuer struct{}

func (silentIssuer) RoundTrip(r *http.Request) (*http.Response, error) {
	<-r.Context().Done()

	return nil, r.Context().Err() //nolint:wrapcheck // test transport
}

// Verifications of one provider wait for its key fetch in flight, so a
// fetch nobody answers has to end.
func TestProviderClientGivesUpOnSilentIssuer(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		client, err := httpClientFor(&ProviderConfig{})
		if err != nil {
			t.Fatal(err)
		}

		client.Transport = silentIssuer{}

		resp, err := client.Get("https://issuer.example/jwks") //nolint:noctx // the client's own bound is under test
		if err == nil {
			_ = resp.Body.Close()
		}

		if ue := new(url.Error); !errors.As(err, &ue) || !ue.Timeout() {
			t.Errorf("Get: %v, want a timeout", err)
		}
	})
}
