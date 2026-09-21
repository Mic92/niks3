package oidc

import (
	"testing"
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
