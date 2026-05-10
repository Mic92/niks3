package oidc

import (
	"testing"
)

func TestGlobMatch(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		// Exact matches
		{"foo", "foo", true},
		{"foo", "bar", false},

		// Star wildcard
		{"*", "", true},
		{"*", "anything", true},
		{"foo*", "foo", true},
		{"foo*", "foobar", true},
		{"foo*", "bar", false},
		{"*bar", "bar", true},
		{"*bar", "foobar", true},
		{"*bar", "foo", false},
		{"foo*bar", "foobar", true},
		{"foo*bar", "foo123bar", true},
		{"foo*bar", "foobarbaz", false},

		// Multiple stars
		{"*/*", "foo/bar", true},
		{"*/*", "foo", false},
		{"refs/heads/*", "refs/heads/main", true},
		{"refs/heads/*", "refs/tags/v1.0", false},
		{"refs/*/main", "refs/heads/main", true},

		// Question mark
		{"fo?", "foo", true},
		{"fo?", "fo", false},
		{"fo?", "fooo", false},
		{"?oo", "foo", true},
		{"?oo", "boo", true},

		// GitHub Actions patterns
		{"repo:myorg/*:*", "repo:myorg/myrepo:ref:refs/heads/main", true},
		{"repo:myorg/*:*", "repo:otherorg/myrepo:ref:refs/heads/main", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.value, func(t *testing.T) {
			if got := globMatch(tt.pattern, tt.value); got != tt.want {
				t.Errorf("globMatch(%q, %q) = %v, want %v", tt.pattern, tt.value, got, tt.want)
			}
		})
	}
}

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
