package oidc_test

import (
	"slices"
	"testing"

	"github.com/Mic92/niks3/server/oidc"
	"github.com/Mic92/niks3/server/oidc/oidctest"
	"github.com/golang-jwt/jwt/v5"
)

func TestScopes_LegacyProviderDefaultsToWrite(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)
	ctx, v := oidctest.NewValidator(t, oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {Issuer: m.Issuer(), Audience: m.Config().ClientID, BoundSubject: []string{"ci:*"}},
		},
	})

	claims, err := v.ValidateToken(ctx, oidctest.SignToken(t, m, jwt.MapClaims{"sub": "ci:a"}))
	if err != nil {
		t.Fatal(err)
	}

	if !claims.Has(oidc.ScopeWrite) || claims.Has(oidc.ScopeAdmin) || claims.Has(oidc.ScopeRead) {
		t.Errorf("scopes = %v, want [write]", claims.Scopes)
	}
}

func TestScopes_Rules(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)
	ctx, v := oidctest.NewValidator(t, oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: m.Config().ClientID,
				Rules: []oidc.Rule{
					{BoundSubject: []string{"sa:ci:*"}, Scopes: []oidc.Scope{oidc.ScopeWrite}},
					{BoundSubject: []string{"sa:ops:gc"}, Scopes: []oidc.Scope{oidc.ScopeAdmin}},
					{BoundClaims: map[string][]string{"team": {"nix"}}, Scopes: []oidc.Scope{oidc.ScopeRead}},
				},
			},
		},
	})

	tests := []struct {
		claims jwt.MapClaims
		want   []oidc.Scope
	}{
		{jwt.MapClaims{"sub": "sa:ci:builder"}, []oidc.Scope{oidc.ScopeWrite}},
		{jwt.MapClaims{"sub": "sa:ops:gc"}, []oidc.Scope{oidc.ScopeAdmin}},
		// Rules union.
		{jwt.MapClaims{"sub": "sa:ci:builder", "team": "nix"}, []oidc.Scope{oidc.ScopeRead, oidc.ScopeWrite}},
		{jwt.MapClaims{"sub": "sa:web:x"}, nil},
	}

	for _, tc := range tests {
		got, err := v.ValidateToken(ctx, oidctest.SignToken(t, m, tc.claims))
		if tc.want == nil {
			if err == nil {
				t.Errorf("%v: expected rejection, got scopes %v", tc.claims, got.Scopes)
			}

			continue
		}

		if err != nil {
			t.Errorf("%v: %v", tc.claims, err)

			continue
		}

		scopes := slices.Clone(got.Scopes)
		slices.Sort(scopes)

		if !slices.Equal(scopes, tc.want) {
			t.Errorf("%v: scopes = %v, want %v", tc.claims, scopes, tc.want)
		}
	}
}

func TestScopes_ConfigValidation(t *testing.T) {
	t.Parallel()

	base := func(p *oidc.ProviderConfig) oidc.Config {
		p.Issuer = "https://issuer.example"
		p.Audience = "aud"

		return oidc.Config{Providers: map[string]*oidc.ProviderConfig{"p": p}}
	}

	bad := []oidc.Config{
		base(&oidc.ProviderConfig{Scopes: []oidc.Scope{"root"}}),
		base(&oidc.ProviderConfig{BoundSubject: []string{"x"}, Rules: []oidc.Rule{{Scopes: []oidc.Scope{oidc.ScopeWrite}}}}),
		base(&oidc.ProviderConfig{Rules: []oidc.Rule{{BoundSubject: []string{"x"}}}}),
	}

	for i, cfg := range bad {
		if _, err := oidc.LoadConfig(oidctest.WriteConfig(t, cfg)); err == nil {
			t.Errorf("config %d: expected validation error", i)
		}
	}
}
