package oidc_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/oidc"
	"github.com/Mic92/niks3/server/oidc/oidctest"
	"github.com/golang-jwt/jwt/v5"
)

func TestPins_ReservedForMatchingRule(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)
	ctx, v := oidctest.NewValidator(t, oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: m.ClientID,
				Rules: []oidc.Rule{
					{BoundSubject: []string{"repo:org/*"}, Scopes: []oidc.Scope{oidc.ScopeWrite}},
					{BoundSubject: []string{"repo:org/infra:main"}, Scopes: []oidc.Scope{oidc.ScopeWrite}, Pins: []string{"worker-*"}},
				},
			},
		},
	})

	if !v.ReservesPin("worker-x86_64-linux") || v.ReservesPin("my-app") {
		t.Error("ReservesPin: want worker-* reserved, my-app free")
	}

	main, err := v.ValidateToken(ctx, oidctest.SignToken(t, m, jwt.MapClaims{"sub": "repo:org/infra:main"}))
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(main.Pins, []string{"worker-*"}) {
		t.Errorf("main: pins = %v", main.Pins)
	}

	other, err := v.ValidateToken(ctx, oidctest.SignToken(t, m, jwt.MapClaims{"sub": "repo:org/app"}))
	if err != nil {
		t.Fatal(err)
	}

	if len(other.Pins) != 0 || !other.Has(oidc.ScopeWrite) {
		t.Errorf("other: pins = %v, scopes = %v", other.Pins, other.Scopes)
	}
}

func TestPins_TopLevelShorthand(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)
	ctx, v := oidctest.NewValidator(t, oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {Issuer: m.Issuer(), Audience: m.ClientID, BoundSubject: []string{"ci:main"}, Pins: []string{"release"}},
		},
	})

	claims, err := v.ValidateToken(ctx, oidctest.SignToken(t, m, jwt.MapClaims{"sub": "ci:main"}))
	if err != nil {
		t.Fatal(err)
	}

	if !v.ReservesPin("release") || v.ReservesPin("release-2") || !slices.Equal(claims.Pins, []string{"release"}) {
		t.Errorf("pins = %v", claims.Pins)
	}
}

func TestPins_ConfigValidation(t *testing.T) {
	t.Parallel()

	for name, c := range map[string]struct {
		provider oidc.ProviderConfig
		want     string
	}{
		"read only": {
			oidc.ProviderConfig{Rules: []oidc.Rule{{Scopes: []oidc.Scope{oidc.ScopeRead}, Pins: []string{"x"}}}},
			"pins need the write or admin scope",
		},
		"bad pattern": {
			oidc.ProviderConfig{Pins: []string{"a/b"}},
			"invalid pin pattern",
		},
		"rules and top-level pins": {
			oidc.ProviderConfig{Pins: []string{"x"}, Rules: []oidc.Rule{{Scopes: []oidc.Scope{oidc.ScopeWrite}}}},
			"rules cannot be combined",
		},
	} {
		p := c.provider
		p.Issuer = "https://issuer.example"
		p.Audience = "aud"

		path := oidctest.WriteConfig(t, oidc.Config{Providers: map[string]*oidc.ProviderConfig{"p": &p}})

		_, err := oidc.LoadConfig(path)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want %q", name, err, c.want)
		}
	}
}
