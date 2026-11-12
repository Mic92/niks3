package oidc_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/oidc"
	"github.com/Mic92/niks3/server/oidc/oidctest"
	"github.com/golang-jwt/jwt/v5"
)

func TestValidateToken_ValidToken(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)

	config := oidc.Config{
		AllowInsecure: true, // Allow HTTP for test mock server
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: m.ClientID,
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	token := oidctest.SignToken(t, m, jwt.MapClaims{
		"sub": "repo:myorg/myrepo:ref:refs/heads/main",
	})

	claims, err := validator.ValidateToken(ctx, token)
	if err != nil {
		t.Fatalf("expected valid token, got error: %v", err)
	}

	if claims.Subject != "repo:myorg/myrepo:ref:refs/heads/main" {
		t.Errorf("expected subject 'repo:myorg/myrepo:ref:refs/heads/main', got %q", claims.Subject)
	}

	if claims.Provider != "test" {
		t.Errorf("expected provider 'test', got %q", claims.Provider)
	}
}

func TestValidateToken_WrongAudience(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)

	config := oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: "https://different-audience.example.com",
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	// Token is signed for the mock's ClientID, but validator expects different audience
	token := oidctest.SignToken(t, m, jwt.MapClaims{
		"sub": "test-subject",
		"aud": m.ClientID, // Different from what validator expects
	})

	_, err := validator.ValidateToken(ctx, token)
	if err == nil {
		t.Fatal("expected error for wrong audience, got nil")
	}
}

func TestValidateToken_Expired(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)

	config := oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: m.ClientID,
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	// Create an already-expired token
	token := oidctest.SignToken(t, m, jwt.MapClaims{
		"sub": "test-subject",
		"exp": time.Now().Add(-time.Hour).Unix(), // Expired 1 hour ago
	})

	_, err := validator.ValidateToken(ctx, token)
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
}

func TestValidateToken_BoundClaimsMismatch(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)

	config := oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: m.ClientID,
				BoundClaims: map[string][]string{
					"repository_owner": {"myorg"},
				},
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	// Token has wrong repository_owner
	token := oidctest.SignToken(t, m, jwt.MapClaims{
		"sub":              "test-subject",
		"repository_owner": "otherorg",
	})

	_, err := validator.ValidateToken(ctx, token)
	if err == nil {
		t.Fatal("expected error for mismatched bound claims, got nil")
	}

	var validationErr *oidc.ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}

	if validationErr.Provider != "test" {
		t.Errorf("expected provider 'test', got %q", validationErr.Provider)
	}
}

func TestValidateToken_BoundSubjectMismatch(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)

	config := oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:       m.Issuer(),
				Audience:     m.ClientID,
				BoundSubject: []string{"repo:myorg/*:*"},
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	// Token has non-matching subject
	token := oidctest.SignToken(t, m, jwt.MapClaims{
		"sub": "repo:otherorg/myrepo:ref:refs/heads/main",
	})

	_, err := validator.ValidateToken(ctx, token)
	if err == nil {
		t.Fatal("expected error for mismatched bound subject, got nil")
	}
}

func TestValidateToken_MultipleProviders(t *testing.T) {
	t.Parallel()

	m1 := oidctest.StartMockOIDC(t)
	m2 := oidctest.StartMockOIDC(t)

	config := oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"provider1": {
				Issuer:   m1.Issuer(),
				Audience: m1.ClientID,
			},
			"provider2": {
				Issuer:   m2.Issuer(),
				Audience: m2.ClientID,
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	// Token from second provider should be validated
	token := oidctest.SignToken(t, m2, jwt.MapClaims{
		"sub": "test-subject-from-provider2",
		"iss": m2.Issuer(),
		"aud": m2.ClientID,
	})

	claims, err := validator.ValidateToken(ctx, token)
	if err != nil {
		t.Fatalf("expected token from second provider to be valid, got error: %v", err)
	}

	if claims.Subject != "test-subject-from-provider2" {
		t.Errorf("expected subject 'test-subject-from-provider2', got %q", claims.Subject)
	}

	if claims.Provider != "provider2" {
		t.Errorf("expected provider 'provider2', got %q", claims.Provider)
	}
}

// A token is verified with the keys of the provider its issuer names, so a
// token from elsewhere cannot make the server fetch anyone's keys.
func TestValidateToken_UnknownIssuerFetchesNoKeys(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int32

	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)

		_, _ = io.WriteString(w, `{"keys":[]}`)
	}))
	defer jwks.Close()

	ctx, validator := oidctest.NewValidator(t, oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"known": {Issuer: "https://known.example", JWKSURL: jwks.URL, Audience: "niks3"},
		},
	})

	_, err := validator.ValidateToken(ctx, oidctest.SignToken(t, oidctest.StartMockOIDC(t), jwt.MapClaims{}))
	if !errors.Is(err, oidc.ErrTokenValidationFailed) {
		t.Fatalf("ValidateToken: %v, want a validation error", err)
	}

	if n := fetches.Load(); n != 0 {
		t.Errorf("%d key fetches for a token of an unconfigured issuer", n)
	}
}

// go-oidc accepts Google's scheme-less issuer for https://accounts.google.com.
func TestValidateToken_GoogleSchemelessIssuer(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)

	ctx, validator := oidctest.NewValidator(t, oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"google": {Issuer: "https://accounts.google.com", JWKSURL: m.JWKSEndpoint(), Audience: m.ClientID},
		},
	})

	if _, err := validator.ValidateToken(ctx, oidctest.SignToken(t, m, jwt.MapClaims{"iss": "accounts.google.com"})); err != nil {
		t.Errorf("ValidateToken: %v", err)
	}
}
