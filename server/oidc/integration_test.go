package oidc_test

import (
	"errors"
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
				Audience: m.Config().ClientID,
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
		"aud": m.Config().ClientID, // Different from what validator expects
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
				Audience: m.Config().ClientID,
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	// Create an already-expired token
	token := oidctest.SignToken(t, m, jwt.MapClaims{
		"sub": "test-subject",
		"exp": m.Now().Add(-time.Hour).Unix(), // Expired 1 hour ago
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
				Audience: m.Config().ClientID,
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
				Audience:     m.Config().ClientID,
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
				Audience: m1.Config().ClientID,
			},
			"provider2": {
				Issuer:   m2.Issuer(),
				Audience: m2.Config().ClientID,
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	// Token from second provider should be validated
	token := oidctest.SignToken(t, m2, jwt.MapClaims{
		"sub": "test-subject-from-provider2",
		"iss": m2.Issuer(),
		"aud": m2.Config().ClientID,
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

func TestValidateToken_NoMatchingProvider(t *testing.T) {
	t.Parallel()

	m1 := oidctest.StartMockOIDC(t)
	m2 := oidctest.StartMockOIDC(t)

	// Configure only m1
	config := oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"provider1": {
				Issuer:   m1.Issuer(),
				Audience: m1.Config().ClientID,
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	// Token from m2 (not configured) should fail
	token := oidctest.SignToken(t, m2, jwt.MapClaims{
		"sub": "test-subject",
		"iss": m2.Issuer(),
		"aud": m2.Config().ClientID,
	})

	_, err := validator.ValidateToken(ctx, token)
	if err == nil {
		t.Fatal("expected error for token from unconfigured provider, got nil")
	}

	var validationErr *oidc.ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}

	if len(validationErr.TriedProviders) != 1 {
		t.Errorf("expected 1 tried provider, got %d", len(validationErr.TriedProviders))
	}
}
