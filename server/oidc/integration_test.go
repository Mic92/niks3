package oidc_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/oidc"
	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
)

// setupMockOIDC starts a mock OIDC server and returns it along with a cleanup function.
func setupMockOIDC(t *testing.T) *mockoidc.MockOIDC {
	t.Helper()

	m, err := mockoidc.Run()
	if err != nil {
		t.Fatalf("failed to start mock OIDC server: %v", err)
	}

	t.Cleanup(func() {
		if err := m.Shutdown(); err != nil {
			t.Errorf("failed to shutdown mock OIDC server: %v", err)
		}
	})

	return m
}

// signToken creates a JWT signed by the mock server's keypair with custom claims.
func signToken(t *testing.T, m *mockoidc.MockOIDC, claims jwt.MapClaims) string {
	t.Helper()

	// Ensure required OIDC claims are present
	if _, ok := claims["iss"]; !ok {
		claims["iss"] = m.Issuer()
	}

	if _, ok := claims["aud"]; !ok {
		claims["aud"] = m.Config().ClientID
	}

	if _, ok := claims["sub"]; !ok {
		claims["sub"] = "test-subject"
	}

	if _, ok := claims["iat"]; !ok {
		claims["iat"] = m.Now().Unix()
	}

	if _, ok := claims["exp"]; !ok {
		claims["exp"] = m.Now().Add(time.Hour).Unix()
	}

	token, err := m.Keypair.SignJWT(claims)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	return token
}

// writeTestConfig writes an OIDC config file and returns its path.
func writeTestConfig(t *testing.T, config oidc.Config) string {
	t.Helper()
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "oidc.json")

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	return configPath
}

func TestValidateToken_ValidToken(t *testing.T) {
	t.Parallel()

	m := setupMockOIDC(t)

	config := oidc.Config{
		AllowInsecure: true, // Allow HTTP for test mock server
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: m.Config().ClientID,
			},
		},
	}
	configPath := writeTestConfig(t, config)

	cfg, err := oidc.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validator, err := oidc.NewValidator(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	token := signToken(t, m, jwt.MapClaims{
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

	m := setupMockOIDC(t)

	config := oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: "https://different-audience.example.com",
			},
		},
	}
	configPath := writeTestConfig(t, config)

	cfg, err := oidc.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validator, err := oidc.NewValidator(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Token is signed for the mock's ClientID, but validator expects different audience
	token := signToken(t, m, jwt.MapClaims{
		"sub": "test-subject",
		"aud": m.Config().ClientID, // Different from what validator expects
	})

	_, err = validator.ValidateToken(ctx, token)
	if err == nil {
		t.Fatal("expected error for wrong audience, got nil")
	}
}

func TestValidateToken_Expired(t *testing.T) {
	t.Parallel()

	m := setupMockOIDC(t)

	config := oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: m.Config().ClientID,
			},
		},
	}
	configPath := writeTestConfig(t, config)

	cfg, err := oidc.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validator, err := oidc.NewValidator(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Create an already-expired token
	token := signToken(t, m, jwt.MapClaims{
		"sub": "test-subject",
		"exp": m.Now().Add(-time.Hour).Unix(), // Expired 1 hour ago
	})

	_, err = validator.ValidateToken(ctx, token)
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
}

func TestValidateToken_BoundClaimsMismatch(t *testing.T) {
	t.Parallel()

	m := setupMockOIDC(t)

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
	configPath := writeTestConfig(t, config)

	cfg, err := oidc.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validator, err := oidc.NewValidator(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Token has wrong repository_owner
	token := signToken(t, m, jwt.MapClaims{
		"sub":              "test-subject",
		"repository_owner": "otherorg",
	})

	_, err = validator.ValidateToken(ctx, token)
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

	m := setupMockOIDC(t)

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
	configPath := writeTestConfig(t, config)

	cfg, err := oidc.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validator, err := oidc.NewValidator(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Token has non-matching subject
	token := signToken(t, m, jwt.MapClaims{
		"sub": "repo:otherorg/myrepo:ref:refs/heads/main",
	})

	_, err = validator.ValidateToken(ctx, token)
	if err == nil {
		t.Fatal("expected error for mismatched bound subject, got nil")
	}
}

func TestValidateToken_MultipleProviders(t *testing.T) {
	t.Parallel()

	m1 := setupMockOIDC(t)
	m2 := setupMockOIDC(t)

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
	configPath := writeTestConfig(t, config)

	cfg, err := oidc.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validator, err := oidc.NewValidator(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Token from second provider should be validated
	token := signToken(t, m2, jwt.MapClaims{
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

	m1 := setupMockOIDC(t)
	m2 := setupMockOIDC(t)

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
	configPath := writeTestConfig(t, config)

	cfg, err := oidc.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validator, err := oidc.NewValidator(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// Token from m2 (not configured) should fail
	token := signToken(t, m2, jwt.MapClaims{
		"sub": "test-subject",
		"iss": m2.Issuer(),
		"aud": m2.Config().ClientID,
	})

	_, err = validator.ValidateToken(ctx, token)
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
