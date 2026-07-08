// Package oidctest provides shared helpers for tests that exercise OIDC
// token validation against a mock provider.
package oidctest

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/oidc"
	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
)

// StartMockOIDC starts a mock OIDC server that is shut down when the test ends.
func StartMockOIDC(t *testing.T) *mockoidc.MockOIDC {
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

// SignToken creates a JWT signed by the mock server's keypair, filling in
// required OIDC claims that are not set explicitly.
func SignToken(t *testing.T, m *mockoidc.MockOIDC, claims jwt.MapClaims) string {
	t.Helper()

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

// WriteConfig writes an OIDC config file and returns its path.
func WriteConfig(t *testing.T, config oidc.Config) string {
	t.Helper()

	configPath := filepath.Join(t.TempDir(), "oidc.json")

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	return configPath
}

// NewValidator round-trips config through a file and LoadConfig, then builds
// a validator, mirroring production startup.
func NewValidator(t *testing.T, config oidc.Config) (context.Context, *oidc.Validator) {
	t.Helper()

	cfg, err := oidc.LoadConfig(WriteConfig(t, config))
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	validator, err := oidc.NewValidator(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	return ctx, validator
}
