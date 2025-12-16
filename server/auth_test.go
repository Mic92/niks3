package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/oidc"
	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
)

func TestService_AuthMiddleware(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	// check that health check works also with database closed
	service.Pool.Close()

	service.APIToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: service.AuthMiddleware(service.HealthCheckHandler),
		header: map[string]string{
			"Authorization": "Bearer " + service.APIToken,
		},
	})

	checkUnauthorized := func(t *testing.T, w *httptest.ResponseRecorder) {
		t.Helper()

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status code %d, got %d", http.StatusUnauthorized, w.Code)
		}
	}

	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/health",
		handler:       service.AuthMiddleware(service.HealthCheckHandler),
		checkResponse: &checkUnauthorized,
		header: map[string]string{
			"Authorization": "Bearer wrongtoken",
		},
	})
}

func TestService_AuthMiddleware_OIDC(t *testing.T) {
	t.Parallel()

	// Start mock OIDC server
	m, err := mockoidc.Run()
	ok(t, err)
	t.Cleanup(func() {
		if err := m.Shutdown(); err != nil {
			t.Errorf("failed to shutdown mock OIDC server: %v", err)
		}
	})

	// Create OIDC config file
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
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "oidc.json")
	data, err := json.Marshal(config)
	ok(t, err)
	ok(t, os.WriteFile(configPath, data, 0o600))

	// Load config and create validator
	cfg, err := oidc.LoadConfig(configPath)
	ok(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	validator, err := oidc.NewValidator(ctx, cfg)
	ok(t, err)

	// Create test service with OIDC validator
	service := createTestService(t)
	defer service.Close()
	service.Pool.Close() // health check works without DB
	service.OIDCValidator = validator
	service.APIToken = "static-api-token-at-least-36-chars-long"

	// Helper to sign tokens
	signToken := func(claims jwt.MapClaims) string {
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
		ok(t, err)
		return token
	}

	checkUnauthorized := func(t *testing.T, w *httptest.ResponseRecorder) {
		t.Helper()
		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status code %d, got %d", http.StatusUnauthorized, w.Code)
		}
	}

	t.Run("valid OIDC token", func(t *testing.T) {
		token := signToken(jwt.MapClaims{
			"sub":              "repo:myorg/myrepo:ref:refs/heads/main",
			"repository_owner": "myorg",
		})

		testRequest(t, &TestRequest{
			method:  "GET",
			path:    "/health",
			handler: service.AuthMiddleware(service.HealthCheckHandler),
			header: map[string]string{
				"Authorization": "Bearer " + token,
			},
		})
	})

	t.Run("OIDC token with wrong org rejected", func(t *testing.T) {
		token := signToken(jwt.MapClaims{
			"sub":              "repo:otherorg/repo:ref:refs/heads/main",
			"repository_owner": "otherorg",
		})

		testRequest(t, &TestRequest{
			method:        "GET",
			path:          "/health",
			handler:       service.AuthMiddleware(service.HealthCheckHandler),
			checkResponse: &checkUnauthorized,
			header: map[string]string{
				"Authorization": "Bearer " + token,
			},
		})
	})

	t.Run("malformed token rejected", func(t *testing.T) {
		testRequest(t, &TestRequest{
			method:        "GET",
			path:          "/health",
			handler:       service.AuthMiddleware(service.HealthCheckHandler),
			checkResponse: &checkUnauthorized,
			header: map[string]string{
				"Authorization": "Bearer not-a-valid-jwt",
			},
		})
	})

	t.Run("static token still works with OIDC configured", func(t *testing.T) {
		testRequest(t, &TestRequest{
			method:  "GET",
			path:    "/health",
			handler: service.AuthMiddleware(service.HealthCheckHandler),
			header: map[string]string{
				"Authorization": "Bearer " + service.APIToken,
			},
		})
	})
}
