package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Mic92/niks3/server/oidc"
	"github.com/Mic92/niks3/server/oidc/oidctest"
	"github.com/golang-jwt/jwt/v5"
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

func TestService_AuthMiddleware_MTLSProxyHeader(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()
	service.Pool.Close()

	service.APIToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	service.MTLSProxyHeader = "X-SSL-Client-Verify"

	// Verified client cert: header set to SUCCESS, no bearer token.
	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: service.AuthMiddleware(service.HealthCheckHandler),
		header: map[string]string{
			"X-SSL-Client-Verify": "SUCCESS",
		},
	})

	checkUnauthorized := func(t *testing.T, w *httptest.ResponseRecorder) {
		t.Helper()

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status code %d, got %d", http.StatusUnauthorized, w.Code)
		}
	}

	// Header present but not SUCCESS — e.g. nginx ssl_verify_client optional
	// passes "NONE" or "FAILED".
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/health",
		handler:       service.AuthMiddleware(service.HealthCheckHandler),
		checkResponse: &checkUnauthorized,
		header: map[string]string{
			"X-SSL-Client-Verify": "NONE",
		},
	})

	// Header configured but not sent: bearer auth still required.
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/health",
		handler:       service.AuthMiddleware(service.HealthCheckHandler),
		checkResponse: &checkUnauthorized,
	})

	// Header not configured: SUCCESS is ignored.
	service.MTLSProxyHeader = ""
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/health",
		handler:       service.AuthMiddleware(service.HealthCheckHandler),
		checkResponse: &checkUnauthorized,
		header: map[string]string{
			"X-SSL-Client-Verify": "SUCCESS",
		},
	})
}

func TestService_AuthMiddleware_MTLSBoundSubjects(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()
	service.Pool.Close()

	service.APIToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	service.MTLSProxyHeader = "X-SSL-Client-Verify"
	service.MTLSSubjectHeader = "X-SSL-Client-Dn"
	service.MTLSBoundSubjects = []string{"CN=ci-runner,*", "CN=admin"}

	checkUnauthorized := func(t *testing.T, w *httptest.ResponseRecorder) {
		t.Helper()

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status code %d, got %d", http.StatusUnauthorized, w.Code)
		}
	}

	// Subject matches a glob pattern.
	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: service.AuthMiddleware(service.HealthCheckHandler),
		header: map[string]string{
			"X-SSL-Client-Verify": "SUCCESS",
			"X-SSL-Client-Dn":     "CN=ci-runner,O=Acme",
		},
	})

	// Subject matches an exact pattern.
	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: service.AuthMiddleware(service.HealthCheckHandler),
		header: map[string]string{
			"X-SSL-Client-Verify": "SUCCESS",
			"X-SSL-Client-Dn":     "CN=admin",
		},
	})

	// Subject does not match.
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/health",
		handler:       service.AuthMiddleware(service.HealthCheckHandler),
		checkResponse: &checkUnauthorized,
		header: map[string]string{
			"X-SSL-Client-Verify": "SUCCESS",
			"X-SSL-Client-Dn":     "CN=untrusted,O=Other",
		},
	})

	// Verified cert but missing subject header.
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/health",
		handler:       service.AuthMiddleware(service.HealthCheckHandler),
		checkResponse: &checkUnauthorized,
		header: map[string]string{
			"X-SSL-Client-Verify": "SUCCESS",
		},
	})

	// Bearer token still works as fallback even when subject doesn't match.
	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: service.AuthMiddleware(service.HealthCheckHandler),
		header: map[string]string{
			"X-SSL-Client-Verify": "SUCCESS",
			"X-SSL-Client-Dn":     "CN=untrusted",
			"Authorization":       "Bearer " + service.APIToken,
		},
	})
}

func TestService_ReadAuthMiddleware(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()
	service.Pool.Close()

	service.MTLSProxyHeader = "X-SSL-Client-Verify"
	service.MTLSSubjectHeader = "X-SSL-Client-Dn"

	checkUnauthorized := func(t *testing.T, w *httptest.ResponseRecorder) {
		t.Helper()

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status code %d, got %d", http.StatusUnauthorized, w.Code)
		}
	}

	// Default: no read bound subjects → public, no headers needed.
	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: service.ReadAuthMiddleware(service.HealthCheckHandler),
	})

	// Read bound subjects set → unauthenticated read rejected.
	service.MTLSBoundSubjectsRead = []string{"CN=reader-*"}
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/health",
		handler:       service.ReadAuthMiddleware(service.HealthCheckHandler),
		checkResponse: &checkUnauthorized,
	})

	// Matching cert subject → allowed.
	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: service.ReadAuthMiddleware(service.HealthCheckHandler),
		header: map[string]string{
			"X-SSL-Client-Verify": "SUCCESS",
			"X-SSL-Client-Dn":     "CN=reader-1",
		},
	})

	// Subject allowed for write but not read.
	service.MTLSBoundSubjects = []string{"CN=writer"}
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/health",
		handler:       service.ReadAuthMiddleware(service.HealthCheckHandler),
		checkResponse: &checkUnauthorized,
		header: map[string]string{
			"X-SSL-Client-Verify": "SUCCESS",
			"X-SSL-Client-Dn":     "CN=writer",
		},
	})
}

func TestService_AuthMiddleware_OIDC(t *testing.T) {
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
	_, validator := oidctest.NewValidator(t, config)

	// Create test service with OIDC validator
	service := createTestService(t)
	t.Cleanup(service.Close)

	service.Pool.Close() // health check works without DB
	service.OIDCValidator = validator
	service.APIToken = "static-api-token-at-least-36-chars-long"

	checkUnauthorized := func(t *testing.T, w *httptest.ResponseRecorder) {
		t.Helper()

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status code %d, got %d", http.StatusUnauthorized, w.Code)
		}
	}

	t.Run("valid OIDC token", func(t *testing.T) {
		t.Parallel()

		token := oidctest.SignToken(t, m, jwt.MapClaims{
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
		t.Parallel()

		token := oidctest.SignToken(t, m, jwt.MapClaims{
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
		t.Parallel()

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
		t.Parallel()

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
