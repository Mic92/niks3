package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Mic92/niks3/server/oidc"
	"github.com/Mic92/niks3/server/oidc/oidctest"
	"github.com/golang-jwt/jwt/v5"
)

func TestCreatePin_ReservedPins(t *testing.T) {
	t.Parallel()

	m := oidctest.StartMockOIDC(t)
	_, validator := oidctest.NewValidator(t, oidc.Config{
		AllowInsecure: true,
		Providers: map[string]*oidc.ProviderConfig{
			"test": {
				Issuer:   m.Issuer(),
				Audience: m.ClientID,
				Rules: []oidc.Rule{
					{BoundSubject: []string{"ci:*"}, Scopes: []oidc.Scope{oidc.ScopeWrite}},
					{BoundSubject: []string{"ci:main"}, Scopes: []oidc.Scope{oidc.ScopeWrite}, Pins: []string{"worker-*"}},
				},
			},
		},
	})

	service := createTestService(t)
	t.Cleanup(service.Close)

	service.OIDCValidator = validator
	service.APIToken = "static-api-token-at-least-36-chars-long"

	bearer := func(sub string) string {
		return "Bearer " + oidctest.SignToken(t, m, jwt.MapClaims{"sub": sub})
	}

	for _, c := range []struct {
		name, auth, pin string
		forbidden       bool
	}{
		{"other rule, reserved pin", bearer("ci:app"), "worker-x86_64-linux", true},
		{"reserving rule", bearer("ci:main"), "worker-x86_64-linux", false},
		{"other rule, free pin", bearer("ci:app"), "my-app", false},
		{"static token", "Bearer " + service.APIToken, "worker-x86_64-linux", false},
	} {
		check := func(t *testing.T, w *httptest.ResponseRecorder) {
			t.Helper()

			// allowed requests pass the check and then fail on the empty body
			if got := w.Code == http.StatusForbidden; got != c.forbidden {
				t.Errorf("%s: status %d, forbidden want %v", c.name, w.Code, c.forbidden)
			}
		}

		testRequest(t, &TestRequest{
			method:        "POST",
			path:          "/api/pins/" + c.pin,
			pathValues:    map[string]string{"name": c.pin},
			handler:       service.RequireScope(oidc.ScopeWrite, service.CreatePinHandler),
			header:        map[string]string{"Authorization": c.auth},
			checkResponse: &check,
		})
	}
}
