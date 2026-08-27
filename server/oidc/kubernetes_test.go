package oidc_test

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/oidc"
	"github.com/Mic92/niks3/server/oidc/oidctest"
	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
)

// kubeAPIServer mimics the Kubernetes issuer: private CA, discovery and JWKS
// require authentication.
func kubeAPIServer(t *testing.T, kp *mockoidc.Keypair, wantBearer string) *httptest.Server {
	t.Helper()

	jwks, err := kp.JWKS()
	if err != nil {
		t.Fatal(err)
	}

	var srv *httptest.Server

	mux := http.NewServeMux()
	auth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+wantBearer {
				http.Error(w, "unauthorized", http.StatusUnauthorized)

				return
			}

			next(w, r)
		}
	}
	mux.HandleFunc("/.well-known/openid-configuration", auth(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                srv.URL,
			"jwks_uri":                              srv.URL + "/openid/v1/jwks",
			"response_types_supported":              []string{"id_token"},
			"subject_types_supported":               []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	}))
	mux.HandleFunc("/openid/v1/jwks", auth(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/jwk-set+json")
		_, _ = w.Write(jwks)
	}))

	srv = httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

func TestValidateToken_KubernetesServiceAccount(t *testing.T) {
	t.Parallel()

	kp, err := mockoidc.NewKeypair(nil)
	if err != nil {
		t.Fatal(err)
	}

	const discoveryToken = "niks3-own-serviceaccount-token"

	srv := kubeAPIServer(t, kp, discoveryToken)
	dir := t.TempDir()

	caFile := filepath.Join(dir, "ca.crt")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})

	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte(discoveryToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	config := oidc.Config{
		Providers: map[string]*oidc.ProviderConfig{
			"kubernetes": {
				Issuer:          srv.URL,
				Audience:        "niks3",
				BoundSubject:    []string{"system:serviceaccount:ci:*"},
				CAFile:          caFile,
				BearerTokenFile: tokenFile,
			},
		},
	}
	ctx, validator := oidctest.NewValidator(t, config)

	sign := func(sub string) string {
		tok, err := kp.SignJWT(jwt.MapClaims{
			"iss": srv.URL,
			"aud": []string{"niks3"},
			"sub": sub,
			"iat": time.Now().Unix(),
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		if err != nil {
			t.Fatal(err)
		}

		return tok
	}

	claims, err := validator.ValidateToken(ctx, sign("system:serviceaccount:ci:builder"))
	if err != nil {
		t.Fatalf("expected pod in ci namespace to be allowed: %v", err)
	}

	if claims.Provider != "kubernetes" {
		t.Errorf("provider = %q", claims.Provider)
	}

	if _, err := validator.ValidateToken(ctx, sign("system:serviceaccount:default:web")); err == nil {
		t.Fatal("expected pod outside bound namespace to be rejected")
	}
}

func TestNewValidator_KubernetesRequiresCA(t *testing.T) {
	t.Parallel()

	kp, err := mockoidc.NewKeypair(nil)
	if err != nil {
		t.Fatal(err)
	}

	srv := kubeAPIServer(t, kp, "x")

	cfg := &oidc.Config{Providers: map[string]*oidc.ProviderConfig{
		"kubernetes": {Issuer: srv.URL, Audience: "niks3"},
	}}

	if _, err := oidc.NewValidator(t.Context(), cfg); err == nil {
		t.Fatal("expected discovery against private CA without ca_file to fail")
	}
}
