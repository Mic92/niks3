package server_test

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mic92/niks3/client"
	"github.com/Mic92/niks3/client/oidclogin"
	"github.com/Mic92/niks3/server/oidc"
	"github.com/Mic92/niks3/server/oidc/oidctest"
	"github.com/golang-jwt/jwt/v5"
)

// nativeLoginStore substitutes an in-memory secure store for end-to-end tests.
// The production Secret Service backend is tested separately over private D-Bus.
type nativeLoginStore struct {
	mu      sync.Mutex
	records map[string][]byte
}

func (s *nativeLoginStore) Load(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, found := s.records[key]
	if !found {
		return nil, oidclogin.ErrNotFound
	}

	return data, nil
}

func (s *nativeLoginStore) Save(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[key] = data

	return nil
}

func (s *nativeLoginStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key)

	return nil
}

func TestNativeOIDCLoginPushRefresh(t *testing.T) {
	t.Parallel()
	for _, omitID := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-id-token", true: "omitted-id-token"}[omitID], func(t *testing.T) {
			t.Parallel()
			m := oidctest.StartMockOIDC(t)
			var issuer string
			var refreshes atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					ok(t, json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "jwks_uri": m.JWKSEndpoint(), "token_endpoint": issuer + "/token", "device_authorization_endpoint": issuer + "/device", "id_token_signing_alg_values_supported": []string{"RS256"}}))
				case "/device":
					ok(t, json.NewEncoder(w).Encode(map[string]any{"device_code": "test-device-secret", "user_code": "ABCD", "verification_uri": "https://login.example.com/verify", "expires_in": 600, "interval": 1}))
				case "/token":
					ok(t, r.ParseForm())
					refreshing := r.Form.Get("grant_type") == "refresh_token"
					if refreshing {
						if r.Form.Get("refresh_token") != "test-refresh-initial" {
							t.Error("wrong refresh token submitted")
						}
						refreshes.Add(1)
					}
					response := map[string]any{"access_token": "opaque-access-token", "token_type": "Bearer", "expires_in": 7200, "refresh_token": "test-refresh-initial"}
					if refreshing {
						response["refresh_token"] = "test-refresh-rotated"
					}
					if !refreshing || !omitID {
						response["id_token"] = oidctest.SignToken(t, m, jwt.MapClaims{"iss": issuer, "sub": "native-user", "groups": []string{"cache-writers"}, "jti": strconv.Itoa(int(refreshes.Load()))})
					}
					ok(t, json.NewEncoder(w).Encode(response))
				default:
					http.NotFound(w, r)
				}
			}))
			defer provider.Close()
			issuer = provider.URL
			_, validator := oidctest.NewValidator(t, oidc.Config{AllowInsecure: true, Providers: map[string]*oidc.ProviderConfig{"native": {Issuer: issuer, Audience: m.ClientID, BoundClaims: map[string][]string{"groups": {"cache-writers"}}}}})
			service := createTestService(t)
			t.Cleanup(service.Close)
			service.OIDCValidator = validator
			ok(t, service.InitializeBucket(t.Context()))
			mux := http.NewServeMux()
			registerTestHandlers(mux, service)
			api := httptest.NewTLSServer(mux)
			defer api.Close()
			store := &nativeLoginStore{records: make(map[string][]byte)}
			manager := &oidclogin.Manager{Dir: filepath.Join(t.TempDir(), "profiles"), Store: store}
			ok(t, manager.Login(t.Context(), api.URL, oidclogin.Config{Issuer: issuer, ClientID: m.ClientID}, io.Discard))
			caPath := filepath.Join(t.TempDir(), "ca.pem")
			ok(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw}), 0o600))
			nixEnv := setupIsolatedNixStore(t)
			for i := range 2 {
				// A fresh client and source model separate CLI processes. Force the saved
				// ID token to its refresh time between pushes without sleeping for expiry.
				if i == 1 {
					store.mu.Lock()
					for key, data := range store.records {
						var c oidclogin.Credential
						ok(t, json.Unmarshal(data, &c))
						c.RefreshAt = time.Time{}
						updated, err := json.Marshal(c) //nolint:gosec // G117: synthetic test credential in an in-memory store
						ok(t, err)
						store.records[key] = updated
					}
					store.mu.Unlock()
				}
				c, err := client.NewClientWithTokenSource(t.Context(), api.URL, manager.TokenSource(api.URL))
				ok(t, err)
				ok(t, c.SetClientTLS("", "", caPath))
				c.NixEnv = nixEnv
				path := nixEvalStorePath(t, nixEnv, fmt.Sprintf(`builtins.toFile "native-oidc-%d" "native login %d"`, i, i))
				_, err = c.PushPaths(t.Context(), []string{path})
				ok(t, err)
				c.WaitRegistrations()
				hash, err := client.GetStorePathHash(path)
				ok(t, err)
				verifyNarinfoInS3(t.Context(), t, service, hash, path)
			}
			if refreshes.Load() != 1 {
				t.Fatalf("refresh count = %d, want 1", refreshes.Load())
			}
			status, err := manager.Status(t.Context(), api.URL)
			ok(t, err)
			if status.Refreshable == omitID {
				t.Fatal("incorrect saved refresh capability")
			}
			ok(t, manager.Logout(t.Context(), api.URL))
			token, err := manager.TokenSource(api.URL)(t.Context())
			if token != "" || err == nil || !strings.Contains(err.Error(), "login") {
				t.Fatal("logged out credential still available")
			}
		})
	}
}
