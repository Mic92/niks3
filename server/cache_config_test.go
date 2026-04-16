package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/signing"
)

// Same key pair as nix/checks/nixos-test-niks3.nix — keep in sync.
//
//nolint:gosec // test-only key, also committed in the NixOS test fixture
const (
	testSigningSecret = "niks3-test-1:0knWkx/F+6IJmI4dkvNs14SCaewg9ZWSAQUNg9juRxh/8x+rzUJx9SWdyGOVl21IbJlQemUKG40qW2TTyrE++w=="
	testSigningPublic = "niks3-test-1:f/Mfq81CcfUlnchjlZdtSGyZUHplChuNKltk08qxPvs="
)

func TestCacheConfigHandler(t *testing.T) {
	t.Parallel()

	key, err := signing.ParseKey(testSigningSecret)
	if err != nil {
		t.Fatalf("ParseKey: %v", err)
	}

	tests := []struct {
		name     string
		cacheURL string
		keys     []*signing.Key
		query    string
		want     api.CacheConfig
	}{
		{
			name:     "full config, no issuer",
			cacheURL: "https://cache.example.com",
			keys:     []*signing.Key{key},
			query:    "",
			want: api.CacheConfig{
				SubstituterURL: "https://cache.example.com",
				PublicKeys:     []string{testSigningPublic},
			},
		},
		{
			name:     "no cache url configured",
			cacheURL: "",
			keys:     []*signing.Key{key},
			query:    "",
			want: api.CacheConfig{
				SubstituterURL: "",
				PublicKeys:     []string{testSigningPublic},
			},
		},
		{
			name:     "no signing keys",
			cacheURL: "https://cache.example.com",
			keys:     nil,
			query:    "",
			want: api.CacheConfig{
				SubstituterURL: "https://cache.example.com",
				PublicKeys:     []string{},
			},
		},
		{
			name:     "issuer requested but no OIDC validator",
			cacheURL: "https://cache.example.com",
			keys:     []*signing.Key{key},
			query:    "?issuer=https://token.actions.githubusercontent.com",
			want: api.CacheConfig{
				SubstituterURL: "https://cache.example.com",
				PublicKeys:     []string{testSigningPublic},
				// OIDCAudience omitted — validator is nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := &server.Service{
				CacheURL:    tt.cacheURL,
				SigningKeys: tt.keys,
				// OIDCValidator: nil — issuer→audience wiring is covered by
				// oidc.TestAudienceForIssuer; constructing a Validator
				// requires live discovery.
			}

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/cache-config"+tt.query, nil)
			rr := httptest.NewRecorder()

			s.CacheConfigHandler(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
			}

			if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}

			var got api.CacheConfig
			if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
				t.Fatalf("unmarshal: %v; body: %s", err, rr.Body.String())
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}
