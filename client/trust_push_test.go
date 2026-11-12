package client

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Mic92/niks3/api"
)

func TestPushPathsRefusesUntrustedClosure(t *testing.T) {
	store := t.TempDir()
	top := filepath.Join(store, strings.Repeat("a", 32)+"-top")
	dependency := "/nix/store/" + strings.Repeat("b", 32) + "-dependency"

	info, err := json.Marshal(map[string]any{
		top:        map[string]any{"narSize": 2, "narHash": "sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=", "references": []string{dependency}, "ultimate": true},
		dependency: map[string]any{"narSize": 2, "narHash": "sha256-FePFYIlMuycIXPZbWi7LGEiMmZSX9FMbaQenWBzm1Sc=", "signatures": []string{"other-test-1:c2lnbmF0dXJl"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	capture := filepath.Join(t.TempDir(), "path-info.json")
	if err = os.WriteFile(capture, info, 0o600); err != nil {
		t.Fatal(err)
	}

	bin := t.TempDir()
	if err = os.WriteFile(filepath.Join(bin, "nix"), []byte("#!/bin/sh\ncat \"$NIKS3_TEST_PATH_INFO\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("NIKS3_TEST_PATH_INFO", capture)

	upstream, _ := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)).Public().(ed25519.PublicKey)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/objects/present":
			_ = json.NewEncoder(w).Encode(api.PresentResponse{})
		case "/api/cache-config":
			_ = json.NewEncoder(w).Encode(api.CacheConfig{UpstreamPublicKeys: []string{"upstream-test-1:" + base64.StdEncoding.EncodeToString(upstream)}})
		default:
			t.Errorf("refused closure reached %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	c, err := NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	c.storeDir = store

	completed, err := c.PushPaths(t.Context(), []string{top})

	var untrusted *UntrustedPathsError
	if !errors.As(err, &untrusted) || !slices.Equal(untrusted.Paths, []string{dependency}) || len(completed) != 0 {
		t.Fatalf("completed = %v, err = %v; want %s refused", completed, err, dependency)
	}
}
