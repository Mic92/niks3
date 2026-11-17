package client_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/client"
)

// A server from before the rename only has /api/objects/present.
func TestClosuresExist_OlderServer(t *testing.T) {
	t.Parallel()

	path := "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-hello"
	key := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/objects/present" {
			http.NotFound(w, r)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.PresentResponse{Present: []string{key}})
	}))
	defer srv.Close()

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	exists, err := c.ClosuresExist(t.Context(), []string{path})
	if err != nil {
		t.Fatal(err)
	}

	if !exists[path] {
		t.Fatalf("exists=%v, want %s", exists, path)
	}
}
