package client_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/client"
)

// The commit deletes the pending closure, so a retry after a lost response is
// answered 404. That is a success if the closure is now present, and still a
// failure if it is not (the server cleaned the pending closure up). A push
// commits all its roots at once, so it succeeded only if every root is
// present: another client may have committed one of them while the server
// cleaned this push up.
func TestCompletePendingClosure_NotFoundIsSuccessWhenClosurePresent(t *testing.T) {
	t.Parallel()

	const (
		rootA = "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo"
		rootB = "3wvjk4qkw0fq0nb3dxb7cxkxqqrs0k3n.narinfo"
	)

	for _, tc := range []struct {
		name    string
		route   string
		keys    []string
		present []string
		wantErr bool
	}{
		{name: "committed earlier", route: "pending_closures", keys: []string{rootA}, present: []string{rootA}},
		{name: "cleaned up", route: "pending_closures", keys: []string{rootA}, present: []string{}, wantErr: true},
		{name: "push committed earlier", route: "pushes", keys: []string{rootA, rootB}, present: []string{rootA, rootB}},
		{name: "push cleaned up, first root committed by another client", route: "pushes", keys: []string{rootA, rootB}, present: []string{rootA}, wantErr: true},
		{name: "push cleaned up, last root committed by another client", route: "pushes", keys: []string{rootA, rootB}, present: []string{rootB}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var presentCalls atomic.Int32

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/" + tc.route + "/42/complete":
					http.Error(w, "not found", http.StatusNotFound)
				case "/api/objects/present":
					presentCalls.Add(1)

					var req api.PresentRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !slices.Equal(req.Keys, tc.keys) {
						http.Error(w, "unexpected present request", http.StatusBadRequest)

						return
					}

					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(api.PresentResponse{Present: tc.present})
				default:
					http.Error(w, "unexpected request "+r.URL.Path, http.StatusBadRequest)
				}
			}))
			defer srv.Close()

			c, err := client.NewTestClientForServer(srv.URL)
			if err != nil {
				t.Fatal(err)
			}

			err = c.CompletePendingClosure(t.Context(), tc.route, "42", tc.keys)

			if presentCalls.Load() != 1 {
				t.Errorf("present was queried %d times, want 1", presentCalls.Load())
			}

			if tc.wantErr {
				var statusErr *client.HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
					t.Fatalf("err = %v, want the 404 from the commit", err)
				}
			} else if err != nil {
				t.Fatalf("err = %v, want success: every root is present", err)
			}
		})
	}
}

// Without a closure key the 404 stands.
func TestCompletePendingClosure_NotFoundWithoutKey(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "pending closure not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c, err := client.NewTestClientForServer(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	var statusErr *client.HTTPStatusError
	if err := c.CompletePendingClosure(t.Context(), "pending_closures", "42", nil); !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("err = %v, want 404", err)
	}
}
