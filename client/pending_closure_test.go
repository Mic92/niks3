package client_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/client"
)

// The commit deletes the pending closure, so a retry after a lost response is
// answered 404. That is a success if the closure is now present, and still a
// failure if it is not (the server cleaned the pending closure up).
func TestCompletePendingClosure_NotFoundIsSuccessWhenClosurePresent(t *testing.T) {
	t.Parallel()

	const closureKey = "26xbg1ndr7hbcncrlf9nhx5is2b25d13.narinfo"

	for _, tc := range []struct {
		name    string
		present bool
		wantErr bool
	}{
		{name: "committed earlier", present: true, wantErr: false},
		{name: "cleaned up", present: false, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var presentCalls atomic.Int32

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/pending_closures/42/complete":
					http.Error(w, "pending closure not found", http.StatusNotFound)
				case "/api/objects/present":
					presentCalls.Add(1)

					var req api.PresentRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Keys) != 1 || req.Keys[0] != closureKey {
						http.Error(w, "unexpected present request", http.StatusBadRequest)

						return
					}

					resp := api.PresentResponse{Present: []string{}}
					if tc.present {
						resp.Present = []string{closureKey}
					}

					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(resp)
				default:
					http.Error(w, "unexpected request "+r.URL.Path, http.StatusBadRequest)
				}
			}))
			defer srv.Close()

			c, err := client.NewTestClientForServer(srv.URL)
			if err != nil {
				t.Fatal(err)
			}

			err = c.CompletePendingClosure(t.Context(), "pending_closures", "42", closureKey)

			if presentCalls.Load() != 1 {
				t.Errorf("present was queried %d times, want 1", presentCalls.Load())
			}

			if tc.wantErr {
				var statusErr *client.HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
					t.Fatalf("err = %v, want the 404 from the commit", err)
				}
			} else if err != nil {
				t.Fatalf("err = %v, want success: the closure is present", err)
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
	if err := c.CompletePendingClosure(t.Context(), "pending_closures", "42", ""); !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("err = %v, want 404", err)
	}
}
