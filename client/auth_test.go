package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Mic92/niks3/client"
)

// TestDoServerRequestAttachesToken verifies the Authorization header is
// resolved from the TokenSource per request, not snapshotted at construction.
func TestDoServerRequestAttachesToken(t *testing.T) {
	t.Parallel()

	tok := "tok-a"

	var seen []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ts := client.TokenSource(func(context.Context) (string, error) { return tok, nil })
	c := client.NewTestClientWithToken(&http.Client{}, client.DefaultRetryConfig(), ts)

	for _, want := range []string{"Bearer tok-a", "Bearer tok-b"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := c.DoServerRequest(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}

		_ = resp.Body.Close()

		if got := seen[len(seen)-1]; got != want {
			t.Fatalf("Authorization = %q, want %q", got, want)
		}

		tok = "tok-b" // rotate for the next iteration
	}
}
