//nolint:testpackage // Test the private API route registration, including path variables.
package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFarmLeadRoutes(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	(&Service{}).registerAPIRoutes(mux)

	for _, tc := range []struct {
		path    string
		pattern string
	}{
		{"/api/farm/lead", "POST /api/farm/lead"},
		{"/api/farm/lead/build-x86", "POST /api/farm/lead/{farmID}"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tc.path, nil)
			_, pattern := mux.Handler(req)
			if pattern != tc.pattern {
				t.Fatalf("route %s matched %q, want %q", tc.path, pattern, tc.pattern)
			}
		})
	}
}
