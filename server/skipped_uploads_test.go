package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
)

func TestSkippedUploadsHandler(t *testing.T) {
	t.Parallel()

	s := &server.Service{Metrics: server.NewMetrics()}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/uploads/skipped",
		strings.NewReader(`{"paths": 3, "nar_bytes": 5000000000}`))
	rr := httptest.NewRecorder()
	s.SkippedUploadsHandler(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body: %s", rr.Code, rr.Body.String())
	}

	rec := httptest.NewRecorder()
	s.Metrics.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("reading metrics: %v", err)
	}

	for _, want := range []string{
		"niks3_upload_skipped_paths_total 3",
		"niks3_upload_skipped_nar_bytes_total 5e+09",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
