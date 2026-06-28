package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mic92/niks3/api"
)

// TestGCMetrics verifies a garbage collection run is recorded in the GC
// instruments exposed at /metrics.
func TestGCMetrics(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	status := service.RunGCForTest(0, 0, true)
	if status.State != api.GCTaskStateSucceeded {
		t.Fatalf("GC state = %q, want succeeded (error: %s)", status.State, status.Error)
	}

	rec := httptest.NewRecorder()
	service.Metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	body, err := io.ReadAll(rec.Body)
	ok(t, err)

	for _, want := range []string{
		`niks3_gc_runs_total{result="succeeded"} 1`,
		"niks3_gc_duration_seconds_count 1",
		"niks3_gc_objects_deleted_total 0",
		"niks3_gc_last_run_timestamp_seconds ",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}
