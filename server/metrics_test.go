package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestMetricsInventory verifies the /metrics endpoint reports the cache
// inventory gauges sourced from object_stats.
func TestMetricsInventory(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()
	databaseOnlyTest(t)

	ctx := t.Context()
	queries := pg.New(service.Pool)

	hash := "cccccccccccccccccccccccccccccccc"
	narKey := "nar/" + hash + ".nar.zst"

	pendingClosure, err := queries.InsertPendingClosure(ctx, hash+".narinfo")
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pendingClosure.ID, Key: hash + ".narinfo", Refs: []string{narKey}},
		{PendingClosureID: pendingClosure.ID, Key: narKey, Refs: []string{}, Size: pgtype.Int8{Int64: 4096, Valid: true}},
	})
	ok(t, err)
	ok(t, queries.CommitPendingClosure(ctx, pendingClosure.ID))

	service.StartInventoryRefresh(ctx)

	// Drive a request through the middleware so the HTTP metrics populate.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	instrumented := service.Metrics.Instrument(mux)
	instrumented.ServeHTTP(
		httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil),
	)

	// Any token is a valid method to Go's server. Unknown ones must share
	// one label value, or a client can mint series without bound.
	for _, method := range []string{"BOGUS", "M1", "M2"} {
		instrumented.ServeHTTP(
			httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), method, "/health", nil),
		)
	}

	rec := httptest.NewRecorder()
	service.Metrics.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	body, err := io.ReadAll(rec.Body)
	ok(t, err)

	for _, want := range []string{
		"niks3_cache_objects 2",
		"niks3_cache_logical_bytes 4096",
		"niks3_pending_closures 0",
		"niks3_db_connections_max",
		`niks3_http_requests_total{method="GET",route="GET /health",status="200"} 1`,
		`niks3_http_requests_total{method="other",route="unmatched",status="405"} 3`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics output missing %q", want)
		}
	}

	for _, unwanted := range []string{`method="BOGUS"`, `method="M1"`, `method="M2"`} {
		if strings.Contains(string(body), unwanted) {
			t.Errorf("metrics output labels a series with a client-chosen method: %s", unwanted)
		}
	}
}
