package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	commitOK(t)(queries.CommitPendingClosure(ctx, pendingClosure.ID))

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

	// Unknown methods must share one label, or clients can mint series.
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

// Both commit paths report how long a commit took and how many pending objects
// it folded in.
func TestMetricsCommit(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	base := strings.Repeat("a", 32)
	root := strings.Repeat("b", 32)

	resp := createPush(t, service, []string{root + ".narinfo"}, pkgObjects(base), pkgObjects(root, base))
	completePush(t, service, resp.ID) // 4 pending objects

	ctx := t.Context()
	queries := pg.New(service.Pool)

	other := strings.Repeat("c", 32)
	pending, err := queries.InsertPendingClosure(ctx, other+".narinfo")
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pending.ID, Key: other + ".narinfo", Refs: []string{narKeyFor(other)}},
		{PendingClosureID: pending.ID, Key: narKeyFor(other), Refs: []string{}},
	})
	ok(t, err)

	check := checkStatusCode(http.StatusNoContent)
	id := strconv.FormatInt(pending.ID, 10)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/pending_closures/" + id + "/complete",
		handler:       service.CommitPendingClosureHandler,
		pathValues:    map[string]string{"id": id},
		checkResponse: &check,
	})

	rec := httptest.NewRecorder()
	service.Metrics.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/metrics", nil))

	body := rec.Body.String()

	for _, want := range []string{"niks3_commit_objects_total 6", "niks3_commit_duration_seconds_count 2"} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

// commitOK returns a function that fails the test if a commit fails and drops
// the folded-object count.
func commitOK(t *testing.T) func(int64, error) {
	t.Helper()

	return func(_ int64, err error) {
		t.Helper()
		ok(t, err)
	}
}

// commitErr keeps only the error of a commit.
func commitErr(_ int64, err error) error { return err }
