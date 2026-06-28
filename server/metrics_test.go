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

	rec := httptest.NewRecorder()
	service.Metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	body, err := io.ReadAll(rec.Body)
	ok(t, err)

	for _, want := range []string{
		"niks3_cache_objects 2",
		"niks3_cache_logical_bytes 4096",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}
