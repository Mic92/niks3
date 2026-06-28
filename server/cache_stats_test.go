package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestCacheStatsHandler(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	hash := "dddddddddddddddddddddddddddddddd"
	narKey := "nar/" + hash + ".nar.zst"

	pendingClosure, err := queries.InsertPendingClosure(ctx, hash+".narinfo")
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pendingClosure.ID, Key: hash + ".narinfo", Refs: []string{narKey}},
		{PendingClosureID: pendingClosure.ID, Key: narKey, Refs: []string{}, Size: pgtype.Int8{Int64: 2048, Valid: true}},
	})
	ok(t, err)
	ok(t, queries.CommitPendingClosure(ctx, pendingClosure.ID))

	rec := httptest.NewRecorder()
	service.CacheStatsHandler(rec, httptest.NewRequest(http.MethodGet, "/api/cache-stats", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("CORS header = %q, want *", got)
	}

	var stats api.CacheStats
	ok(t, json.NewDecoder(rec.Body).Decode(&stats))

	if stats.Objects != 2 || stats.LogicalBytes != 2048 {
		t.Errorf("stats = %+v, want {Objects:2 LogicalBytes:2048}", stats)
	}
}
