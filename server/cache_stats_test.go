package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/internal/testwait"
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
	commitOK(t)(queries.CommitPendingClosure(ctx, pendingClosure.ID))
	ok(t, queries.RecountObjectStats(ctx))

	rec := httptest.NewRecorder()
	service.CacheStatsHandler(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/cache-stats", nil))

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

// The count leaves out deleted objects and is kept for a while.
func TestObjectsAreCountedOnceInAWhile(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	for _, insert := range []string{
		`INSERT INTO objects (key, refs, deleted_at) VALUES ('live', '{}', NULL), ('deleted', '{}', now())`,
		`INSERT INTO objects (key, refs) VALUES ('later', '{}')`,
	} {
		_, err := service.Pool.Exec(ctx, insert)
		ok(t, err)
		ok(t, queries.RecountObjectStats(ctx))

		stats, err := queries.GetObjectStats(ctx)
		ok(t, err)

		if stats.ObjectCount != 1 {
			t.Fatalf("counted %d objects, want the first live one", stats.ObjectCount)
		}
	}
}

// A server leaves the count to the one that is at it.
func TestObjectsAreCountedByOneServer(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx, cancel := context.WithTimeout(t.Context(), testwait.Hung)
	defer cancel()

	other, err := service.Pool.Begin(ctx)
	ok(t, err)

	defer func() { _ = other.Rollback(ctx) }()

	_, err = other.Exec(ctx, `INSERT INTO objects (key, refs) VALUES ('live', '{}')`)
	ok(t, err)
	ok(t, pg.New(other).RecountObjectStats(ctx))
	ok(t, pg.New(service.Pool).RecountObjectStats(ctx))
	ok(t, other.Rollback(ctx))

	stats, err := pg.New(service.Pool).GetObjectStats(ctx)
	ok(t, err)

	if stats.ObjectCount != 0 {
		t.Fatalf("a second server counted %d objects meanwhile", stats.ObjectCount)
	}
}
