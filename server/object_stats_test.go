package server_test

import (
	"testing"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestObjectStatsTrigger verifies the object_stats running totals stay correct
// across every mutation path: commit (insert), tombstone, resurrect and delete.
func TestObjectStatsTrigger(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	assertStats := func(wantCount, wantBytes int64) {
		t.Helper()

		stats, err := queries.GetObjectStats(ctx)
		ok(t, err)

		if stats.ObjectCount != wantCount || stats.TotalBytes != wantBytes {
			t.Fatalf("stats = (count=%d, bytes=%d), want (count=%d, bytes=%d)",
				stats.ObjectCount, stats.TotalBytes, wantCount, wantBytes)
		}
	}

	assertStats(0, 0)

	hash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	narKey := "nar/" + hash + ".nar.zst"
	narinfoKey := hash + ".narinfo"

	pendingClosure, err := queries.InsertPendingClosure(ctx, narinfoKey)
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pendingClosure.ID, Key: narinfoKey, Refs: []string{narKey}}, // NULL size
		{PendingClosureID: pendingClosure.ID, Key: narKey, Refs: []string{}, Size: pgtype.Int8{Int64: 1000, Valid: true}},
	})
	ok(t, err)

	err = queries.CommitPendingClosure(ctx, pendingClosure.ID)
	ok(t, err)

	assertStats(2, 1000)

	now := pgtype.Timestamp{Time: time.Now().UTC(), Valid: true}
	_, err = service.Pool.Exec(ctx,
		"UPDATE objects SET deleted_at = $1 WHERE key = $2", now, narKey)
	ok(t, err)

	assertStats(1, 0) // tombstone

	_, err = service.Pool.Exec(ctx,
		"UPDATE objects SET deleted_at = NULL WHERE key = $1", narKey)
	ok(t, err)

	assertStats(2, 1000) // resurrect

	_, err = service.Pool.Exec(ctx, "DELETE FROM objects WHERE key = $1", narKey)
	ok(t, err)

	assertStats(1, 0) // delete
}
