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

// Committing n objects must not take time quadratic in n.
func TestObjectStatsLargeCommit(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	const objects = 60000

	var id int64

	ok(t, service.Pool.QueryRow(ctx, `INSERT INTO pending_closures (key, started_at)
		VALUES (repeat('0', 32) || '.narinfo', now()) RETURNING id`).Scan(&id))

	_, err := service.Pool.Exec(ctx, `INSERT INTO pending_objects (pending_closure_id, key, refs, size)
		SELECT $1, lpad(i::text, 32, '0') || '.narinfo', '{}', 100 FROM generate_series(1, $2::int) AS i`, id, objects)
	ok(t, err)

	start := time.Now()

	_, err = service.Pool.Exec(ctx, "SELECT commit_pending_closure($1)", id)
	ok(t, err)

	if took := time.Since(start); took > 8*time.Second {
		t.Errorf("committing %d objects took %v", objects, took)
	}

	stats, err := pg.New(service.Pool).GetObjectStats(ctx)
	ok(t, err)

	if stats.ObjectCount != objects || stats.TotalBytes != objects*100 {
		t.Errorf("stats = (count=%d, bytes=%d), want (count=%d, bytes=%d)",
			stats.ObjectCount, stats.TotalBytes, objects, objects*100)
	}
}
