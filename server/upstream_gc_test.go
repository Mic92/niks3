package server_test

import (
	"testing"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestGCMissingUpstreamReference(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	rootKey := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo"
	narKey := "nar/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.nar.zst"
	upstreamKey := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.narinfo"

	pending, err := queries.InsertPendingClosure(ctx, rootKey)
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pending.ID, Key: rootKey, Refs: []string{narKey, upstreamKey}},
		{PendingClosureID: pending.ID, Key: narKey, Refs: []string{}},
	})
	ok(t, err)
	ok(t, queries.CommitPendingClosure(ctx, pending.ID))

	// A reference to an object served only by an upstream cache stops the
	// recursive join, while the local root and its NAR remain reachable.
	_, err = queries.MarkStaleObjects(ctx)
	ok(t, err)

	var marked int

	err = service.Pool.QueryRow(ctx,
		"SELECT count(*) FROM objects WHERE deleted_at IS NOT NULL").Scan(&marked)
	ok(t, err)

	if marked != 0 {
		t.Fatalf("marked objects = %d, want none while closure is active", marked)
	}

	objects, err := queries.GetClosureObjects(ctx, rootKey)
	ok(t, err)

	if len(objects) != 2 {
		t.Fatalf("reachable objects = %v, want only local root and NAR", objects)
	}

	_, err = queries.DeleteClosures(ctx, pgtype.Timestamp{
		Time:  time.Now().UTC().Add(time.Hour),
		Valid: true,
	})
	ok(t, err)

	_, err = queries.MarkStaleObjects(ctx)
	ok(t, err)

	err = service.Pool.QueryRow(ctx,
		"SELECT count(*) FROM objects WHERE deleted_at IS NOT NULL").Scan(&marked)
	ok(t, err)

	if marked != 2 {
		t.Fatalf("marked objects = %d, want both local objects after closure deletion", marked)
	}
}
