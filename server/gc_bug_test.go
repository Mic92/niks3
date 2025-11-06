package server_test

import (
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestGCBugBareHashReferences verifies that the GC bug fix works correctly.
// Previously, references were stored as bare hashes, causing GC to incorrectly
// delete reachable objects. Now they're stored as object keys (hash.narinfo).
func TestGCBugBareHashReferences(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	hashA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	// Create closure B
	pendingClosureB, err := queries.InsertPendingClosure(ctx, hashB+".narinfo")
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pendingClosureB.ID, Key: hashB + ".narinfo", Refs: []string{"nar/" + hashB + ".nar.zst"}},
		{PendingClosureID: pendingClosureB.ID, Key: "nar/" + hashB + ".nar.zst", Refs: []string{}},
	})
	ok(t, err)

	err = queries.CommitPendingClosure(ctx, pendingClosureB.ID)
	ok(t, err)

	// Create closure A that references B using proper object key
	pendingClosureA, err := queries.InsertPendingClosure(ctx, hashA+".narinfo")
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		// Fixed: References B as object key "hashB.narinfo" (not bare hash)
		{PendingClosureID: pendingClosureA.ID, Key: hashA + ".narinfo", Refs: []string{hashB + ".narinfo", "nar/" + hashA + ".nar.zst"}},
		{PendingClosureID: pendingClosureA.ID, Key: "nar/" + hashA + ".nar.zst", Refs: []string{}},
	})
	ok(t, err)

	err = queries.CommitPendingClosure(ctx, pendingClosureA.ID)
	ok(t, err)

	// Update closure A to have a newer timestamp
	timeAfterB := time.Now().UTC()

	time.Sleep(100 * time.Millisecond)

	pendingClosureA2, err := queries.InsertPendingClosure(ctx, hashA+".narinfo")
	ok(t, err)
	err = queries.CommitPendingClosure(ctx, pendingClosureA2.ID)
	ok(t, err)

	time.Sleep(100 * time.Millisecond)

	// Delete old closures (should delete B but keep A)
	_, err = queries.DeleteClosures(ctx, pgtype.Timestamp{
		Time:  timeAfterB.Add(50 * time.Millisecond),
		Valid: true,
	})
	ok(t, err)

	// Verify B is deleted but A remains
	_, err = queries.GetClosure(ctx, hashB+".narinfo")
	if err == nil {
		t.Fatal("Closure B should have been deleted")
	}

	_, err = queries.GetClosure(ctx, hashA+".narinfo")
	ok(t, err)

	// Run GC (mark stale objects)
	_, err = queries.MarkStaleObjects(ctx)
	ok(t, err)

	// Check what objects were marked for deletion
	rows, err := service.Pool.Query(ctx, "SELECT key FROM objects WHERE deleted_at IS NOT NULL")
	ok(t, err)

	defer rows.Close()

	var markedForDeletion []string

	for rows.Next() {
		var key string

		err = rows.Scan(&key)
		ok(t, err)

		markedForDeletion = append(markedForDeletion, key)
	}

	ok(t, rows.Err())

	// Check if bug occurred
	bugOccurred := false

	for _, key := range markedForDeletion {
		if key == hashB+".narinfo" || key == "nar/"+hashB+".nar.zst" {
			bugOccurred = true

			break
		}
	}

	if bugOccurred {
		t.Errorf("REGRESSION: GC incorrectly marked B's objects for deletion even though A references them")
		t.Errorf("Objects marked for deletion:")

		for _, key := range markedForDeletion {
			if strings.Contains(key, hashB) {
				t.Errorf("  - %s", key)
			}
		}
	}
}
