package server_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/minio/minio-go/v7"
)

func createTestClosure(t *testing.T, service *server.Service, queries *pg.Queries, hash string) {
	t.Helper()
	ctx := t.Context()

	uploadTestObject := func(key string) {
		_, err := service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
		ok(t, err)
	}

	pendingClosure, err := queries.InsertPendingClosure(ctx, hash+".narinfo")
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pendingClosure.ID, Key: hash + ".narinfo", Refs: []string{"nar/" + hash + ".nar.zst"}},
		{PendingClosureID: pendingClosure.ID, Key: "nar/" + hash + ".nar.zst", Refs: []string{}},
	})
	ok(t, err)

	uploadTestObject(hash + ".narinfo")
	uploadTestObject("nar/" + hash + ".nar.zst")

	err = queries.CommitPendingClosure(ctx, pendingClosure.ID)
	ok(t, err)
}

func createOrphanedObjects(t *testing.T, service *server.Service, objects []struct {
	key  string
	refs []string
},
) {
	t.Helper()
	ctx := t.Context()

	for _, obj := range objects {
		_, err := service.Pool.Exec(ctx, "INSERT INTO objects (key, refs) VALUES ($1, $2)", obj.key, obj.refs)
		ok(t, err)

		_, err = service.MinioClient.PutObject(ctx, service.Bucket, obj.key, nil, 0, minio.PutObjectOptions{})
		ok(t, err)
	}
}

func deleteObjectsFromS3AndDB(t *testing.T, service *server.Service, queries *pg.Queries, objsToDelete []string) {
	t.Helper()
	ctx := t.Context()

	// Delete from S3
	objectsCh := make(chan minio.ObjectInfo, len(objsToDelete))
	for _, obj := range objsToDelete {
		objectsCh <- minio.ObjectInfo{Key: obj}
	}

	close(objectsCh)

	for result := range service.MinioClient.RemoveObjectsWithResult(ctx, service.Bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if result.Err != nil {
			t.Errorf("Failed to delete object %s: %v", result.ObjectName, result.Err)
		}
	}

	// Delete from database
	err := queries.DeleteObjects(ctx, objsToDelete)
	ok(t, err)
}

// TestOrphanedObjectsGC validates that orphaned objects (objects that reference
// each other but are not reachable from any closure) are properly garbage collected.
//
// Test scenario:
// - Create closure A with objects A1, A2
// - Create closure B with objects B1, B2
// - Create orphaned objects X1 -> X2 -> X3 (chain of references not reachable from any closure)
// - Create orphaned single object Y1 (no references, not reachable)
// - Delete closure B
// - Run GC
//
// Expected results:
// - Closure A objects (A1, A2): KEPT (reachable)
// - Closure B objects (B1, B2): DELETED (closure deleted)
// - Orphaned chain (X1, X2, X3): DELETED (not reachable from any closure)
// - Orphaned single (Y1): DELETED (not reachable from any closure).
func TestOrphanedObjectsGC(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	// Generate test hashes
	hashA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hashX1 := "xxxxxxxx1111111111111111111111111"
	hashX2 := "xxxxxxxx2222222222222222222222222"
	hashX3 := "xxxxxxxx3333333333333333333333333"
	hashY := "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"

	// Create Closure B (will be deleted)
	createTestClosure(t, service, queries, hashB)

	// Wait and set cutoff time (B will be deleted, A will be kept)
	time.Sleep(100 * time.Millisecond)
	cutoffTime := time.Now().UTC()
	time.Sleep(100 * time.Millisecond)

	// Create Closure A (should be kept)
	createTestClosure(t, service, queries, hashA)

	// Create orphaned object chain X1 -> X2 -> X3
	createOrphanedObjects(t, service, []struct {
		key  string
		refs []string
	}{
		{hashX1 + ".narinfo", []string{hashX2 + ".narinfo", "nar/" + hashX1 + ".nar.zst"}},
		{"nar/" + hashX1 + ".nar.zst", []string{}},
		{hashX2 + ".narinfo", []string{hashX3 + ".narinfo", "nar/" + hashX2 + ".nar.zst"}},
		{"nar/" + hashX2 + ".nar.zst", []string{}},
		{hashX3 + ".narinfo", []string{"nar/" + hashX3 + ".nar.zst"}},
		{"nar/" + hashX3 + ".nar.zst", []string{}},
	})

	// Create orphaned single object Y
	createOrphanedObjects(t, service, []struct {
		key  string
		refs []string
	}{
		{hashY + ".narinfo", []string{"nar/" + hashY + ".nar.zst"}},
		{"nar/" + hashY + ".nar.zst", []string{}},
	})

	// Delete Closure B
	_, err := queries.DeleteClosures(ctx, pgtype.Timestamp{
		Time:  cutoffTime,
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

	// ===== Run GC (mark stale objects) =====
	_, err = queries.MarkStaleObjects(ctx)
	ok(t, err)

	// Verify marking results
	checkObjectMarkedForDeletion := func(key string) bool {
		var deletedAt pgtype.Timestamp

		err := service.Pool.QueryRow(ctx,
			"SELECT deleted_at FROM objects WHERE key = $1",
			key).Scan(&deletedAt)
		if err != nil {
			return false // Object doesn't exist or query failed
		}

		return deletedAt.Valid
	}

	// Check Closure A objects - should NOT be marked
	if checkObjectMarkedForDeletion(hashA + ".narinfo") {
		t.Error("Closure A narinfo should NOT be marked for deletion")
	}

	if checkObjectMarkedForDeletion("nar/" + hashA + ".nar.zst") {
		t.Error("Closure A NAR should NOT be marked for deletion")
	}

	// Check Closure B objects - should be marked
	if !checkObjectMarkedForDeletion(hashB + ".narinfo") {
		t.Error("Closure B narinfo SHOULD be marked for deletion")
	}

	if !checkObjectMarkedForDeletion("nar/" + hashB + ".nar.zst") {
		t.Error("Closure B NAR SHOULD be marked for deletion")
	}

	// Check orphaned chain X - all should be marked
	orphanedKeys := []string{
		hashX1 + ".narinfo", "nar/" + hashX1 + ".nar.zst",
		hashX2 + ".narinfo", "nar/" + hashX2 + ".nar.zst",
		hashX3 + ".narinfo", "nar/" + hashX3 + ".nar.zst",
	}
	for _, key := range orphanedKeys {
		if !checkObjectMarkedForDeletion(key) {
			t.Errorf("Orphaned object %s SHOULD be marked for deletion", key)
		}
	}

	// Check orphaned single Y - should be marked
	if !checkObjectMarkedForDeletion(hashY + ".narinfo") {
		t.Error("Orphaned Y narinfo SHOULD be marked for deletion")
	}

	if !checkObjectMarkedForDeletion("nar/" + hashY + ".nar.zst") {
		t.Error("Orphaned Y NAR SHOULD be marked for deletion")
	}

	// Actually delete the objects (simulate full GC)
	objsToDelete, err := queries.GetObjectsReadyForDeletion(ctx, pg.GetObjectsReadyForDeletionParams{
		GracePeriodSeconds: 0,
		LimitCount:         1000,
	})
	ok(t, err)

	if len(objsToDelete) == 0 {
		t.Fatal("Expected objects to be ready for deletion")
	}

	deleteObjectsFromS3AndDB(t, service, queries, objsToDelete)

	// Verify final state
	checkObjectExists := func(key string) bool {
		var count int

		err := service.Pool.QueryRow(ctx,
			"SELECT COUNT(*) FROM objects WHERE key = $1",
			key).Scan(&count)

		return err == nil && count > 0
	}

	checkS3ObjectExists := func(key string) bool {
		_, err := service.MinioClient.StatObject(ctx, service.Bucket, key,
			minio.StatObjectOptions{})

		return err == nil
	}

	// Closure A objects should still exist
	if !checkObjectExists(hashA + ".narinfo") {
		t.Error("Closure A narinfo should still exist in DB")
	}

	if !checkS3ObjectExists(hashA + ".narinfo") {
		t.Error("Closure A narinfo should still exist in S3")
	}

	// Closure B objects should be deleted
	if checkObjectExists(hashB + ".narinfo") {
		t.Error("Closure B narinfo should be deleted from DB")
	}

	if checkS3ObjectExists(hashB + ".narinfo") {
		t.Error("Closure B narinfo should be deleted from S3")
	}

	// Orphaned chain should be deleted
	for _, key := range []string{hashX1 + ".narinfo", hashX2 + ".narinfo", hashX3 + ".narinfo"} {
		if checkObjectExists(key) {
			t.Errorf("Orphaned object %s should be deleted from DB", key)
		}

		if checkS3ObjectExists(key) {
			t.Errorf("Orphaned object %s should be deleted from S3", key)
		}
	}

	// Orphaned single should be deleted
	if checkObjectExists(hashY + ".narinfo") {
		t.Error("Orphaned Y narinfo should be deleted from DB")
	}

	if checkS3ObjectExists(hashY + ".narinfo") {
		t.Error("Orphaned Y narinfo should be deleted from S3")
	}

	// Print summary
	t.Logf("GC Test Summary:")
	t.Logf("  - Kept:    %d objects from closure A", 2)
	t.Logf("  - Deleted: %d objects from closure B", 2)
	t.Logf("  - Deleted: %d orphaned chain objects (X1->X2->X3)", 6)
	t.Logf("  - Deleted: %d orphaned single objects (Y)", 2)
	t.Logf("  - Total deleted: %d objects", len(objsToDelete))
}

// TestOrphanedObjectsGCStressTest is a more intensive stress test that creates
// multiple closures and orphaned object graphs to validate GC behavior at scale.
func TestOrphanedObjectsGCStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	const (
		numActiveClosure   = 10
		numDeletedClosures = 5
		numOrphanedChains  = 20
		chainLength        = 5
	)

	uploadTestObject := func(key string) {
		_, err := service.MinioClient.PutObject(ctx, service.Bucket, key,
			nil, 0, minio.PutObjectOptions{})
		ok(t, err)
	}

	generateHash := func(prefix string, id int) string {
		return fmt.Sprintf("%s%030d", prefix, id)
	}

	// Track what should be kept vs deleted
	activeKeys := make(map[string]bool)
	deletedKeys := make(map[string]bool)

	// ===== Create closures that will be deleted =====
	for i := range numDeletedClosures {
		hash := generateHash("deleted", i)
		narKey := "nar/" + hash + ".nar.zst"
		infoKey := hash + ".narinfo"

		pendingClosure, err := queries.InsertPendingClosure(ctx, infoKey)
		ok(t, err)

		_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
			{PendingClosureID: pendingClosure.ID, Key: infoKey, Refs: []string{narKey}},
			{PendingClosureID: pendingClosure.ID, Key: narKey, Refs: []string{}},
		})
		ok(t, err)

		uploadTestObject(infoKey)
		uploadTestObject(narKey)

		err = queries.CommitPendingClosure(ctx, pendingClosure.ID)
		ok(t, err)

		deletedKeys[infoKey] = true
		deletedKeys[narKey] = true
	}

	// Wait and set cutoff time (deleted closures will be removed, active ones will be kept)
	time.Sleep(100 * time.Millisecond)
	cutoffTime := time.Now().UTC()
	time.Sleep(100 * time.Millisecond)

	// ===== Create active closures (should be kept) =====
	for i := range numActiveClosure {
		hash := generateHash("active", i)
		narKey := "nar/" + hash + ".nar.zst"
		infoKey := hash + ".narinfo"

		pendingClosure, err := queries.InsertPendingClosure(ctx, infoKey)
		ok(t, err)

		_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
			{PendingClosureID: pendingClosure.ID, Key: infoKey, Refs: []string{narKey}},
			{PendingClosureID: pendingClosure.ID, Key: narKey, Refs: []string{}},
		})
		ok(t, err)

		uploadTestObject(infoKey)
		uploadTestObject(narKey)

		err = queries.CommitPendingClosure(ctx, pendingClosure.ID)
		ok(t, err)

		activeKeys[infoKey] = true
		activeKeys[narKey] = true
	}

	// ===== Create orphaned object chains =====
	for chainID := range numOrphanedChains {
		for linkID := range chainLength {
			hash := generateHash(fmt.Sprintf("orphan%d_", chainID), linkID)
			narKey := "nar/" + hash + ".nar.zst"
			infoKey := hash + ".narinfo"

			var refs []string

			if linkID < chainLength-1 {
				// Reference next object in chain
				nextHash := generateHash(fmt.Sprintf("orphan%d_", chainID), linkID+1)
				refs = []string{nextHash + ".narinfo", narKey}
			} else {
				refs = []string{narKey}
			}

			_, err := service.Pool.Exec(ctx,
				"INSERT INTO objects (key, refs) VALUES ($1, $2)",
				infoKey, refs)
			ok(t, err)
			uploadTestObject(infoKey)

			_, err = service.Pool.Exec(ctx,
				"INSERT INTO objects (key, refs) VALUES ($1, $2)",
				narKey, []string{})
			ok(t, err)
			uploadTestObject(narKey)

			deletedKeys[infoKey] = true
			deletedKeys[narKey] = true
		}
	}

	t.Logf("Created %d active closures, %d to-delete closures, %d orphaned chains",
		numActiveClosure, numDeletedClosures, numOrphanedChains)

	// ===== Delete the marked closures =====
	_, err := queries.DeleteClosures(ctx, pgtype.Timestamp{
		Time:  cutoffTime,
		Valid: true,
	})
	ok(t, err)

	// ===== Run GC =====
	_, err = queries.MarkStaleObjects(ctx)
	ok(t, err)

	// Get objects ready for deletion
	objsToDelete, err := queries.GetObjectsReadyForDeletion(ctx, pg.GetObjectsReadyForDeletionParams{
		GracePeriodSeconds: 0,
		LimitCount:         10000,
	})
	ok(t, err)

	t.Logf("Marked %d objects for deletion", len(objsToDelete))

	// Delete from S3
	objectsCh := make(chan minio.ObjectInfo, len(objsToDelete))
	for _, obj := range objsToDelete {
		objectsCh <- minio.ObjectInfo{Key: obj}
	}

	close(objectsCh)

	deletionErrors := 0

	for result := range service.MinioClient.RemoveObjectsWithResult(ctx, service.Bucket,
		objectsCh, minio.RemoveObjectsOptions{}) {
		if result.Err != nil {
			t.Errorf("Failed to delete object %s: %v", result.ObjectName, result.Err)

			deletionErrors++
		}
	}

	if deletionErrors > 0 {
		t.Fatalf("Encountered %d deletion errors", deletionErrors)
	}

	// Delete from database
	err = queries.DeleteObjects(ctx, objsToDelete)
	ok(t, err)

	// ===== Verify all active objects still exist =====
	for key := range activeKeys {
		var count int

		err := service.Pool.QueryRow(ctx,
			"SELECT COUNT(*) FROM objects WHERE key = $1",
			key).Scan(&count)
		ok(t, err)

		if count == 0 {
			t.Errorf("Active object %s was incorrectly deleted", key)
		}
	}

	// ===== Verify all deleted/orphaned objects are gone =====
	remainingDeleted := 0

	for key := range deletedKeys {
		var count int

		err := service.Pool.QueryRow(ctx,
			"SELECT COUNT(*) FROM objects WHERE key = $1",
			key).Scan(&count)
		ok(t, err)

		if count > 0 {
			remainingDeleted++
		}
	}

	if remainingDeleted > 0 {
		t.Errorf("%d objects that should be deleted still remain in DB", remainingDeleted)
	}

	t.Logf("Stress test completed successfully:")
	t.Logf("  - Active objects preserved: %d", len(activeKeys))
	t.Logf("  - Objects deleted: %d", len(deletedKeys))
	t.Logf("  - Total GC'd: %d", len(objsToDelete))
}

// TestResurrectedObjectNotDeleted tests the critical bug where objects marked
// as active after S3 deletion failure would still be selected for deletion
// on the next GC run because GetObjectsReadyForDeletion only checked
// first_deleted_at, not deleted_at.
//
// Bug scenario:
// 1. Create closure A with objects
// 2. Delete closure A (objects become orphaned and marked for deletion)
// 3. Delete objects from S3 manually (simulating S3 deletion)
// 4. Resurrect closure A (objects should become active again)
// 5. Mark objects as active (simulating S3 deletion failure recovery)
// 6. Run GC again - objects should NOT be selected for deletion
//
// Without the fix, step 6 would incorrectly return the resurrected objects,
// causing active objects to be deleted.
func TestResurrectedObjectNotDeleted(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	// Step 1: Create a closure with objects
	hash := "testobject111111111111111111111"
	createTestClosure(t, service, queries, hash)

	objectKey := hash + ".narinfo"
	narKey := "nar/" + hash + ".nar.zst"

	// Step 2: Delete the closure (objects become orphaned)
	// Use a cutoff time slightly in the future to ensure deletion
	time.Sleep(10 * time.Millisecond)
	cutoffTime := time.Now().UTC().Add(1 * time.Second)

	deletedCount, err := queries.DeleteClosures(ctx, pgtype.Timestamp{
		Time:  cutoffTime,
		Valid: true,
	})
	ok(t, err)

	if deletedCount < 1 {
		t.Fatalf("Expected at least 1 closure to be deleted, got %d", deletedCount)
	}

	// Step 3: Run GC marking phase - objects should be marked for deletion
	markedCount, err := queries.MarkStaleObjects(ctx)
	ok(t, err)

	if markedCount < 2 {
		t.Fatalf("Expected at least 2 objects to be marked, got %d", markedCount)
	}

	// Verify objects are marked for deletion
	var deletedAt, firstDeletedAt pgtype.Timestamp

	err = service.Pool.QueryRow(ctx,
		"SELECT deleted_at, first_deleted_at FROM objects WHERE key = $1",
		objectKey).Scan(&deletedAt, &firstDeletedAt)
	ok(t, err)

	if !deletedAt.Valid || !firstDeletedAt.Valid {
		t.Fatal("Objects should be marked for deletion after MarkStaleObjects")
	}

	// Step 4 & 5: Simulate S3 deletion failure scenario
	// In real code, this happens when S3 deletion fails in removeS3Objects
	// and MarkObjectsAsActive is called in handleFailedObject
	err = queries.MarkObjectsAsActive(ctx, []string{objectKey, narKey})
	ok(t, err)

	// Verify objects are resurrected (deleted_at = NULL, first_deleted_at still set)
	err = service.Pool.QueryRow(ctx,
		"SELECT deleted_at, first_deleted_at FROM objects WHERE key = $1",
		objectKey).Scan(&deletedAt, &firstDeletedAt)
	ok(t, err)

	if deletedAt.Valid {
		t.Fatal("Object should be resurrected (deleted_at should be NULL)")
	}

	if !firstDeletedAt.Valid {
		t.Fatal("Object should still have first_deleted_at set (this is the bug trigger)")
	}

	// Step 6: Next GC run - GetObjectsReadyForDeletion should NOT return resurrected objects
	objsToDelete, err := queries.GetObjectsReadyForDeletion(ctx, pg.GetObjectsReadyForDeletionParams{
		GracePeriodSeconds: 0,
		LimitCount:         1000,
	})
	ok(t, err)

	// Check if the resurrected objects were incorrectly selected for deletion
	for _, key := range objsToDelete {
		if key == objectKey || key == narKey {
			t.Fatalf("BUG: Resurrected object %q was incorrectly selected for deletion! "+
				"This would cause active objects to be deleted from S3.", key)
		}
	}
}
