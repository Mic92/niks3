package server_test

import (
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/minio/minio-go/v7"
)

func commitClosure(t *testing.T, service *server.Service, narinfoKey, narKey string) {
	t.Helper()
	ctx := t.Context()
	queries := pg.New(service.Pool)

	pc, err := queries.InsertPendingClosure(ctx, narinfoKey)
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pc.ID, Key: narinfoKey, Refs: []string{narKey}},
		{PendingClosureID: pc.ID, Key: narKey, Refs: []string{}},
	})
	ok(t, err)

	for _, key := range []string{narinfoKey, narKey} {
		_, err = service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
		ok(t, err)
	}

	ok(t, queries.CommitPendingClosure(ctx, pc.ID))
}

func tombstoneEverything(t *testing.T, queries *pg.Queries) {
	t.Helper()

	_, err := queries.DeleteClosures(t.Context(), pgtype.Timestamp{Time: time.Now().UTC().Add(time.Minute), Valid: true})
	ok(t, err)

	_, err = queries.MarkStaleObjects(t.Context())
	ok(t, err)
}

// A push that arrives while its objects are tombstoned must be offered upload
// URLs for them, must keep the reaper away while pending, and must end with
// live rows after commit.
func TestPushOfTombstonedObjects(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	hash := "gcgcgcgcgcgcgcgcgcgcgcgcgcgcgc01"
	narinfoKey := hash + ".narinfo"
	narKey := narKeyFor(hash)

	commitClosure(t, service, narinfoKey, narKey)
	tombstoneEverything(t, queries)

	resp := createPendingClosure(t, service, map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 1024},
		},
	})

	if _, found := resp.PendingObjects[narinfoKey]; !found {
		t.Errorf("tombstoned narinfo not offered for upload")
	}

	if _, found := resp.PendingObjects[narKey]; !found {
		t.Errorf("tombstoned nar not offered for upload")
	}

	stats, err := service.CleanupOrphanObjectsForTest(ctx, 0)
	ok(t, err)

	if stats.DeletedCount != 0 {
		t.Fatalf("reaper deleted %d objects that belong to a pending closure", stats.DeletedCount)
	}

	_, err = service.MinioClient.StatObject(ctx, service.Bucket, narKey, minio.StatObjectOptions{})
	ok(t, err)

	commitPendingClosure(t, service, resp.ID, map[string]map[string]any{
		narinfoKey: {
			"store_path":  "/nix/store/" + hash + "-test",
			"url":         narKey,
			"compression": "zstd",
			"nar_hash":    "sha256:0000000000000000000000000000000000000000000000000000",
			"nar_size":    1,
			"file_hash":   "sha256:0000000000000000000000000000000000000000000000000000",
			"file_size":   1,
			"references":  []string{},
		},
	})

	live, err := queries.GetLiveObjects(ctx, []string{narinfoKey, narKey})
	ok(t, err)

	if len(live) != 2 {
		t.Fatalf("expected both objects live after commit, got %v", live)
	}
}

// Already-live objects are not re-uploaded but are still registered as
// pending, so GC expiring their last referrer mid-push cannot reap them.
func TestLiveObjectsProtectedWhilePending(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	dep := "gcgcgcgcgcgcgcgcgcgcgcgcgcgcgc02"
	depNarinfo := dep + ".narinfo"
	depNar := narKeyFor(dep)

	commitClosure(t, service, depNarinfo, depNar)

	top := "gcgcgcgcgcgcgcgcgcgcgcgcgcgcgc03"
	topNarinfo := top + ".narinfo"
	topNar := narKeyFor(top)

	resp := createPendingClosure(t, service, map[string]any{
		"closure": topNarinfo,
		"objects": []map[string]any{
			{"key": topNarinfo, "type": "narinfo", "refs": []string{topNar, depNarinfo}},
			{"key": topNar, "type": "nar", "refs": []string{}, "nar_size": 1024},
			{"key": depNarinfo, "type": "narinfo", "refs": []string{depNar}},
			{"key": depNar, "type": "nar", "refs": []string{}, "nar_size": 1024},
		},
	})

	if _, found := resp.PendingObjects[depNar]; found {
		t.Errorf("live dependency offered for upload")
	}

	// dep's only committed referrer expires while top is still uploading.
	tombstoneEverything(t, queries)

	stats, err := service.CleanupOrphanObjectsForTest(ctx, 0)
	ok(t, err)

	if stats.MarkedCount != 0 || stats.DeletedCount != 0 {
		t.Fatalf("GC touched objects of a pending closure: %+v", stats)
	}

	_, err = service.MinioClient.StatObject(ctx, service.Bucket, depNar, minio.StatObjectOptions{})
	ok(t, err)
}

func TestReapCountsEachObjectOnce(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	for _, h := range []string{"gcgcgcgcgcgcgcgcgcgcgcgcgcgcgc10", "gcgcgcgcgcgcgcgcgcgcgcgcgcgcgc11"} {
		createTestClosure(t, service, queries, h)
	}

	tombstoneEverything(t, queries)

	stats, err := service.CleanupOrphanObjectsForTest(ctx, 0)
	ok(t, err)

	if stats.DeletedCount != 4 {
		t.Fatalf("DeletedCount = %d, want 4", stats.DeletedCount)
	}

	var remaining int

	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM objects WHERE key LIKE 'gcgcgcgcgcgcgcgcgcgcgcgcgcgcgc1%' OR key LIKE 'nar/gcgcgcgcgcgcgcgcgcgcgcgcgcgcgc1%'").Scan(&remaining))

	if remaining != 0 {
		t.Fatalf("%d rows left after reap", remaining)
	}
}
