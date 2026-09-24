package server_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/pg"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// postPendingClosureJSON drives CreatePendingClosureHandler with a raw body.
func postPendingClosureJSON(t *testing.T, service *server.Service, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pending_closures", strings.NewReader(body))
	w := httptest.NewRecorder()
	service.CreatePendingClosureHandler(w, req)

	return w
}

// closureBody builds a two-object closure request: a narinfo referencing one NAR.
func closureBody(hash, narKey string) string {
	return `{"closure":"` + hash + `.narinfo","objects":[` +
		`{"key":"` + hash + `.narinfo","type":"narinfo","refs":["` + narKey + `"]},` +
		`{"key":"` + narKey + `","type":"nar","refs":[],"nar_size":1}]}`
}

func objectIsLive(t *testing.T, service *server.Service, key string) bool {
	t.Helper()

	var live bool
	ok(t, service.Pool.QueryRow(t.Context(), "SELECT deleted_at IS NULL FROM objects WHERE key=$1", key).Scan(&live))

	return live
}

func objectInS3(t *testing.T, service *server.Service, key string) bool {
	t.Helper()

	_, err := service.MinioClient.StatObject(t.Context(), service.Bucket, key, minio.StatObjectOptions{})

	return err == nil
}

// A push that deduplicates against objects of an older closure must keep
// those objects alive even if GC removes the older closure before the push
// commits.
func TestPushDedupSurvivesConcurrentGC(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	q := pg.New(service.Pool)

	hashOld := strings.Repeat("a", 32)
	hashNew := strings.Repeat("c", 32)
	oldNar := "nar/" + strings.Repeat("a", 52) + ".nar.zst"

	// Older committed closure: hashOld.narinfo -> oldNar, both in S3.
	oldClosure, err := q.InsertPendingClosure(ctx, hashOld+".narinfo")
	ok(t, err)
	_, err = q.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: oldClosure.ID, Key: hashOld + ".narinfo", Refs: []string{oldNar}},
		{PendingClosureID: oldClosure.ID, Key: oldNar, Refs: []string{}},
	})
	ok(t, err)

	for _, key := range []string{hashOld + ".narinfo", oldNar} {
		_, err = service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
		ok(t, err)
	}

	ok(t, q.CommitPendingClosure(ctx, oldClosure.ID))

	// New push lists its whole closure, as the client does; the server
	// deduplicates hashOld's objects, so only the new ones are offered.
	objects := `{"closure":"` + hashNew + `.narinfo","objects":[` +
		`{"key":"` + hashNew + `.narinfo","type":"narinfo","refs":["` + hashOld + `.narinfo","nar/` + strings.Repeat("c", 52) + `.nar.zst"]},` +
		`{"key":"nar/` + strings.Repeat("c", 52) + `.nar.zst","type":"nar","refs":[],"nar_size":1},` +
		`{"key":"` + hashOld + `.narinfo","type":"narinfo","refs":["` + oldNar + `"]},` +
		`{"key":"` + oldNar + `","type":"nar","refs":[],"nar_size":1}]}`

	w := postPendingClosureJSON(t, service, objects)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp server.PendingClosureResponse
	ok(t, json.Unmarshal(w.Body.Bytes(), &resp))

	if _, offered := resp.PendingObjects[hashOld+".narinfo"]; offered {
		t.Errorf("present object %s.narinfo was offered for upload", hashOld)
	}

	for key := range resp.PendingObjects {
		_, err := service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
		ok(t, err)
	}

	time.Sleep(50 * time.Millisecond)

	// GC while the push is in flight: the old closure has aged out.
	st := service.RunGCForTest(0, 24*time.Hour, false)
	if st.State != "succeeded" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	var id int64
	ok(t, service.Pool.QueryRow(ctx, "SELECT id FROM pending_closures WHERE key=$1", hashNew+".narinfo").Scan(&id))
	ok(t, q.CommitPendingClosure(ctx, id))

	if !objectIsLive(t, service, oldNar) {
		t.Errorf("%s is tombstoned although committed closure %s.narinfo references it", oldNar, hashNew)
	}

	// Grace period elapses; the new closure is fresh and must be kept.
	st = service.RunGCForTest(24*time.Hour, 24*time.Hour, true)
	if st.State != "succeeded" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	if !objectInS3(t, service, oldNar) {
		t.Errorf("%s deleted from S3 although committed closure %s.narinfo references it", oldNar, hashNew)
	}
}

// Deduplicated objects get a pending_objects row so GC protects them, but are
// not offered to the client.
func TestDeduplicatedObjectsRecordedAsPending(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	hash := strings.Repeat("k", 32)
	narKey := "nar/" + strings.Repeat("l", 52) + ".nar.zst"

	_, err := service.Pool.Exec(ctx, "INSERT INTO objects (key, refs) VALUES ($1, '{}')", narKey)
	ok(t, err)
	seededWithoutS3(t, narKey)

	w := postPendingClosureJSON(t, service, closureBody(hash, narKey))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp server.PendingClosureResponse
	ok(t, json.Unmarshal(w.Body.Bytes(), &resp))

	if _, offered := resp.PendingObjects[narKey]; offered {
		t.Errorf("present object %s was offered for upload", narKey)
	}

	if _, offered := resp.PendingObjects[hash+".narinfo"]; !offered {
		t.Errorf("missing object %s.narinfo was not offered for upload", hash)
	}

	var pending int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM pending_objects WHERE key=$1", narKey).Scan(&pending))

	if pending != 1 {
		t.Errorf("expected 1 pending_objects row for deduplicated %s, got %d", narKey, pending)
	}
}

// The sweep must not delete an object an in-flight closure has pending, even
// after the grace period, and the commit must leave it live in both places.
func TestGCSweepSkipsPendingObjects(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	q := pg.New(service.Pool)

	hash := strings.Repeat("i", 32)
	narKey := "nar/" + strings.Repeat("j", 52) + ".nar.zst"

	// Orphan tombstoned an hour ago, still within the grace window.
	_, err := service.Pool.Exec(ctx, `INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
		VALUES ($1, '{}', timezone('UTC', now()) - interval '1 hour', timezone('UTC', now()) - interval '1 hour')`, narKey)
	ok(t, err)

	w := postPendingClosureJSON(t, service, closureBody(hash, narKey))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp server.PendingClosureResponse
	ok(t, json.Unmarshal(w.Body.Bytes(), &resp))

	if _, offered := resp.PendingObjects[narKey]; !offered {
		t.Fatalf("tombstoned object %s must be offered for upload", narKey)
	}

	// The client uploads what it was offered; the NAR's async registration
	// has not landed yet.
	for key := range resp.PendingObjects {
		_, err = service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
		ok(t, err)
	}

	st := service.RunGCForTest(24*time.Hour, 24*time.Hour, true)
	if st.State != "succeeded" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	if !objectInS3(t, service, narKey) {
		t.Errorf("sweep deleted %s from S3 while a pending closure had it pending", narKey)
	}

	var id int64
	ok(t, service.Pool.QueryRow(ctx, "SELECT id FROM pending_closures WHERE key=$1", hash+".narinfo").Scan(&id))
	ok(t, q.CommitPendingClosure(ctx, id))

	if !objectIsLive(t, service, narKey) {
		t.Errorf("%s not live after commit", narKey)
	}
}

// A tombstoned object is offered for re-upload immediately; the request must
// not fail when GC removes the tombstone row while the request is running.
func TestTombstonedObjectOfferedWithoutWaiting(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	hash := strings.Repeat("g", 32)
	narKey := "nar/" + strings.Repeat("h", 52) + ".nar.zst"

	_, err := service.Pool.Exec(ctx, `INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
		VALUES ($1, '{}', timezone('UTC', now()), timezone('UTC', now()))`, narKey)
	ok(t, err)

	go func() {
		time.Sleep(500 * time.Millisecond)
		_, _ = service.Pool.Exec(ctx, "DELETE FROM objects WHERE key=$1", narKey)
	}()

	start := time.Now()
	w := postPendingClosureJSON(t, service, closureBody(hash, narKey))

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, strings.TrimSpace(w.Body.String()))
	}

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("request blocked for %s waiting on a tombstone", elapsed)
	}

	var resp server.PendingClosureResponse
	ok(t, json.Unmarshal(w.Body.Bytes(), &resp))

	if _, offered := resp.PendingObjects[narKey]; !offered {
		t.Errorf("tombstoned object %s was not offered for upload", narKey)
	}
}

// Each stale object is handed to the S3 deleter exactly once.
func TestGCSweepDeliversEachKeyOnce(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	createOrphanedObjects(t, service, []struct {
		key  string
		refs []string
	}{
		{key: "nar/" + strings.Repeat("a", 52) + ".nar.zst", refs: []string{}},
		{key: "nar/" + strings.Repeat("b", 52) + ".nar.zst", refs: []string{}},
		{key: "nar/" + strings.Repeat("c", 52) + ".nar.zst", refs: []string{}},
	})

	st := service.RunGCForTest(24*time.Hour, 24*time.Hour, true)
	if st.State != "succeeded" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	if st.Stats.ObjectsDeletedAfterGracePeriod != 3 {
		t.Errorf("3 orphan objects, GC reported %d deletions", st.Stats.ObjectsDeletedAfterGracePeriod)
	}
}

// A failed S3 verification must roll back the transaction and release the
// pool connection.
func TestCreatePendingClosureVerifyS3FailureReleasesConnection(t *testing.T) {
	t.Parallel()

	service := createTestService(t)

	defer func() {
		done := make(chan struct{})

		go func() {
			service.Close()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("Pool.Close() hangs: a connection with an open transaction was never released")
		}
	}()

	ctx := t.Context()

	hash := strings.Repeat("a", 32)
	narKey := "nar/" + strings.Repeat("b", 52) + ".nar.zst"

	_, err := service.Pool.Exec(ctx, "INSERT INTO objects (key, refs) VALUES ($1, '{}')", narKey)
	ok(t, err)
	seededWithoutS3(t, narKey)

	// Point S3 verification at a closed port.
	bad, err := minio.New("127.0.0.1:1", &minio.Options{Creds: credentials.NewStaticV4("a", "b", ""), MaxRetries: 1})
	ok(t, err)

	service.MinioClient = bad

	before := service.Pool.Stat().AcquiredConns()

	w := postPendingClosureJSON(t, service, `{"closure":"`+hash+`.narinfo","verify_s3":true,"objects":[`+
		`{"key":"`+hash+`.narinfo","type":"narinfo","refs":["`+narKey+`"]},`+
		`{"key":"`+narKey+`","type":"nar","refs":[],"nar_size":1}]}`)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}

	time.Sleep(200 * time.Millisecond)

	if after := service.Pool.Stat().AcquiredConns(); after > before {
		t.Errorf("%d pool connection(s) still acquired after the failed request", after-before)
	}

	// The client got no response, so it will never commit this closure: its
	// rows must not linger and shield objects from GC until cleanup.
	var pending int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM pending_closures").Scan(&pending))

	if pending != 0 {
		t.Errorf("%d pending closure(s) left behind by the failed request", pending)
	}
}

// A push that finds an object present must have its pending row in place
// before it decides so. Otherwise a GC run between the check and the row can
// tombstone and, in force mode, sweep the object, and the closure then
// commits it as live with nothing behind it in S3.
//
// GC runs at two points of the push. Before the pending rows are written it
// sweeps the NAR, which must then be offered. After the push has decided the
// NAR is present it must find the NAR shielded by its pending row. Only the
// second point tells the two orders apart: with the check ahead of the rows,
// GC before both steps still leaves the check seeing the sweep.
func TestForceGCDuringPushOffersSweptObject(t *testing.T) {
	t.Parallel()

	for _, point := range []struct {
		name string
		set  func(*server.Service, func())
	}{
		{"before pending rows", (*server.Service).SetTestHookBeforePendingInsert},
		{"after presence check", (*server.Service).SetTestHookAfterPresenceCheck},
	} {
		t.Run(point.name, func(t *testing.T) {
			t.Parallel()

			service := createTestService(t)
			defer service.Close()

			ctx := t.Context()

			hash := strings.Repeat("f", 32)
			narKey := "nar/" + strings.Repeat("g", 52) + ".nar.zst"

			// The NAR is live in the database and in S3 but reachable from no
			// closure (its closure was collected).
			_, err := service.Pool.Exec(ctx, "INSERT INTO objects (key, refs) VALUES ($1, '{}')", narKey)
			ok(t, err)

			_, err = service.MinioClient.PutObject(ctx, service.Bucket, narKey, nil, 0, minio.PutObjectOptions{})
			ok(t, err)

			gcRuns := 0

			point.set(service, func() {
				gcRuns++

				st := service.RunGCForTest(0, 24*time.Hour, true)
				if st.State != "succeeded" {
					t.Errorf("GC failed: %s", st.Error)
				}
			})

			w := postPendingClosureJSON(t, service, closureBody(hash, narKey))
			if w.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}

			if gcRuns != 1 {
				t.Fatalf("GC hook ran %d times, want 1", gcRuns)
			}

			var resp server.PendingClosureResponse
			ok(t, json.Unmarshal(w.Body.Bytes(), &resp))

			// Either the row survived the GC run, in which case the object must
			// still be in S3, or the object was swept and must be offered.
			if _, offered := resp.PendingObjects[narKey]; !offered {
				if !objectInS3(t, service, narKey) {
					t.Fatalf("%s was swept from S3 but reported present", narKey)
				}
			}

			for key := range resp.PendingObjects {
				_, err := service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
				ok(t, err)
			}

			id, err := strconv.ParseInt(resp.ID, 10, 64)
			ok(t, err)
			ok(t, pg.New(service.Pool).CommitPendingClosure(ctx, id))

			if !objectIsLive(t, service, narKey) {
				t.Errorf("%s not live after commit", narKey)
			}

			if !objectInS3(t, service, narKey) {
				t.Errorf("%s live in the database but missing from S3", narKey)
			}
		})
	}
}

// A row a push resurrected while the sweep was deleting the object from S3
// must survive the sweep's row delete: the push re-uploaded the object after
// the delete, so the row is right and the object is present again.
func TestSweepRowDeleteSparesResurrectedObject(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	q := pg.New(service.Pool)

	swept := "nar/" + strings.Repeat("a", 52) + ".nar.zst"
	resurrected := "nar/" + strings.Repeat("b", 52) + ".nar.zst"

	_, err := service.Pool.Exec(ctx,
		`INSERT INTO objects (key, deleted_at, first_deleted_at)
		 SELECT unnest($1::varchar[]), now() - interval '2 days', now() - interval '2 days'`,
		[]string{swept, resurrected})
	ok(t, err)

	// The sweep selected both keys and deleted them from S3; meanwhile a
	// push re-uploaded and registered the second one.
	ok(t, q.RegisterCompletedObject(ctx, pg.RegisterCompletedObjectParams{Key: resurrected, Refs: []string{}}))
	seededWithoutS3(t, resurrected)

	ok(t, q.DeleteTombstonedObjects(ctx, []string{swept, resurrected}))

	var n int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM objects WHERE key = $1", swept).Scan(&n))

	if n != 0 {
		t.Errorf("tombstoned row %s survived the sweep", swept)
	}

	if !objectIsLive(t, service, resurrected) {
		t.Errorf("resurrected row %s was deleted by the sweep", resurrected)
	}
}

// pushOneClosure runs a push of closureBody(hash, narKey) as the client does:
// create the pending closure, upload and register what it is offered, commit.
// It reports failures as an error so it can run on its own goroutine.
func pushOneClosure(t *testing.T, service *server.Service, hash, narKey string) error {
	t.Helper()

	ctx := t.Context()

	w := postPendingClosureJSON(t, service, closureBody(hash, narKey))
	if w.Code != http.StatusOK {
		return fmt.Errorf("create pending closure: %d %s", w.Code, w.Body.String())
	}

	var resp server.PendingClosureResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		return fmt.Errorf("decode pending closure: %w", err)
	}

	q := pg.New(service.Pool)

	for key := range resp.PendingObjects {
		if _, err := service.MinioClient.PutObject(ctx, service.Bucket, key, strings.NewReader("new"), 3, minio.PutObjectOptions{}); err != nil {
			return fmt.Errorf("upload %s: %w", key, err)
		}

		if err := q.RegisterCompletedObject(ctx, pg.RegisterCompletedObjectParams{Key: key, Refs: []string{}}); err != nil {
			return fmt.Errorf("register %s: %w", key, err)
		}
	}

	id, err := strconv.ParseInt(resp.ID, 10, 64)
	if err != nil {
		return fmt.Errorf("parse closure id: %w", err)
	}

	if err := q.CommitPendingClosure(ctx, id); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// A push that re-uploads a tombstoned object between the sweep selecting it
// and deleting it from S3 must not lose the upload: either the sweep spares
// the key, or the push is told to upload only once the delete is done.
func TestSweepSparesObjectReuploadedMidSweep(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	hash := strings.Repeat("p", 32)
	narKey := "nar/" + strings.Repeat("q", 52) + ".nar.zst"

	// An orphan tombstoned two days ago, past the grace period, still in S3.
	_, err := service.Pool.Exec(ctx, `INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
		VALUES ($1, '{}', timezone('UTC', now()) - interval '2 days', timezone('UTC', now()) - interval '2 days')`, narKey)
	ok(t, err)

	_, err = service.MinioClient.PutObject(ctx, service.Bucket, narKey, strings.NewReader("old"), 3, minio.PutObjectOptions{})
	ok(t, err)

	pushed := make(chan error, 1)
	hookRuns := 0

	// The push runs while the sweep is between selecting the page and
	// deleting it from S3. It is given a second to finish; a push that is
	// held back until the sweep is done is let run to its end afterwards.
	service.SetTestHookBeforeSweepDelete(func() {
		hookRuns++
		if hookRuns > 1 {
			return
		}

		go func() { pushed <- pushOneClosure(t, service, hash, narKey) }()

		select {
		case err := <-pushed:
			pushed <- err
		case <-time.After(time.Second):
		}
	})

	st := service.RunGCForTest(24*time.Hour, 24*time.Hour, false)
	if st.State != "succeeded" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	if hookRuns == 0 {
		t.Fatal("the sweep never selected the tombstoned object")
	}

	ok(t, <-pushed)

	if !objectIsLive(t, service, narKey) {
		t.Errorf("%s not live after its closure committed", narKey)
	}

	if !objectInS3(t, service, narKey) {
		t.Errorf("%s live in the database but deleted from S3 by the sweep", narKey)
	}
}

// waitForLockWaiter returns once a backend of the service's database is
// waiting for a lock.
func waitForLockWaiter(t *testing.T, service *server.Service) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for {
		var waiting int
		ok(t, service.Pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting))

		if waiting > 0 {
			return
		}

		if time.Now().After(deadline) {
			t.Fatal("no backend started waiting for a lock")
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// A commit that GC's pending cleanup races must either fail or leave every
// object of the closure recorded, live and in S3. Here the commit waits on
// the closure row, held by a concurrent commit or present check of the same
// root, while a collection whose cleanup takes the aged pending closure runs.
func TestCommitRacingPendingCleanupKeepsObjects(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	q := pg.New(service.Pool)

	hash := strings.Repeat("z", 32)
	narinfo := hash + ".narinfo"
	narKey := "nar/" + strings.Repeat("9", 52) + ".nar.zst"

	// The root is a closure already, as on a push with --verify-s3-integrity.
	_, err := service.Pool.Exec(ctx, "INSERT INTO closures (key, updated_at) VALUES ($1, timezone('UTC', now()))", narinfo)
	ok(t, err)

	// A push that uploaded everything and aged past the cleanup cutoff
	// before committing.
	pc, err := q.InsertPendingClosure(ctx, narinfo)
	ok(t, err)

	_, err = q.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pc.ID, Key: narinfo, Refs: []string{narKey}},
		{PendingClosureID: pc.ID, Key: narKey, Refs: []string{}},
	})
	ok(t, err)

	for _, key := range []string{narinfo, narKey} {
		_, err = service.MinioClient.PutObject(ctx, service.Bucket, key, strings.NewReader("x"), 1, minio.PutObjectOptions{})
		ok(t, err)
	}

	_, err = service.Pool.Exec(ctx, "UPDATE pending_closures SET started_at = started_at - interval '2 days' WHERE id = $1", pc.ID)
	ok(t, err)

	holder, err := service.Pool.Begin(ctx)
	ok(t, err)

	defer func() { _ = holder.Rollback(ctx) }()

	_, err = holder.Exec(ctx, "SELECT 1 FROM closures WHERE key = $1 FOR UPDATE", narinfo)
	ok(t, err)

	committed := make(chan error, 1)

	go func() { committed <- q.CommitPendingClosure(ctx, pc.ID) }()

	waitForLockWaiter(t, service)

	st := service.RunGCForTest(24*time.Hour, 24*time.Hour, true)
	if st.State != "succeeded" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	ok(t, holder.Rollback(ctx))

	if err := <-committed; err != nil {
		t.Logf("commit failed, as it may: %v", err)

		return
	}

	// The client is told its closure is cached, so all of it must be.
	for _, key := range []string{narinfo, narKey} {
		var live int
		ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM objects WHERE key = $1 AND deleted_at IS NULL", key).Scan(&live))

		if live != 1 {
			t.Errorf("commit succeeded but %s is not recorded live", key)
		}

		if !objectInS3(t, service, key) {
			t.Errorf("commit succeeded but %s is gone from S3", key)
		}
	}
}
