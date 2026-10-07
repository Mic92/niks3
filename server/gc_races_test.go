package server_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
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

	commitOK(t)(q.CommitPendingClosure(ctx, oldClosure.ID))

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
	commitOK(t)(q.CommitPendingClosure(ctx, id))

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

	// The client uploads the NAR; its async registration has not landed yet.
	_, err = service.MinioClient.PutObject(ctx, service.Bucket, narKey, nil, 0, minio.PutObjectOptions{})
	ok(t, err)

	st := service.RunGCForTest(24*time.Hour, 24*time.Hour, true)
	if st.State != "succeeded" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	if !objectInS3(t, service, narKey) {
		t.Errorf("sweep deleted %s from S3 while a pending closure had it pending", narKey)
	}

	var id int64
	ok(t, service.Pool.QueryRow(ctx, "SELECT id FROM pending_closures WHERE key=$1", hash+".narinfo").Scan(&id))
	commitOK(t)(q.CommitPendingClosure(ctx, id))

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
// GC runs before the pending rows exist, and after the push decided what is
// present. Only the second point detects a check ordered before the rows.
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

			// Live in the database and S3, but no closure reaches it.
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

			// Either the object survived, or it was swept and must be offered.
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
			commitOK(t)(pg.New(service.Pool).CommitPendingClosure(ctx, id))

			if !objectIsLive(t, service, narKey) {
				t.Errorf("%s not live after commit", narKey)
			}

			if !objectInS3(t, service, narKey) {
				t.Errorf("%s live in the database but missing from S3", narKey)
			}
		})
	}
}

// A failed S3 delete must leave the tombstone: reviving the row would leave a
// live object behind that S3 may already have lost.
func TestFailedS3DeleteKeepsTombstone(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	hash := strings.Repeat("d", 32)
	narKey := "nar/" + strings.Repeat("d", 52) + ".nar.zst"

	w := postPendingClosureJSON(t, service, closureBody(hash, narKey))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	for _, key := range []string{hash + ".narinfo", narKey} {
		_, err := service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
		ok(t, err)
	}

	var id int64
	ok(t, service.Pool.QueryRow(ctx, "SELECT id FROM pending_closures WHERE key=$1", hash+".narinfo").Scan(&id))
	commitOK(t)(pg.New(service.Pool).CommitPendingClosure(ctx, id))

	target, err := url.Parse(fmt.Sprintf("http://localhost:%d", testRustfsServer.port))
	ok(t, err)

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
			http.Error(w, "<Error><Code>AccessDenied</Code></Error>", http.StatusForbidden)

			return
		}

		httputil.NewSingleHostReverseProxy(target).ServeHTTP(w, r)
	}))
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	ok(t, err)

	service.MinioClient = testRustfsServer.ClientWithEndpoint(t, proxyURL.Host)

	time.Sleep(50 * time.Millisecond)

	st := service.RunGCForTest(0, 24*time.Hour, true)
	if st.Stats.ObjectsFailedToDelete == 0 {
		t.Fatalf("S3 delete did not fail: %+v", st.Stats)
	}

	if objectIsLive(t, service, narKey) {
		t.Errorf("%s is live again after its S3 delete failed", narKey)
	}
}

// The sweep must not delete a tombstoned object a running push transaction
// has locked, because that push is about to treat it as present.
func TestSweepSkipsRowsLockedByPush(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	key := "nar/" + strings.Repeat("e", 52) + ".nar.zst"

	_, err := service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
	ok(t, err)

	_, err = service.Pool.Exec(ctx, `INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
		VALUES ($1, '{}', timezone('UTC', now()) - interval '1 hour', timezone('UTC', now()) - interval '1 hour')`, key)
	ok(t, err)

	tx, err := service.Pool.Begin(ctx)
	ok(t, err)

	defer func() { _ = tx.Rollback(ctx) }()

	var locked string
	ok(t, tx.QueryRow(ctx, "SELECT key FROM objects WHERE key = $1 FOR KEY SHARE", key).Scan(&locked))

	done := make(chan api.GCTaskStatus, 1)

	go func() { done <- service.RunGCForTest(24*time.Hour, 24*time.Hour, true) }()

	select {
	case st := <-done:
		if st.State != "succeeded" {
			t.Fatalf("GC failed: %s", st.Error)
		}
	case <-time.After(5 * time.Second):
		ok(t, tx.Commit(ctx))
		<-done
		t.Fatal("the sweep waits for a row a push holds instead of skipping it")
	}

	if !objectInS3(t, service, key) {
		t.Fatalf("%s deleted from S3 while a push held its row", key)
	}

	ok(t, tx.Commit(ctx))

	if st := service.RunGCForTest(24*time.Hour, 24*time.Hour, true); st.State != "succeeded" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	if objectInS3(t, service, key) {
		t.Errorf("%s survived the sweep after the push released it", key)
	}
}

// Registering an upload needs a pending row: it is what keeps the sweep away
// from the object, so without one the row would outlive its object.
func TestRegisterCompletedObjectNeedsPendingRow(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	key := "nar/" + strings.Repeat("f", 52) + ".nar.zst"

	ok(t, pg.New(service.Pool).RegisterCompletedObject(ctx, pg.RegisterCompletedObjectParams{
		Key: key, Refs: []string{},
	}))

	var n int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM objects WHERE key = $1", key).Scan(&n))

	if n != 0 {
		t.Errorf("registered %s without a pending row", key)
	}
}

// Looking up existing objects in a push transaction locks their rows against the sweep.
func TestGetExistingObjectsLocksRows(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	key := "nar/" + strings.Repeat("g", 52) + ".nar.zst"

	_, err := service.Pool.Exec(ctx, `INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
		VALUES ($1, '{}', timezone('UTC', now()) - interval '1 hour', timezone('UTC', now()) - interval '1 hour')`, key)
	ok(t, err)

	push, err := service.Pool.Begin(ctx)
	ok(t, err)

	defer func() { _ = push.Rollback(ctx) }()

	_, err = pg.New(push).GetExistingObjects(ctx, []string{key})
	ok(t, err)

	sweep, err := service.Pool.Begin(ctx)
	ok(t, err)

	defer func() { _ = sweep.Rollback(ctx) }()

	keys, err := pg.New(sweep).LockObjectsReadyForDeletion(ctx, pg.LockObjectsReadyForDeletionParams{LimitCount: 10})
	ok(t, err)

	if len(keys) != 0 {
		t.Errorf("sweep picked %v while a push transaction held the rows", keys)
	}
}

// A push whose pending rows commit after the sweep selected a key must keep
// the object: the sweep rechecks once it holds the row.
func TestSweepRechecksPendingAfterLocking(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	key := "nar/" + strings.Repeat("h", 52) + ".nar.zst"

	_, err := service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
	ok(t, err)

	_, err = service.Pool.Exec(ctx, `INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
		VALUES ($1, '{}', timezone('UTC', now()) - interval '1 hour', timezone('UTC', now()) - interval '1 hour')`, key)
	ok(t, err)

	service.SetTestHookAfterSweepSelect(func() {
		q := pg.New(service.Pool)

		pc, err := q.InsertPendingClosure(ctx, "late.narinfo")
		ok(t, err)

		_, err = q.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
			{PendingClosureID: pc.ID, Key: key, Refs: []string{}},
		})
		ok(t, err)
	})

	if st := service.RunGCForTest(0, 24*time.Hour, true); st.Error != "" {
		t.Fatalf("GC failed: %s", st.Error)
	}

	if _, err := service.MinioClient.StatObject(ctx, service.Bucket, key, minio.StatObjectOptions{}); err != nil {
		t.Errorf("sweep deleted %s although a push had it pending: %v", key, err)
	}
}

// A push that re-offers a tombstoned object waits for a sweep holding its row,
// so its upload cannot land before the sweep's S3 delete.
func TestPushWaitsForSweepHoldingRow(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	hash := strings.Repeat("k", 32)
	narKey := "nar/" + strings.Repeat("k", 52) + ".nar.zst"

	_, err := service.Pool.Exec(ctx, `INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
		VALUES ($1, '{}', timezone('UTC', now()) - interval '1 hour', timezone('UTC', now()) - interval '1 hour')`, narKey)
	ok(t, err)

	sweep, err := service.Pool.Begin(ctx)
	ok(t, err)

	defer func() { _ = sweep.Rollback(ctx) }()

	_, err = sweep.Exec(ctx, "SELECT key FROM objects WHERE key = $1 FOR UPDATE", narKey)
	ok(t, err)

	done := make(chan int, 1)

	go func() { done <- postPendingClosureJSON(t, service, closureBody(hash, narKey)).Code }()

	select {
	case code := <-done:
		t.Fatalf("push answered %d while the sweep held the row", code)
	case <-time.After(500 * time.Millisecond):
	}

	ok(t, sweep.Commit(ctx))

	select {
	case code := <-done:
		if code != http.StatusOK {
			t.Errorf("status=%d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("push still blocked after the sweep committed")
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

// A commit racing GC's pending cleanup must either fail or leave every object
// of the closure live and in S3.
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

	go func() { committed <- commitErr(q.CommitPendingClosure(ctx, pc.ID)) }()

	waitForLockWaiter(t, service)

	gcDone := make(chan api.GCTaskStatus, 1)

	go func() { gcDone <- service.RunGCForTest(24*time.Hour, 24*time.Hour, true) }()

	select {
	case st := <-gcDone:
		if st.State != "succeeded" {
			t.Fatalf("GC failed: %s", st.Error)
		}
	case <-time.After(10 * time.Second):
		ok(t, holder.Rollback(ctx))
		t.Fatal("the cleanup waits for a closure being committed instead of skipping it")
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
