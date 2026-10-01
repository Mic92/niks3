package server_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
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
	ok(t, pg.New(service.Pool).CommitPendingClosure(ctx, id))

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
