package server_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
	"github.com/minio/minio-go/v7"
)

// deleteObservingProxy sits between the service and RustFS. It records every
// key named in a DeleteObjects request and every single DeleteObject. With
// failMultiDelete set it forwards each DeleteObjects request and then answers
// with a bucket level InternalError, which is what Fastly Object Storage does
// when any key in the batch is missing: the keys that existed are deleted and
// the response names none of them.
type deleteObservingProxy struct {
	proxy           *httputil.ReverseProxy
	failMultiDelete bool

	mu         sync.Mutex
	batchKeys  []string
	singleKeys []string
}

type deleteObjectsRequest struct {
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

func newDeleteObservingProxy(tb testing.TB, failMultiDelete bool) *deleteObservingProxy {
	tb.Helper()

	target, err := url.Parse(fmt.Sprintf("http://localhost:%d", testRustfsServer.port))
	ok(tb, err)

	return &deleteObservingProxy{
		proxy:           httputil.NewSingleHostReverseProxy(target),
		failMultiDelete: failMultiDelete,
	}
}

func (p *deleteObservingProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
		p.serveMultiDelete(w, r)

		return
	case r.Method == http.MethodDelete:
		p.mu.Lock()
		p.singleKeys = append(p.singleKeys, r.URL.Path)
		p.mu.Unlock()
	}

	p.proxy.ServeHTTP(w, r)
}

func (p *deleteObservingProxy) serveMultiDelete(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	var req deleteObjectsRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	p.mu.Lock()
	for _, obj := range req.Objects {
		p.batchKeys = append(p.batchKeys, obj.Key)
	}
	p.mu.Unlock()

	r.Body = io.NopCloser(bytes.NewReader(body))

	if !p.failMultiDelete {
		p.proxy.ServeHTTP(w, r)

		return
	}

	// delete the keys that exist, then hide the outcome behind a bucket level error
	p.proxy.ServeHTTP(httptest.NewRecorder(), r)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>InternalError</Code><Message>Internal error.</Message><Resource>/bucket</Resource></Error>`))
}

// seedExpiredOrphans uploads count objects and inserts them as unreachable
// rows whose grace period has already passed.
func seedExpiredOrphans(tb testing.TB, service *server.Service, prefix string, count int) []string {
	tb.Helper()

	ctx := tb.Context()
	keys := make([]string, 0, count)

	for i := range count {
		key := fmt.Sprintf("%s%030d.narinfo", prefix, i)

		_, err := service.MinioClient.PutObject(ctx, service.Bucket, key, strings.NewReader("x"), 1, minio.PutObjectOptions{})
		ok(tb, err)

		_, err = service.Pool.Exec(ctx,
			`INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
			 VALUES ($1, '{}', timezone('UTC', now()) - interval '1 hour', timezone('UTC', now()) - interval '1 hour')`,
			key)
		ok(tb, err)

		keys = append(keys, key)
	}

	return keys
}

func assertGCDeletedAll(tb testing.TB, status api.GCTaskStatus, count int) {
	tb.Helper()

	if status.State != api.GCTaskStateSucceeded {
		tb.Fatalf("gc ended in state %s: %s", status.State, status.Error)
	}

	if status.Stats.ObjectsDeletedAfterGracePeriod != count || status.Stats.ObjectsFailedToDelete != 0 {
		tb.Fatalf("expected %d deleted and 0 failed, got %d deleted and %d failed",
			count, status.Stats.ObjectsDeletedAfterGracePeriod, status.Stats.ObjectsFailedToDelete)
	}
}

func assertObjectsGone(tb testing.TB, service *server.Service, keys []string) {
	tb.Helper()

	ctx := tb.Context()

	var remaining int

	err := service.Pool.QueryRow(ctx, "SELECT count(*) FROM objects WHERE key = ANY($1)", keys).Scan(&remaining)
	ok(tb, err)

	if remaining != 0 {
		tb.Fatalf("%d of %d rows remain in the database", remaining, len(keys))
	}

	for _, key := range []string{keys[0], keys[len(keys)-1]} {
		_, err := service.MinioClient.StatObject(ctx, service.Bucket, key, minio.StatObjectOptions{})
		if minio.ToErrorResponse(err).Code != minio.NoSuchKey {
			tb.Fatalf("object %s is still present or failed differently: %v", key, err)
		}
	}
}

// TestOrphanedObjectsGCDeletesEachKeyOnce checks that the producer never hands
// the same key to S3 twice. Fetching the next page with a plain LIMIT while the
// previous batch had not been flushed to the database returned the previous
// page again and sent every key several times.
func TestOrphanedObjectsGCDeletesEachKeyOnce(t *testing.T) {
	t.Parallel()

	proxy := newDeleteObservingProxy(t, false)
	proxyServer := httptest.NewServer(proxy)

	defer proxyServer.Close()

	service := createTestServiceWithThrottlingProxy(t, proxyServer.URL)
	defer service.Close()

	const count = 2*server.DeletionBatchSize + 500

	keys := seedExpiredOrphans(t, service, "once", count)

	status := service.RunGCForTest(24*time.Hour, 0, false)
	assertGCDeletedAll(t, status, count)

	proxy.mu.Lock()
	seen := slices.Clone(proxy.batchKeys)
	proxy.mu.Unlock()

	slices.Sort(seen)

	if unique := slices.Compact(slices.Clone(seen)); len(unique) != len(seen) {
		t.Fatalf("%d keys were sent to S3 more than once", len(seen)-len(unique))
	}

	if len(seen) != count {
		t.Fatalf("expected %d keys in DeleteObjects requests, saw %d", count, len(seen))
	}

	assertObjectsGone(t, service, keys)
}

// TestOrphanedObjectsGCFallsBackToSingleDeletes checks that a bucket level
// error on DeleteObjects, which minio reports for every key in the batch, is
// settled per key with DeleteObject instead of re-activating rows for objects
// that S3 has already removed.
func TestOrphanedObjectsGCFallsBackToSingleDeletes(t *testing.T) {
	t.Parallel()

	proxy := newDeleteObservingProxy(t, true)
	proxyServer := httptest.NewServer(proxy)

	defer proxyServer.Close()

	service := createTestServiceWithThrottlingProxy(t, proxyServer.URL)
	defer service.Close()

	const count = server.DeletionBatchSize + 500

	keys := seedExpiredOrphans(t, service, "fallback", count)

	status := service.RunGCForTest(24*time.Hour, 0, false)
	assertGCDeletedAll(t, status, count)

	proxy.mu.Lock()
	singles := len(proxy.singleKeys)
	proxy.mu.Unlock()

	if singles != count {
		t.Fatalf("expected %d single deletes after the batch errors, saw %d", count, singles)
	}

	assertObjectsGone(t, service, keys)
}
