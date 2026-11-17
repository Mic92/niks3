package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/pg"
	"github.com/Mic92/niks3/server/signing"
	"github.com/minio/minio-go/v7"
)

// A build is one worker's result for a derivation: the same narinfo keys as
// any other build of it, with NARs of its own.
type build struct {
	outputs []string // store path hashes
	nars    []string
}

func newBuild(drv, worker int, outputs int) build {
	var bld build

	for out := range outputs {
		bld.outputs = append(bld.outputs, fmt.Sprintf("%030d%02d", drv, out))
		bld.nars = append(bld.nars, fmt.Sprintf("nar/%048d%02d%02d.nar.zst", drv, worker, out))
	}

	return bld
}

func (bld build) keys() []string {
	keys := make([]string, 0, len(bld.outputs))
	for _, hash := range bld.outputs {
		keys = append(keys, hash+".narinfo")
	}

	return keys
}

func (bld build) push(t *testing.T, service *server.Service) string {
	t.Helper()

	objects := make([]map[string]any, 0, 2*len(bld.outputs))
	for out, hash := range bld.outputs {
		objects = append(objects,
			map[string]any{"key": hash + ".narinfo", "type": "narinfo", "refs": []string{bld.nars[out]}},
			map[string]any{"key": bld.nars[out], "type": "nar", "refs": []string{}})
	}

	return createPush(t, service, bld.keys(), objects).ID
}

// sign returns the NAR each narinfo key is or is to be published with.
func (bld build) sign(t *testing.T, service *server.Service, id string) map[string]string {
	t.Helper()

	return bld.signAs(t, service, id, true)
}

func (bld build) signAs(t *testing.T, service *server.Service, id string, claim bool) map[string]string {
	t.Helper()

	narinfos := map[string]any{}
	for out, hash := range bld.outputs {
		narinfos[hash+".narinfo"] = map[string]any{
			"store_path":  "/nix/store/" + hash + "-test",
			"url":         bld.nars[out],
			"compression": "zstd",
			"nar_hash":    "sha256:" + strings.Repeat("0", 52),
			"nar_size":    1000,
			"references":  []string{},
		}
	}

	body, err := json.Marshal(map[string]any{"narinfos": narinfos, "claim": claim})
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:     "POST",
		path:       "/api/pushes/" + id + "/sign",
		body:       body,
		handler:    service.SignNarinfosHandler,
		pathValues: map[string]string{"id": id},
	})

	var signed struct {
		Claimed   map[string]server.NarinfoMetadata `json:"claimed"`
		Published map[string]string                 `json:"published"`
	}

	ok(t, json.Unmarshal(rr.Body.Bytes(), &signed))

	published := map[string]string{}
	for out, hash := range bld.outputs {
		published[hash+".narinfo"] = bld.nars[out]
	}

	for key, narinfo := range signed.Claimed {
		if len(narinfo.Signatures) != 1 {
			t.Errorf("claimed narinfo %s has signatures %v, want one", key, narinfo.Signatures)
		}

		published[key] = narinfo.URL
	}

	maps.Copy(published, signed.Published)

	return published
}

func claimService(t *testing.T) *server.Service {
	t.Helper()

	service := createTestService(t)

	key, err := signing.ParseKey(testSigningSecret)
	ok(t, err)

	service.SigningKeys = []signing.Signer{key}

	return service
}

func collectGarbage(t *testing.T, service *server.Service) {
	t.Helper()

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/closures?older-than=1000h&failed-uploads-older-than=0s&force=true",
		handler: service.CleanupClosuresOlder,
	})

	if status := waitForGC(t, service); status.State != api.GCTaskStateSucceeded {
		t.Fatalf("garbage collection ended as %s", status.State)
	}
}

// Two workers built the same derivation with different results. The cache
// publishes the first one, and the second push ends up referring to it.
func TestClaim_SecondBuildPublishesTheFirst(t *testing.T) {
	t.Parallel()

	service := claimService(t)
	defer service.Close()

	first, second := newBuild(1, 1, 2), newBuild(1, 2, 2)
	firstID, secondID := first.push(t, service), second.push(t, service)

	won := first.sign(t, service, firstID)
	for out, key := range first.keys() {
		if won[key] != first.nars[out] {
			t.Fatalf("first build was told to publish %v", won)
		}
	}

	lost := second.sign(t, service, secondID)
	for out, key := range first.keys() {
		if lost[key] != first.nars[out] {
			t.Fatalf("second build was told to publish %v, want the first build's %v", lost, first.nars)
		}
	}

	// A client from before claims would not know what to do with one.
	former := newBuild(1, 3, 2)
	if told := former.signAs(t, service, former.push(t, service), false); told[former.keys()[0]] != former.nars[0] {
		t.Fatalf("a client that did not ask to claim was told to publish %v", told)
	}

	again := first.sign(t, service, firstID)
	if again[first.keys()[0]] != first.nars[0] {
		t.Fatalf("repeated sign of the first build was told to publish %v", again)
	}

	// The first push never commits. Its NARs must outlive it.
	completePush(t, service, secondID)
	collectGarbage(t, service)

	for _, nar := range first.nars {
		if n := countRows(t, service, `SELECT count(*) FROM objects WHERE key = $1 AND deleted_at IS NULL`, nar); n != 1 {
			t.Errorf("published NAR %s was collected", nar)
		}
	}

	for _, nar := range second.nars {
		if n := countRows(t, service, `SELECT count(*) FROM objects WHERE key = $1`, nar); n != 0 {
			t.Errorf("NAR %s of the losing build was kept", nar)
		}
	}
}

// Whatever the order, all workers publish the outputs of one build.
func TestClaim_RacingBuildsAgree(t *testing.T) {
	t.Parallel()

	service := claimService(t)
	defer service.Close()

	const workers = 8

	for drv := range 10 {
		var (
			wg        sync.WaitGroup
			published [workers]map[string]string
		)

		for wrk := range workers {
			bld := newBuild(drv, wrk, 3)
			id := bld.push(t, service)

			wg.Go(func() { published[wrk] = bld.sign(t, service, id) })
		}

		wg.Wait()

		from := -1

		for wrk := range workers {
			if published[wrk][newBuild(drv, wrk, 3).keys()[0]] == newBuild(drv, wrk, 3).nars[0] {
				from = wrk
			}
		}

		if from < 0 {
			t.Fatalf("drv %d: nobody won: %v", drv, published)
		}

		want := newBuild(drv, from, 3)
		for wrk := range workers {
			for out, key := range want.keys() {
				if published[wrk][key] != want.nars[out] {
					t.Fatalf("drv %d: worker %d publishes %v, worker %d won with %v", drv, wrk, published[wrk], from, want.nars)
				}
			}
		}
	}
}

// A build whose push died is collected. The next build takes its place, and
// the narinfo the dead build had published is gone before that.
func TestClaim_CollectedBuildIsReplaced(t *testing.T) {
	t.Parallel()

	service := claimService(t)
	defer service.Close()

	ctx := t.Context()

	dead, next := newBuild(1, 1, 2), newBuild(1, 2, 2)

	dead.sign(t, service, dead.push(t, service))

	stale := dead.keys()[0]
	_, err := service.MinioClient.PutObject(ctx, service.Bucket, stale,
		bytes.NewReader([]byte("dead")), 4, minio.PutObjectOptions{})
	ok(t, err)

	// Marks the dead build's objects. They stay in the bucket for now.
	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	published := next.sign(t, service, next.push(t, service))
	for out, key := range next.keys() {
		if published[key] != next.nars[out] {
			t.Fatalf("next build was told to publish %v, but that build is collected", published)
		}
	}

	_, err = service.MinioClient.StatObject(ctx, service.Bucket, stale, minio.StatObjectOptions{})
	if err == nil || minio.ToErrorResponse(err).Code != minio.NoSuchKey {
		t.Fatalf("narinfo of the collected build is still served (err=%v)", err)
	}
}

// Once a push commits, the committed objects say which build the cache
// serves and the claim is gone. A push that signs later publishes nothing.
func TestClaim_CommittedBuildIsPublished(t *testing.T) {
	t.Parallel()

	service := claimService(t)
	defer service.Close()

	first, second := newBuild(1, 1, 2), newBuild(1, 2, 2)
	firstID, secondID := first.push(t, service), second.push(t, service)

	first.sign(t, service, firstID)
	completePush(t, service, firstID)

	if n := countRows(t, service, `SELECT count(*) FROM publish_claims`); n != 0 {
		t.Fatalf("%d claims left after the commit", n)
	}

	told := second.sign(t, service, secondID)
	for out, key := range first.keys() {
		if told[key] != first.nars[out] {
			t.Fatalf("second build was told %v is published, want the first build's %v", told, first.nars)
		}
	}

	if n := countRows(t, service, `SELECT count(*) FROM publish_claims`); n != 0 {
		t.Fatalf("%d claims on committed narinfos", n)
	}

	completePush(t, service, secondID)

	for out, key := range first.keys() {
		if n := countRows(t, service, `SELECT count(*) FROM objects WHERE key = $1 AND refs = $2`, key, first.nars[out:out+1]); n != 1 {
			t.Errorf("narinfo %s no longer refers to the first build's NAR alone", key)
		}
	}
}

// GC marked a committed build, and its narinfo is still in the bucket. A new
// build of the path wins, and the old narinfo is gone before it is told so.
func TestClaim_MarkedBuildIsDeletedFirst(t *testing.T) {
	t.Parallel()

	service := claimService(t)
	defer service.Close()

	ctx := t.Context()

	old, next := newBuild(1, 1, 2), newBuild(1, 2, 2)

	oldID := old.push(t, service)
	old.sign(t, service, oldID)

	for _, key := range old.keys() {
		_, err := service.MinioClient.PutObject(ctx, service.Bucket, key,
			bytes.NewReader([]byte("old")), 3, minio.PutObjectOptions{})
		ok(t, err)
	}

	completePush(t, service, oldID)

	_, err := service.Pool.Exec(ctx, `UPDATE objects SET deleted_at = timezone('UTC', now()), first_deleted_at = timezone('UTC', now())`)
	ok(t, err)

	told := next.sign(t, service, next.push(t, service))
	for out, key := range next.keys() {
		if told[key] != next.nars[out] {
			t.Fatalf("next build was told to publish %v", told)
		}

		_, err := service.MinioClient.StatObject(ctx, service.Bucket, key, minio.StatObjectOptions{})
		if err == nil || minio.ToErrorResponse(err).Code != minio.NoSuchKey {
			t.Fatalf("narinfo %s of the marked build is still served (err=%v)", key, err)
		}
	}
}

// A push that the cleanup removed cannot claim.
func TestClaim_RemovedPushClaimsNothing(t *testing.T) {
	t.Parallel()

	service := claimService(t)
	defer service.Close()

	bld := newBuild(1, 1, 1)

	_, err := pg.New(service.Pool).ClaimNarinfos(t.Context(), pg.ClaimNarinfosParams{
		PushID: 4711, Keys: bld.keys(), NarKeys: bld.nars, Narinfos: [][]byte{[]byte("{}")},
	})
	if err == nil || !strings.Contains(err.Error(), "Push does not exist") {
		t.Fatalf("claim of a removed push: err=%v", err)
	}
}

// A claim is deleted with its narinfo.
func TestClaim_CollectedWithItsObjects(t *testing.T) {
	t.Parallel()

	service := claimService(t)
	defer service.Close()

	bld := newBuild(1, 1, 1)
	bld.sign(t, service, bld.push(t, service))

	collectGarbage(t, service)

	if n := countRows(t, service, `SELECT count(*) FROM publish_claims`); n != 0 {
		t.Fatalf("%d claims left after their objects were collected", n)
	}
}
