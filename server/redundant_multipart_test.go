package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/minio/minio-go/v7"
)

// TestRedundantMultipartUpload drives two pending closures that share the same
// NAR through the multipart path. Completing one upload must abort the other's
// in-flight upload.
func TestRedundantMultipartUpload(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	service := createTestService(t)
	defer service.Close()

	// Unique nix-base32 hashes (no e/o/t/u) to avoid collisions under -parallel.
	sharedNarKey := narKeyFor("gggggggggggggggggggggggggggggg01")
	firstNarinfo := "gggggggggggggggggggggggggggggg10.narinfo"
	secondNarinfo := "gggggggggggggggggggggggggggggg20.narinfo"

	postSharedClosure := func(narinfoKey string) server.PendingObject {
		t.Helper()

		resp := createPendingClosure(t, service, map[string]any{
			"closure": narinfoKey,
			"objects": []map[string]any{
				{"key": narinfoKey, "type": "narinfo", "refs": []string{sharedNarKey}},
				{"key": sharedNarKey, "type": "nar", "refs": []string{}, "nar_size": 100 * 1024 * 1024},
			},
		})

		nar := resp.PendingObjects[sharedNarKey]
		if nar.MultipartInfo == nil || nar.MultipartInfo.UploadID == "" {
			t.Fatalf("expected multipart upload for shared NAR")
		}

		return nar
	}

	coreClient := minio.Core{Client: service.MinioClient}

	winner := postSharedClosure(firstNarinfo)
	loser := postSharedClosure(secondNarinfo)
	loserUploadID := loser.MultipartInfo.UploadID

	// Loser's upload is live until the winner completes.
	_, err := coreClient.ListObjectParts(ctx, service.Bucket, sharedNarKey, loserUploadID, 0, 10)
	ok(t, err)

	// Completing the winner registers the NAR and aborts the loser's upload.
	handleMultipartUpload(ctx, t, sharedNarKey, winner, service)

	if _, err := coreClient.ListObjectParts(ctx, service.Bucket, sharedNarKey, loserUploadID, 0, 10); err == nil {
		t.Error("expected loser's multipart upload to be aborted, but it is still live")
	}

	// When the abort itself fails, the loser's tracking row must stay: it is
	// the only handle on the upload, and the pending-closure cleanup aborts it
	// once the closure ages out.
	otherNarKey := narKeyFor("gggggggggggggggggggggggggggggg02")
	thirdNarinfo := "gggggggggggggggggggggggggggggg40.narinfo"
	fourthNarinfo := "gggggggggggggggggggggggggggggg50.narinfo"

	postClosure := func(narinfoKey string) server.PendingObject {
		t.Helper()

		resp := createPendingClosure(t, service, map[string]any{
			"closure": narinfoKey,
			"objects": []map[string]any{
				{"key": narinfoKey, "type": "narinfo", "refs": []string{otherNarKey}},
				{"key": otherNarKey, "type": "nar", "refs": []string{}, "nar_size": 100 * 1024 * 1024},
			},
		})

		return resp.PendingObjects[otherNarKey]
	}

	winner2 := postClosure(thirdNarinfo)
	straggler := postClosure(fourthNarinfo)
	stragglerUploadID := straggler.MultipartInfo.UploadID

	good := service.MinioClient
	service.SetTestHookBeforeRedundantAbort(func() { service.MinioClient = brokenS3Client(t) })

	handleMultipartUpload(ctx, t, otherNarKey, winner2, service)

	service.MinioClient = good
	service.SetTestHookBeforeRedundantAbort(nil)

	var rows int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM multipart_uploads WHERE upload_id = $1", stragglerUploadID).Scan(&rows))

	if rows != 1 {
		t.Fatalf("straggler's row dropped although its abort failed (rows=%d)", rows)
	}

	_, err = coreClient.ListObjectParts(ctx, service.Bucket, otherNarKey, stragglerUploadID, 0, 10)
	ok(t, err) // still live

	// The kept row lets the cleanup finish the job.
	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	if _, err := coreClient.ListObjectParts(ctx, service.Bucket, otherNarKey, stragglerUploadID, 0, 10); err == nil {
		t.Error("expected the cleanup to abort the straggler's upload, but it is still live")
	}
}

// TestCompleteMultipartUpload_ErrorButObjectExists verifies that a failing
// CompleteMultipartUpload is treated as success when the object is already
// present in S3, covering lost responses and retried completions that return
// NoSuchUpload, but only once the errored upload is closed: while its abort
// fails the request fails too and the upload keeps its row.
func TestCompleteMultipartUpload_ErrorButObjectExists(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	service := createTestService(t)
	defer service.Close()

	narKey := narKeyFor("gggggggggggggggggggggggggggggg03")
	narinfoKey := "gggggggggggggggggggggggggggggg30.narinfo"

	resp := createPendingClosure(t, service, map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 100 * 1024 * 1024},
		},
	})

	uploadID := resp.PendingObjects[narKey].MultipartInfo.UploadID

	// The blob already landed in S3 (e.g. a prior completion whose response was
	// lost), but the multipart upload row is still registered.
	_, err := service.MinioClient.PutObject(ctx, service.Bucket, narKey,
		strings.NewReader("payload"), -1, minio.PutObjectOptions{})
	ok(t, err)

	// Completing with a bogus part fails in S3, yet the object exists, so the
	// handler must report success once it has closed the upload.
	completeBody, err := json.Marshal(map[string]any{
		"object_key": narKey,
		"upload_id":  uploadID,
		"parts":      []map[string]any{{"part_number": 1, "etag": "deadbeef"}},
	})
	ok(t, err)

	coreClient := minio.Core{Client: service.MinioClient}

	// While S3 refuses the abort the upload stays open, so its row is the
	// only handle on it: the request must fail, keeping the row, rather than
	// let the client commit the closure and cascade the row away.
	good := service.MinioClient
	service.MinioClient = interceptedS3Client(t, func(r *http.Request, next http.RoundTripper) (*http.Response, error) {
		if r.Method == http.MethodDelete && r.URL.Query().Has("uploadId") {
			return nil, errors.New("injected: abort refused")
		}

		return next.RoundTrip(r)
	})

	unavailable := checkStatusCode(http.StatusServiceUnavailable)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/multipart/complete",
		body:          completeBody,
		handler:       service.CompleteMultipartUploadHandler,
		checkResponse: &unavailable,
	})

	service.MinioClient = good

	var kept int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM multipart_uploads WHERE upload_id = $1", uploadID).Scan(&kept))

	if kept != 1 {
		t.Fatalf("multipart upload row dropped although its abort failed (rows=%d)", kept)
	}

	if _, err := coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10); err != nil {
		t.Fatalf("upload closed although its abort was refused: %v", err)
	}

	// The client's retry, with S3 answering again, closes the upload.
	success := checkStatusCode(http.StatusNoContent)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/multipart/complete",
		body:          completeBody,
		handler:       service.CompleteMultipartUploadHandler,
		checkResponse: &success,
	})

	// The tracking row is gone, so the upload must have been aborted: with
	// no row nothing could ever reap it.
	var rows int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM multipart_uploads WHERE upload_id = $1", uploadID).Scan(&rows))

	if rows != 0 {
		t.Errorf("multipart upload row still present after completion (rows=%d)", rows)
	}

	if _, err := coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10); err == nil {
		t.Error("errored multipart upload left open after its row was dropped")
	}
}
