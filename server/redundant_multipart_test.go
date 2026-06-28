package server_test

import (
	"context"
	"encoding/json"
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

		body, err := json.Marshal(map[string]any{
			"closure": narinfoKey,
			"objects": []map[string]any{
				{"key": narinfoKey, "type": "narinfo", "refs": []string{sharedNarKey}},
				{"key": sharedNarKey, "type": "nar", "refs": []string{}, "nar_size": 100 * 1024 * 1024},
			},
		})
		ok(t, err)

		rr := testRequest(t, &TestRequest{
			method:  "POST",
			path:    "/api/pending_closures",
			body:    body,
			handler: service.CreatePendingClosureHandler,
		})

		var resp server.PendingClosureResponse
		ok(t, json.Unmarshal(rr.Body.Bytes(), &resp))

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
}

// TestCompleteMultipartUpload_ErrorButObjectExists verifies that a failing
// CompleteMultipartUpload is treated as success when the object is already
// present in S3, covering lost responses and retried completions that return
// NoSuchUpload.
func TestCompleteMultipartUpload_ErrorButObjectExists(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	service := createTestService(t)
	defer service.Close()

	narKey := narKeyFor("gggggggggggggggggggggggggggggg03")
	narinfoKey := "gggggggggggggggggggggggggggggg30.narinfo"

	body, err := json.Marshal(map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 100 * 1024 * 1024},
		},
	})
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/pending_closures",
		body:    body,
		handler: service.CreatePendingClosureHandler,
	})

	var resp server.PendingClosureResponse
	ok(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	uploadID := resp.PendingObjects[narKey].MultipartInfo.UploadID

	// The blob already landed in S3 (e.g. a prior completion whose response was
	// lost), but the multipart upload row is still registered.
	_, err = service.MinioClient.PutObject(ctx, service.Bucket, narKey,
		strings.NewReader("payload"), -1, minio.PutObjectOptions{})
	ok(t, err)

	// Completing with a bogus part fails in S3, yet the object exists, so the
	// handler must report success.
	completeBody, err := json.Marshal(map[string]any{
		"object_key": narKey,
		"upload_id":  uploadID,
		"parts":      []map[string]any{{"part_number": 1, "etag": "deadbeef"}},
	})
	ok(t, err)

	success := checkStatusCode(http.StatusNoContent)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/multipart/complete",
		body:          completeBody,
		handler:       service.CompleteMultipartUploadHandler,
		checkResponse: &success,
	})
}
