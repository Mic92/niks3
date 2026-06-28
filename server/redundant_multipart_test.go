package server_test

import (
	"context"
	"encoding/json"
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
