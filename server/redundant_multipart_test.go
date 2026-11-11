package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Mic92/niks3/internal/testwait"
	"github.com/Mic92/niks3/server"
	"github.com/minio/minio-go/v7"
)

// TestSharedNarBothUploadsComplete drives two pending closures that share a
// NAR through the multipart path. Completing one must leave the other's upload
// alone: on Garage an abort racing a completion deletes the completed object.
func TestSharedNarBothUploadsComplete(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), testwait.Hung)
	defer cancel()

	service := createTestService(t)
	defer service.Close()

	// Unique nix-base32 hashes (no e/o/t/u) to avoid collisions under -parallel.
	sharedNarKey := narKeyFor("gggggggggggggggggggggggggggggg01")

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

	first := postSharedClosure("gggggggggggggggggggggggggggggg10.narinfo")
	second := postSharedClosure("gggggggggggggggggggggggggggggg20.narinfo")

	handleMultipartUpload(ctx, t, sharedNarKey, first, service)

	coreClient := minio.Core{Client: service.MinioClient}
	_, err := coreClient.ListObjectParts(ctx, service.Bucket, sharedNarKey, second.MultipartInfo.UploadID, 0, 10)
	ok(t, err)

	handleMultipartUpload(ctx, t, sharedNarKey, second, service)

	_, err = service.MinioClient.StatObject(ctx, service.Bucket, sharedNarKey, minio.StatObjectOptions{})
	ok(t, err)
}

// TestCompleteMultipartUpload_ErrorButObjectExists verifies that a failing
// CompleteMultipartUpload is treated as success when the object is already
// present in S3, covering lost responses and retried completions that return
// NoSuchUpload.
func TestCompleteMultipartUpload_ErrorButObjectExists(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), testwait.Hung)
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

	// The row is gone, so the upload must have been aborted.
	var rows int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM multipart_uploads WHERE upload_id = $1", uploadID).Scan(&rows))

	if rows != 0 {
		t.Errorf("multipart upload row still present after completion (rows=%d)", rows)
	}

	coreClient := minio.Core{Client: service.MinioClient}
	if _, err := coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10); err == nil {
		t.Error("errored multipart upload left open after its row was dropped")
	}
}
