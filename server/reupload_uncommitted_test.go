package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// Regression test: a NAR whose multipart upload completed but whose closure
// never committed (sibling object failed, push killed) must not be re-offered
// for upload by later closures, since each re-offer leaks an orphaned
// zero-part multipart upload.
func TestCompletedNarNotReofferedAcrossClosures(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	service := createTestService(t)
	defer service.Close()

	// Unique nix-base32 hashes (alphabet excludes e/o/t/u) to avoid collisions
	// with other -parallel tests sharing the bucket.
	narKey := narKeyFor("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhh01")
	firstNarinfo := "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhh10.narinfo"
	secondNarinfo := "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhh20.narinfo"

	closureFor := func(narinfoKey string) map[string]any {
		return map[string]any{
			"closure": narinfoKey,
			"objects": []map[string]any{
				{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
				{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 100 * 1024 * 1024},
			},
		}
	}

	// First closure: the server opens a multipart upload for the NAR.
	first := createPendingClosure(t, service, closureFor(firstNarinfo))

	nar := first.PendingObjects[narKey]
	if nar.MultipartInfo == nil {
		t.Fatalf("expected a multipart upload for the NAR on the first closure")
	}

	// Upload and complete the NAR: the object now exists in S3.
	handleMultipartUpload(ctx, t, narKey, nar, service)

	// Confirm the NAR is really in S3.
	if _, err := service.MinioClient.StatObject(ctx, service.Bucket, narKey, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("NAR should be present in S3 after completing its multipart upload: %v", err)
	}

	// Deliberately do NOT commit the first closure. A second closure referencing
	// the same NAR must not be asked to upload it again.
	second := createPendingClosure(t, service, closureFor(secondNarinfo))

	if reoffered, ok := second.PendingObjects[narKey]; ok {
		t.Errorf("NAR already in S3 was re-offered for upload (multipart=%v, presigned=%q)",
			reoffered.MultipartInfo != nil, reoffered.PresignedURL)
	}

	// The registration must carry the client-reported size for object_stats.
	var size *int64
	if err := service.Pool.QueryRow(ctx, "SELECT size FROM objects WHERE key = $1", narKey).Scan(&size); err != nil {
		t.Fatalf("failed to query registered object: %v", err)
	}

	if size == nil || *size != 100*1024*1024 {
		t.Errorf("registered object size = %v, want %d", size, 100*1024*1024)
	}
}

// A presigned (non-multipart) upload followed by POST /api/uploads/complete
// must also be recorded, so an uncommitted closure does not cause re-uploads.
func TestPresignedUploadRegisteredBeforeCommit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	service := createTestService(t)
	defer service.Close()

	narKey := narKeyFor("jjjjjjjjjjjjjjjjjjjjjjjjjjjjjj01")
	firstNarinfo := "jjjjjjjjjjjjjjjjjjjjjjjjjjjjjj10.narinfo"
	secondNarinfo := "jjjjjjjjjjjjjjjjjjjjjjjjjjjjjj20.narinfo"

	closureFor := func(narinfoKey string) map[string]any {
		return map[string]any{
			"closure": narinfoKey,
			"objects": []map[string]any{
				{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
				{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 1024},
			},
		}
	}

	first := createPendingClosure(t, service, closureFor(firstNarinfo))

	nar := first.PendingObjects[narKey]
	if nar.PresignedURL == "" {
		t.Fatalf("expected a presigned URL for a small NAR")
	}

	handlePresignedUpload(ctx, t, nar.PresignedURL)

	// Registering an unknown key must be rejected.
	body, err := json.Marshal(map[string]any{"object_key": narKeyFor("jjjjjjjjjjjjjjjjjjjjjjjjjjjjjj99")})
	if err != nil {
		t.Fatal(err)
	}

	checkNotFound := checkStatusCode(http.StatusNotFound)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/uploads/complete",
		body:          body,
		handler:       service.CompleteUploadHandler,
		checkResponse: &checkNotFound,
	})

	body, err = json.Marshal(map[string]any{"object_key": narKey})
	if err != nil {
		t.Fatal(err)
	}

	testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/uploads/complete",
		body:    body,
		handler: service.CompleteUploadHandler,
	})

	// First closure never commits; a second closure must not re-offer the NAR.
	second := createPendingClosure(t, service, closureFor(secondNarinfo))
	if reoffered, ok := second.PendingObjects[narKey]; ok {
		t.Errorf("NAR already in S3 was re-offered for upload (presigned=%q)", reoffered.PresignedURL)
	}
}
