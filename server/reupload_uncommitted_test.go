package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// TestCompletedNarNotReofferedAcrossClosures is a regression test for a NAR that
// has been fully uploaded to S3 (its multipart upload completed) but whose
// closure was never committed -- exactly what happens in CI when one object in
// a batch fails (or the push process is killed) after a sibling NAR already
// finished uploading.
//
// CompleteMultipartUploadHandler finalizes the object in S3 and drops its
// multipart_uploads tracking row, but the object is only recorded in the
// `objects` table at closure commit (commit_pending_closure). So a completed-
// but-uncommitted NAR stays present in S3 yet is treated as missing forever:
// every later closure re-opens a fresh multipart upload for it. When the client
// then notices the object already exists and skips, that fresh multipart is
// abandoned with zero parts and leaks. Over many CI runs this produces
// thousands of orphaned zero-part multipart uploads and needless re-uploads,
// and it prevents the affected path from being served from the cache.
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

	// Deliberately do NOT commit the first closure: this models a batch where a
	// sibling object failed (or the push was killed) after this NAR uploaded, so
	// PushPaths returned before reaching CompletePendingClosure.

	// A later build pushes a different closure that references the same NAR
	// content. The NAR is already in S3, so the server must not ask the client
	// to upload it again.
	second := createPendingClosure(t, service, closureFor(secondNarinfo))

	if reoffered, ok := second.PendingObjects[narKey]; ok {
		t.Errorf("server re-offered an upload for a NAR already present in S3 "+
			"(multipart=%v, presigned=%q); every such re-offer that the client "+
			"then skips leaks an orphaned zero-part multipart upload",
			reoffered.MultipartInfo != nil, reoffered.PresignedURL)
	}
}
