package server_test

import (
	"context"
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
}
