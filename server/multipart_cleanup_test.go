package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestMultipartCleanup(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Create a pending closure with multipart upload
	closureHash := "dadb44fdadb44fdadb44fdadb44f0000"
	closureKey := closureHash + ".narinfo"
	narKey := narKeyFor(closureHash)

	// Use a large NarSize to ensure multipart upload is used
	largeNarSize := uint64(100 * 1024 * 1024) // 100MB

	objects := []map[string]any{
		{"key": closureKey, "type": "narinfo", "refs": []string{narKey}},
		{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": largeNarSize},
	}

	// Create pending closure (this initiates multipart upload)
	pendingClosureResponse := createPendingClosure(t, service, map[string]any{
		"closure": closureKey,
		"objects": objects,
	})

	// Verify that we got a multipart upload
	narPendingObject, exists := pendingClosureResponse.PendingObjects[narKey]
	if !exists {
		t.Fatalf("NAR object not found in pending objects")
	}

	if narPendingObject.MultipartInfo == nil {
		t.Fatalf("Expected multipart upload for NAR file, got presigned URL instead")
	}

	uploadID := narPendingObject.MultipartInfo.UploadID
	if uploadID == "" {
		t.Fatalf("Upload ID is empty")
	}

	// Verify the upload exists in S3
	coreClient := minio.Core{Client: service.MinioClient}
	_, err := coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10)
	ok(t, err) // Should not error if upload exists

	// Don't complete the upload - simulate an abandoned upload

	// Wait a bit to ensure the cleanup will catch it
	time.Sleep(100 * time.Millisecond)

	// While S3 is unreachable the abort fails. The closure and its tracking
	// row must survive the cleanup: the row is the only handle on the upload,
	// and without it the parts would sit in S3 for good.
	good := service.MinioClient
	service.MinioClient = brokenS3Client(t)

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	service.MinioClient = good

	var rows int
	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM multipart_uploads WHERE upload_id = $1", uploadID).Scan(&rows))

	if rows != 1 {
		t.Fatalf("multipart upload row dropped although the abort failed (rows=%d)", rows)
	}

	_, err = coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10)
	ok(t, err) // still live: nothing aborted it

	// Call cleanup with 0 duration (cleans everything)
	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	// Verify the upload was aborted in S3
	_, err = coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10)
	if err == nil {
		t.Error("Expected error when listing parts of aborted upload, but got none")
	}

	ok(t, service.Pool.QueryRow(ctx, "SELECT count(*) FROM pending_closures").Scan(&rows))

	if rows != 0 {
		t.Errorf("%d pending closure(s) left after a successful cleanup", rows)
	}
}

// brokenS3Client returns a client whose every request fails: it points at a
// closed port.
func brokenS3Client(t *testing.T) *minio.Client {
	t.Helper()

	bad, err := minio.New("127.0.0.1:1", &minio.Options{Creds: credentials.NewStaticV4("a", "b", ""), MaxRetries: 1})
	ok(t, err)

	return bad
}
