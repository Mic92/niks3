package server_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/minio/minio-go/v7"
)

func TestMultipartCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	// Create a pending closure with multipart upload
	closureHash := "deadbeefdeadbeefdeadbeefdeadbeef"
	closureKey := closureHash + ".narinfo"
	narKey := "nar/" + closureHash + ".nar.zst"

	// Use a large NarSize to ensure multipart upload is used
	largeNarSize := uint64(100 * 1024 * 1024) // 100MB

	objects := []map[string]any{
		{"key": closureKey, "type": "narinfo", "refs": []string{narKey}},
		{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": largeNarSize},
	}

	body, err := json.Marshal(map[string]any{
		"closure": closureKey,
		"objects": objects,
	})
	ok(t, err)

	// Create pending closure (this initiates multipart upload)
	rr := testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/pending_closures",
		body:    body,
		handler: service.CreatePendingClosureHandler,
	})

	var pendingClosureResponse server.PendingClosureResponse

	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse)
	ok(t, err)

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
	_, err = coreClient.ListObjectParts(ctx, service.Bucket, narKey, uploadID, 0, 10)
	ok(t, err) // Should not error if upload exists

	// Don't complete the upload - simulate an abandoned upload

	// Wait a bit to ensure the cleanup will catch it
	time.Sleep(100 * time.Millisecond)

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
}
