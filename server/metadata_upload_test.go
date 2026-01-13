package server_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server"
)

// TestNARDeduplicationMetadataUploadBug tests that when uploading a store path with
// the same content as an already-uploaded path, the new narinfo and .ls files are uploaded
// even though the NAR is deduplicated.
//
// This is the COMMON case where the bug occurs - NAR deduplication during normal uploads.
// When two store paths have identical content (same NAR hash), the second upload should:
// 1. Reuse the existing NAR file (deduplication)
// 2. Upload new narinfo and .ls files for the second path
//
// Bug: Client only queues NAR tasks, so metadata-only uploads are skipped.
func TestNARDeduplicationMetadataUploadBug(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	testService := &server.Service{
		Pool:          service.Pool,
		MinioClient:   service.MinioClient,
		Bucket:        service.Bucket,
		APIToken:      testAuthToken,
		S3Concurrency: 100,
	}

	err := testService.InitializeBucket(t.Context())
	ok(t, err)

	mux := http.NewServeMux()
	registerTestHandlers(mux, testService)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	ctx := t.Context()
	nixEnv := setupIsolatedNixStore(t)

	// Create first file with specific content
	tempFile1 := filepath.Join(t.TempDir(), "file1.txt")
	content := []byte("identical content for NAR deduplication test")
	err = os.WriteFile(tempFile1, content, 0o600)
	ok(t, err)

	// Upload first path
	storePath1 := nixStoreAdd(t, nixEnv, tempFile1)
	t.Logf("First store path: %s", storePath1)

	err = pushToServer(ctx, ts.URL, testAuthToken, []string{storePath1}, nixEnv)
	ok(t, err)

	hash1 := strings.Split(filepath.Base(storePath1), "-")[0]
	verifyNarinfoInS3(ctx, t, testService, hash1, storePath1)
	verifyLsFileInS3(ctx, t, testService, hash1)

	// Create second file with IDENTICAL content (will have same NAR hash)
	tempFile2 := filepath.Join(t.TempDir(), "file2.txt")
	err = os.WriteFile(tempFile2, content, 0o600)
	ok(t, err)

	// Upload second path - NAR should be deduplicated, but narinfo/ls must still be uploaded
	storePath2 := nixStoreAdd(t, nixEnv, tempFile2)
	t.Logf("Second store path (same content): %s", storePath2)

	// Verify they have different hashes (different store paths)
	hash2 := strings.Split(filepath.Base(storePath2), "-")[0]
	if hash1 == hash2 {
		t.Fatal("Store paths should have different hashes despite same content")
	}

	err = pushToServer(ctx, ts.URL, testAuthToken, []string{storePath2}, nixEnv)
	ok(t, err)

	// Verify second path's narinfo and .ls were uploaded (this will fail due to the bug)
	verifyNarinfoInS3(ctx, t, testService, hash2, storePath2)
	verifyLsFileInS3(ctx, t, testService, hash2)
}
