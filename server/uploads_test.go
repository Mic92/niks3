package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	"github.com/minio/minio-go/v7"
)

const (
	objectTypeNarinfo = "narinfo"
)

// createPendingClosure posts a pending-closure request and returns the
// decoded response.
func createPendingClosure(t *testing.T, service *server.Service, request map[string]any) server.PendingClosureResponse {
	t.Helper()

	body, err := json.Marshal(request)
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/pending_closures",
		body:    body,
		handler: service.CreatePendingClosureHandler,
	})

	var resp server.PendingClosureResponse

	ok(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	return resp
}

// commitPendingClosure completes a pending closure with the given narinfo
// metadata.
func commitPendingClosure(t *testing.T, service *server.Service, id string, narinfos map[string]map[string]any) {
	t.Helper()

	body, err := json.Marshal(map[string]any{"narinfos": narinfos})
	ok(t, err)

	testRequest(t, &TestRequest{
		method:  "POST",
		path:    fmt.Sprintf("/api/pending_closures/%s/complete", id),
		body:    body,
		handler: service.CommitPendingClosureHandler,
		pathValues: map[string]string{
			"id": id,
		},
	})
}

// uploadPendingObjects uploads all pending objects (multipart or presigned)
// and returns the narinfo metadata needed to complete the closure.
func uploadPendingObjects(ctx context.Context, t *testing.T, service *server.Service, resp server.PendingClosureResponse, closureHash, narKey string) map[string]map[string]any {
	t.Helper()

	narinfoMetadata := make(map[string]map[string]any)

	for key, pendingObject := range resp.PendingObjects {
		switch {
		case pendingObject.Type == objectTypeNarinfo:
			// Narinfo is handled server-side - collect metadata instead
			narinfoMetadata[key] = map[string]any{
				"store_path":  "/nix/store/" + closureHash + "-test-package",
				"url":         narKey,
				"compression": "zstd",
				"nar_hash":    "sha256:0000000000000000000000000000000000000000000000000000",
				"nar_size":    1000,
				"file_hash":   "sha256:1111111111111111111111111111111111111111111111111111",
				"file_size":   500,
				"references":  []string{},
			}
		case pendingObject.MultipartInfo != nil:
			handleMultipartUpload(ctx, t, key, pendingObject, service)
		default:
			handlePresignedUpload(ctx, t, pendingObject.PresignedURL)
		}
	}

	return narinfoMetadata
}

// checkStatusCode returns a checkResponse function that validates the expected status code.
func checkStatusCode(expectedStatus int) func(*testing.T, *httptest.ResponseRecorder) {
	return func(t *testing.T, rr *httptest.ResponseRecorder) {
		t.Helper()

		if rr.Code != expectedStatus {
			t.Errorf("expected http status %d, got %d (%s)", expectedStatus, rr.Code, rr.Body.String())
		}
	}
}

func TestService_cleanupPendingClosuresHandler(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	// should be a no-op
	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	closureHash := "00000000000000000000000000000000"
	closureKey := closureHash + ".narinfo"
	narKey := narKeyFor(closureHash)
	pendingClosureResponse := createPendingClosure(t, service, map[string]any{
		"closure": closureKey,
		"objects": []map[string]any{
			{"key": closureKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}},
		},
	})

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	// Try to complete the cleaned up closure - should fail with not found
	emptyNarinfos, err := json.Marshal(map[string]any{
		"narinfos": map[string]any{},
	})
	ok(t, err)

	checkNotFound := checkStatusCode(http.StatusNotFound)
	testRequest(t, &TestRequest{
		method:  "POST",
		path:    fmt.Sprintf("/api/pending_closures/%s/complete", pendingClosureResponse.ID),
		body:    emptyNarinfos,
		handler: service.CommitPendingClosureHandler,
		pathValues: map[string]string{
			"id": pendingClosureResponse.ID,
		},
		checkResponse: &checkNotFound,
	})
}

// handleMultipartUpload handles uploading a multipart object for testing.
func handleMultipartUpload(ctx context.Context, t *testing.T, key string, pendingObject server.PendingObject, service *server.Service) {
	t.Helper()

	httpClient := &http.Client{}
	completedParts := make([]map[string]any, 0, len(pendingObject.MultipartInfo.PartURLs))

	// Create dummy data that meets S3 minimum part size (5MB)
	minPartSize := 5 * 1024 * 1024 // 5MB

	dummyData := make([]byte, minPartSize)
	for i := range dummyData {
		dummyData[i] = byte(i % 256)
	}

	for i, partURL := range pendingObject.MultipartInfo.PartURLs {
		partNumber := i + 1

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, partURL, bytes.NewReader(dummyData))
		ok(t, err)

		resp, err := httpClient.Do(req)
		ok(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected http status 200 for part %d, got %d", partNumber, resp.StatusCode)
		}

		// Get ETag from response
		etag := resp.Header.Get("ETag")
		if etag == "" {
			t.Errorf("no ETag in response for part %d", partNumber)
		}

		// Remove quotes from ETag if present
		etag = strings.Trim(etag, "\"")

		completedParts = append(completedParts, map[string]any{
			"part_number": partNumber,
			"etag":        etag,
		})

		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}

	// Complete the multipart upload
	completeReq := map[string]any{
		"object_key": key,
		"upload_id":  pendingObject.MultipartInfo.UploadID,
		"parts":      completedParts,
	}

	completeBody, err := json.Marshal(completeReq)
	ok(t, err)

	// Use testRequest to properly call the handler
	//nolint:contextcheck // testRequest is a test helper that doesn't accept context
	testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/multipart/complete",
		body:    completeBody,
		handler: service.CompleteMultipartUploadHandler,
	})
}

// handlePresignedUpload handles uploading to a presigned URL for testing.
func handlePresignedUpload(ctx context.Context, t *testing.T, presignedURL string) {
	t.Helper()

	httpClient := &http.Client{}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, nil)
	ok(t, err)

	resp, err := httpClient.Do(req)
	ok(t, err)

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected http status 200, got %d", resp.StatusCode)
	}
}

func TestService_createPendingClosureHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	invalidBody, err := json.Marshal(map[string]any{})
	ok(t, err)

	checkBadRequest := checkStatusCode(http.StatusBadRequest)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/pending_closures",
		body:          invalidBody,
		handler:       service.CreatePendingClosureHandler,
		checkResponse: &checkBadRequest,
	})

	checkBareClosure := checkStatusCode(http.StatusBadRequest)
	closureHash := "ffffffffffffffffffffffffffffffff"
	narinfoKey := closureHash + ".narinfo"
	narKey := narKeyFor(closureHash)

	bodyBareClosure, err := json.Marshal(map[string]any{
		"closure": closureHash,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}},
		},
	})
	ok(t, err)

	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/pending_closures",
		body:          bodyBareClosure,
		handler:       service.CreatePendingClosureHandler,
		checkResponse: &checkBareClosure,
	})

	closureKey := "00000000000000000000000000000000"
	firstObject := closureKey + ".narinfo" // This should be the narinfo file
	secondObject := narKeyFor(closureKey)  // This should be the NAR file
	objects := []map[string]any{
		{"key": firstObject, "type": "narinfo", "refs": []string{secondObject}}, // narinfo references the NAR file
		{"key": secondObject, "type": "nar", "refs": []string{}},                // NAR file has no references
	}
	pendingClosureResponse := createPendingClosure(t, service, map[string]any{
		"closure": firstObject, // Send the narinfo key as closure key
		"objects": objects,
	})

	if pendingClosureResponse.ID == "" {
		t.Errorf("handler returned empty upload id")
	}

	if len(pendingClosureResponse.PendingObjects) != len(objects) {
		t.Errorf("expected %v, got %v", objects, pendingClosureResponse.PendingObjects)
	}

	narinfoMetadata := uploadPendingObjects(ctx, t, service, pendingClosureResponse, closureKey, narKeyFor(closureKey))
	commitPendingClosure(t, service, pendingClosureResponse.ID, narinfoMetadata)

	rr := testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/api/closures/" + closureKey,
		handler: service.GetClosureHandler,
		pathValues: map[string]string{
			"key": firstObject, // Use the narinfo key for the closure
		},
	})

	var closureResponse server.ClosureResponse

	err = json.Unmarshal(rr.Body.Bytes(), &closureResponse)
	ok(t, err)

	if len(closureResponse.Objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(closureResponse.Objects))
	}

	thirdObject := "cccccccccccccccccccccccccccccccc.narinfo"

	pendingClosureResponse2 := createPendingClosure(t, service, map[string]any{
		"closure": "11111111111111111111111111111111.narinfo", // Send the narinfo key as closure key
		"objects": []map[string]any{
			{"key": firstObject, "type": "narinfo", "refs": []string{}},
			{"key": secondObject, "type": "nar", "refs": []string{firstObject}},
			{"key": thirdObject, "type": "narinfo", "refs": []string{secondObject}},
		},
	})

	// Should only return the one new narinfo object with presigned URL
	if len(pendingClosureResponse2.PendingObjects) != 1 {
		t.Errorf("expected 1 object, got %d", len(pendingClosureResponse2.PendingObjects))
	}

	if v, ok := pendingClosureResponse2.PendingObjects[thirdObject]; !ok {
		t.Errorf("expected thirdObject in response")
	} else if v.Type != objectTypeNarinfo || v.PresignedURL == "" {
		t.Errorf("expected narinfo with presigned URL, got type=%s url=%s", v.Type, v.PresignedURL)
	}

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/closures?older-than=0",
		handler: service.CleanupClosuresOlder,
	})

	waitForGC(t, service)

	checkNotFound2 := checkStatusCode(http.StatusNotFound)
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/api/closures/" + closureKey,
		handler:       service.GetClosureHandler,
		checkResponse: &checkNotFound2,
		pathValues: map[string]string{
			"key": closureKey,
		},
	})
}

func TestService_verifyS3Integrity(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	// Step 1: Upload a closure
	closureKey := "dadb44fdadb44fdadb44fdadb44f0000"
	narinfoKey := closureKey + ".narinfo"
	narKey := narKeyFor(closureKey)
	objects := []map[string]any{
		{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
		{"key": narKey, "type": "nar", "refs": []string{}},
	}
	request := map[string]any{
		"closure": narinfoKey,
		"objects": objects,
	}
	pendingClosureResponse := createPendingClosure(t, service, request)

	narinfoMetadata := uploadPendingObjects(ctx, t, service, pendingClosureResponse, closureKey, narKey)
	commitPendingClosure(t, service, pendingClosureResponse.ID, narinfoMetadata)

	// Step 2: Delete the narinfo from S3 to simulate the bug
	err := service.MinioClient.RemoveObject(ctx, service.Bucket, narinfoKey, minio.RemoveObjectOptions{})
	ok(t, err)

	// Step 3: Try to upload the same closure WITHOUT verify_s3 - should skip upload
	responseWithoutVerify := createPendingClosure(t, service, request)

	// Should have no pending objects because DB thinks they exist
	if len(responseWithoutVerify.PendingObjects) != 0 {
		t.Errorf("expected 0 pending objects without verify_s3, got %d", len(responseWithoutVerify.PendingObjects))
	}

	// Step 4: Try again WITH verify_s3=true - should detect missing object
	responseWithVerify := createPendingClosure(t, service, map[string]any{
		"closure":   narinfoKey,
		"objects":   objects,
		"verify_s3": true,
	})

	// Should detect the missing narinfo and return it as a pending object
	if len(responseWithVerify.PendingObjects) != 1 {
		t.Errorf("expected 1 pending object with verify_s3, got %d", len(responseWithVerify.PendingObjects))
	}

	if _, exists := responseWithVerify.PendingObjects[narinfoKey]; !exists {
		t.Errorf("expected narinfo %s to be in pending objects", narinfoKey)
	}
}

// TestCompleteMultipartUnregistered ensures complete refuses an upload that
// was never registered, so clients cannot finalize multipart uploads outside
// the pending-closure book-keeping.
func TestCompleteMultipartUnregistered(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	body, err := json.Marshal(map[string]any{
		"object_key": narKeyFor("00000000000000000000000000000000"),
		"upload_id":  "does-not-exist",
		"parts":      []map[string]any{{"part_number": 1, "etag": "x"}},
	})
	ok(t, err)

	checkNotFound := checkStatusCode(http.StatusNotFound)
	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/api/multipart/complete",
		body:          body,
		handler:       service.CompleteMultipartUploadHandler,
		checkResponse: &checkNotFound,
	})
}

// TestCreatePendingClosure_SmallNARUsesSimplePUT ensures NARs at or below the
// multipart part size get a single presigned PUT URL instead of a multipart
// upload, which saves S3 API calls (relevant for providers with strict rate
// limits, e.g. Backblaze B2).
func TestCreatePendingClosure_SmallNARUsesSimplePUT(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	closureHash := "dddddddddddddddddddddddddddddd01"
	narinfoKey := closureHash + ".narinfo"
	smallNarKey := narKeyFor(closureHash)
	largeNarKey := narKeyFor("dddddddddddddddddddddddddddddd02")

	resp := createPendingClosure(t, service, map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{smallNarKey, largeNarKey}},
			{"key": smallNarKey, "type": "nar", "refs": []string{}, "nar_size": 1024},
			{"key": largeNarKey, "type": "nar", "refs": []string{}, "nar_size": 20 * 1024 * 1024},
		},
	})

	small := resp.PendingObjects[smallNarKey]
	if small.MultipartInfo != nil {
		t.Errorf("small NAR should not use multipart upload")
	}

	if small.PresignedURL == "" {
		t.Errorf("small NAR should have a presigned URL")
	}

	large := resp.PendingObjects[largeNarKey]
	if large.MultipartInfo == nil {
		t.Errorf("large NAR should use multipart upload")
	}
}
