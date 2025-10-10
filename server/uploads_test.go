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
)

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
	narKey := "nar/" + closureHash + ".nar.zst"
	objects := []map[string]interface{}{
		{"key": closureKey, "refs": []string{narKey}},
		{"key": narKey, "refs": []string{}},
	}
	body, err := json.Marshal(map[string]interface{}{
		"closure": closureKey,
		"objects": objects,
	})
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/pending_closures",
		body:    body,
		handler: service.CreatePendingClosureHandler,
	})

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closures?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	var pendingClosureResponse server.PendingClosureResponse

	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse)
	ok(t, err)

	checkNotFound := checkStatusCode(http.StatusNotFound)
	testRequest(t, &TestRequest{
		method:  "POST",
		path:    fmt.Sprintf("/api/pending_closures/%s/complete", pendingClosureResponse.ID),
		body:    body,
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
	completedParts := make([]map[string]interface{}, 0, len(pendingObject.MultipartInfo.PartURLs))

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

		completedParts = append(completedParts, map[string]interface{}{
			"part_number": partNumber,
			"etag":        etag,
		})

		if err := resp.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}

	// Complete the multipart upload
	completeReq := map[string]interface{}{
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

	invalidBody, err := json.Marshal(map[string]interface{}{})
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
	narKey := "nar/" + closureHash + ".nar.zst"

	bodyBareClosure, err := json.Marshal(map[string]interface{}{
		"closure": closureHash,
		"objects": []map[string]interface{}{
			{"key": narinfoKey, "refs": []string{narKey}},
			{"key": narKey, "refs": []string{}},
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
	firstObject := closureKey + ".narinfo"           // This should be the narinfo file
	secondObject := "nar/" + closureKey + ".nar.zst" // This should be the NAR file
	objects := []map[string]interface{}{
		{"key": firstObject, "refs": []string{secondObject}}, // narinfo references the NAR file
		{"key": secondObject, "refs": []string{}},            // NAR file has no references
	}
	body, err := json.Marshal(map[string]interface{}{
		"closure": firstObject, // Send the narinfo key as closure key
		"objects": objects,
	})
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/pending_closures",
		body:    body,
		handler: service.CreatePendingClosureHandler,
	})

	var pendingClosureResponse server.PendingClosureResponse

	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse)
	ok(t, err)

	if pendingClosureResponse.ID == "" {
		t.Errorf("handler returned empty upload id")
	}

	if len(pendingClosureResponse.PendingObjects) != len(objects) {
		t.Errorf("expected %v, got %v", objects, pendingClosureResponse.PendingObjects)
	}

	for key, pendingObject := range pendingClosureResponse.PendingObjects {
		if pendingObject.MultipartInfo != nil {
			handleMultipartUpload(ctx, t, key, pendingObject, service)
		} else {
			handlePresignedUpload(ctx, t, pendingObject.PresignedURL)
		}
	}

	testRequest(t, &TestRequest{
		method:  "POST",
		path:    fmt.Sprintf("/api/pending_closures/%s/complete", pendingClosureResponse.ID),
		body:    body,
		handler: service.CommitPendingClosureHandler,
		pathValues: map[string]string{
			"id": pendingClosureResponse.ID,
		},
	})

	rr = testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/api/closures/" + closureKey,
		body:    body,
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

	objects2 := []map[string]interface{}{
		{"key": firstObject, "refs": []string{}},
		{"key": secondObject, "refs": []string{firstObject}},
		{"key": thirdObject, "refs": []string{secondObject}},
	}
	body2, err := json.Marshal(map[string]interface{}{
		"closure": "11111111111111111111111111111111.narinfo", // Send the narinfo key as closure key
		"objects": objects2,
	})
	ok(t, err)

	rr = testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/api/pending_closures",
		body:    body2,
		handler: service.CreatePendingClosureHandler,
	})

	ok(t, err)

	var pendingClosureResponse2 server.PendingClosureResponse

	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse2)
	ok(t, err)

	v, ok := pendingClosureResponse2.PendingObjects[thirdObject]
	if len(pendingClosureResponse2.PendingObjects) != 1 || !ok || v.PresignedURL == "" {
		t.Errorf("expected 1 object, got %v", pendingClosureResponse2)
	}

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/closures?older-than=0",
		handler: service.CleanupClosuresOlder,
	})

	checkNotFound2 := checkStatusCode(http.StatusNotFound)
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/api/closures/" + closureKey,
		body:          body,
		handler:       service.GetClosureHandler,
		checkResponse: &checkNotFound2,
		pathValues: map[string]string{
			"key": closureKey,
		},
	})
}
