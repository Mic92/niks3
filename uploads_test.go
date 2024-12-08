package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"
)

func TestServer_cleanupPendingClosuresHandler(t *testing.T) {
	t.Parallel()

	server := createTestServer(t)
	defer server.Close()

	// should be a no-op
	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/pending_closure?older-than=0s",
		handler: server.cleanupPendingClosuresHandler,
	})

	closureKey := "00000000000000000000000000000000"
	objects := []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	body, err := json.Marshal(map[string]interface{}{
		"closure": closureKey,
		"objects": objects,
	})
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/pending_closure",
		body:    body,
		handler: server.createPendingClosureHandler,
	})

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/pending_closure?older-than=0s",
		handler: server.cleanupPendingClosuresHandler,
	})

	var pendingClosureResponse PendingClosureResponse
	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse)
	ok(t, err)

	val := func(t *testing.T, rr *httptest.ResponseRecorder) {
		t.Helper()

		if rr.Code != http.StatusNotFound {
			t.Errorf("expected http status 404, got %d", rr.Code)
		}
	}
	rr = testRequest(t, &TestRequest{
		method:  "POST",
		path:    fmt.Sprintf("/pending_closure/%s/complete", pendingClosureResponse.ID),
		body:    body,
		handler: server.commitPendingClosureHandler,
		pathValues: map[string]string{
			"id": pendingClosureResponse.ID,
		},
		checkResponse: &val,
	})

	slog.Info("commit pending closure", "response", rr.Body.String(), "status", rr.Code)
}

func TestServer_createPendingClosureHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Parallel()

	server := createTestServer(t)
	defer server.Close()

	invalidBody, err := json.Marshal(map[string]interface{}{})
	ok(t, err)

	val := func(t *testing.T, rr *httptest.ResponseRecorder) {
		t.Helper()

		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected http status 400, got %d", rr.Code)
		}
	}

	testRequest(t, &TestRequest{
		method:        "POST",
		path:          "/pending_closure",
		body:          invalidBody,
		handler:       server.createPendingClosureHandler,
		checkResponse: &val,
	})

	closureKey := "00000000000000000000000000000000"
	firstObject := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	secondObject := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	objects := []string{firstObject, secondObject}
	body, err := json.Marshal(map[string]interface{}{
		"closure": closureKey,
		"objects": objects,
	})
	ok(t, err)

	rr := testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/pending_closure",
		body:    body,
		handler: server.createPendingClosureHandler,
	})

	var pendingClosureResponse PendingClosureResponse
	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse)
	ok(t, err)

	if pendingClosureResponse.ID == "" {
		t.Errorf("handler returned empty upload id")
	}

	if len(pendingClosureResponse.PendingObjects) != len(objects) {
		t.Errorf("expected %v, got %v", objects, pendingClosureResponse.PendingObjects)
	}

	_, err = server.minioClient.PutObject(ctx, server.bucketName, firstObject, nil, 0, minio.PutObjectOptions{})
	ok(t, err)

	_, err = server.minioClient.PutObject(ctx, server.bucketName, secondObject, nil, 0, minio.PutObjectOptions{})
	ok(t, err)

	testRequest(t, &TestRequest{
		method:  "POST",
		path:    fmt.Sprintf("/pending_closure/%s/complete", pendingClosureResponse.ID),
		body:    body,
		handler: server.commitPendingClosureHandler,
		pathValues: map[string]string{
			"id": pendingClosureResponse.ID,
		},
	})

	rr = testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/closures/" + closureKey,
		body:    body,
		handler: server.getClosureHandler,
		pathValues: map[string]string{
			"key": closureKey,
		},
	})

	var closureResponse ClosureResponse
	err = json.Unmarshal(rr.Body.Bytes(), &closureResponse)
	slog.Info("get closure", "response", rr.Body.String(), "status", rr.Code)
	ok(t, err)

	objects = closureResponse.Objects
	if len(objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(objects))
	}

	thirdObject := "cccccccccccccccccccccccccccccccc"

	objects2 := []string{firstObject, secondObject, thirdObject}
	body2, err := json.Marshal(map[string]interface{}{
		"closure": "11111111111111111111111111111111",
		"objects": objects2,
	})
	ok(t, err)

	rr = testRequest(t, &TestRequest{
		method:  "POST",
		path:    "/pending_closure",
		body:    body2,
		handler: server.createPendingClosureHandler,
	})

	ok(t, err)

	var pendingClosureResponse2 PendingClosureResponse
	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse2)
	ok(t, err)

	if len(pendingClosureResponse2.PendingObjects) != 1 || pendingClosureResponse2.PendingObjects[0] != thirdObject {
		t.Errorf("expected 1 object, got %v", pendingClosureResponse2)
	}

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/closures?older-than=0",
		handler: server.cleanupClosuresOlder,
	})

	isNotFound := func(t *testing.T, rr *httptest.ResponseRecorder) {
		t.Helper()

		if rr.Code != http.StatusNotFound {
			t.Errorf("expected http status 404, got %d (%s)", rr.Code, rr.Body.String())
		}
	}
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/closures/" + closureKey,
		body:          body,
		handler:       server.getClosureHandler,
		checkResponse: &isNotFound,
		pathValues: map[string]string{
			"key": closureKey,
		},
	})
}
