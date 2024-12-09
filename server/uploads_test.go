package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
)

func TestService_cleanupPendingClosuresHandler(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	// should be a no-op
	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closure?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
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
		path:    "/api/pending_closure",
		body:    body,
		handler: service.CreatePendingClosureHandler,
	})

	testRequest(t, &TestRequest{
		method:  "DELETE",
		path:    "/api/pending_closure?older-than=0s",
		handler: service.CleanupPendingClosuresHandler,
	})

	var pendingClosureResponse server.PendingClosureResponse
	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse)
	ok(t, err)

	val := func(t *testing.T, rr *httptest.ResponseRecorder) {
		t.Helper()

		if rr.Code != http.StatusNotFound {
			t.Errorf("expected http status 404, got %d", rr.Code)
		}
	}
	testRequest(t, &TestRequest{
		method:  "POST",
		path:    fmt.Sprintf("/api/pending_closure/%s/complete", pendingClosureResponse.ID),
		body:    body,
		handler: service.CommitPendingClosureHandler,
		pathValues: map[string]string{
			"id": pendingClosureResponse.ID,
		},
		checkResponse: &val,
	})
}

func TestService_createPendingClosureHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Parallel()

	service := createTestService(t)
	defer service.Close()

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
		path:          "/api/pending_closure",
		body:          invalidBody,
		handler:       service.CreatePendingClosureHandler,
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
		path:    "/api/pending_closure",
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

	httpClient := &http.Client{}

	for _, pendingObject := range pendingClosureResponse.PendingObjects {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, pendingObject.PresignedURL, nil)
		ok(t, err)

		resp, err := httpClient.Do(req)
		ok(t, err)

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected http status 200, got %d", resp.StatusCode)
		}
	}

	testRequest(t, &TestRequest{
		method:  "POST",
		path:    fmt.Sprintf("/api/pending_closure/%s/complete", pendingClosureResponse.ID),
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
			"key": closureKey,
		},
	})

	var closureResponse server.ClosureResponse
	err = json.Unmarshal(rr.Body.Bytes(), &closureResponse)
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
		path:    "/api/pending_closure",
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

	isNotFound := func(t *testing.T, rr *httptest.ResponseRecorder) {
		t.Helper()

		if rr.Code != http.StatusNotFound {
			t.Errorf("expected http status 404, got %d (%s)", rr.Code, rr.Body.String())
		}
	}
	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/api/closures/" + closureKey,
		body:          body,
		handler:       service.GetClosureHandler,
		checkResponse: &isNotFound,
		pathValues: map[string]string{
			"key": closureKey,
		},
	})
}
