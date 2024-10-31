package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServer_startUploadHandler(t *testing.T) {
	server := createTestServer(t)
	defer server.Close()

	invalidBody, err := json.Marshal(map[string]interface{}{})
	ok(t, err)

	val := func(t *testing.T, rr *httptest.ResponseRecorder) {
		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected http status 400, got %d", rr.Code)
		}
	}

	testRequest(&TestRequest{
		method:        "POST",
		path:          "/pending_closure",
		body:          invalidBody,
		handler:       server.createPendingClosureHandler,
		checkResponse: &val,
	}, t)

	closureKey := "00000000000000000000000000000000"
	body, err := json.Marshal(map[string]interface{}{
		"closure": "00000000000000000000000000000000",
		"objects": []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
	})
	ok(t, err)

	rr := testRequest(&TestRequest{
		method:  "POST",
		path:    "/pending_closure",
		body:    body,
		handler: server.createPendingClosureHandler,
	}, t)

	var pendingClosureResponse PendingClosureResponse
	err = json.Unmarshal(rr.Body.Bytes(), &pendingClosureResponse)
	slog.Info("create pending closure", "response", rr.Body.String(), "status", rr.Code)
	ok(t, err)

	if pendingClosureResponse.ID == "" {
		t.Errorf("handler returned empty upload id")
	}

	testRequest(&TestRequest{
		method:  "POST",
		path:    fmt.Sprintf("/pending_closure/%s/complete", pendingClosureResponse.ID),
		body:    body,
		handler: server.commitPendingClosureHandler,
		pathValues: map[string]string{
			"id": pendingClosureResponse.ID,
		},
	}, t)

	rr = testRequest(&TestRequest{
		method:  "GET",
		path:    fmt.Sprintf("/closure/%s", closureKey),
		body:    body,
		handler: server.getClosureHandler,
		pathValues: map[string]string{
			"key": closureKey,
		},
	}, t)

	var closureResponse ClosureResponse
	err = json.Unmarshal(rr.Body.Bytes(), &closureResponse)
	slog.Info("get closure", "response", rr.Body.String(), "status", rr.Code)
	ok(t, err)

	objects := closureResponse.Objects
	if len(objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(objects))
	}
}
