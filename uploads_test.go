package main

import (
	"encoding/json"
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
		path:          "/upload",
		body:          invalidBody,
		handler:       server.startUploadHandler,
		checkResponse: &val,
	}, t)

	body, err := json.Marshal(map[string]interface{}{
		"closure": "00000000000000000000000000000000",
		"objects": []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
	})
	ok(t, err)

	rr := testRequest(&TestRequest{
		method:  "POST",
		path:    "/upload",
		body:    body,
		handler: server.startUploadHandler,
	}, t)

	var uploadResponse UploadResponse
	err = json.Unmarshal(rr.Body.Bytes(), &uploadResponse)
	slog.Info("upload response", "response", rr.Body.String(), "status", rr.Code)
	ok(t, err)
	if uploadResponse.ID == "" {
		t.Errorf("handler returned empty upload id")
	}
}
