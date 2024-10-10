package main

import (
	"encoding/json"
	"log/slog"
	"testing"
)

func TestServer_startUploadHandler(t *testing.T) {
	server := createTestServer(t)
	defer server.Close()

	body, err := json.Marshal(map[string]interface{}{
		"closure_nar_hash": "00000000000000000000000000000000",
		"store_paths":      []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
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
