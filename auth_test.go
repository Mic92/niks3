package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServer_authMiddleware(t *testing.T) {
	t.Parallel()

	server := createTestServer(t)
	defer server.Close()

	// check that health check works also with database closed
	server.pool.Close()

	server.apiToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: server.authMiddleware(server.healthCheckHandler),
		header: map[string]string{
			"Authorization": "Bearer " + server.apiToken,
		},
	})

	checkResponse := func(t *testing.T, w *httptest.ResponseRecorder) {
		t.Helper()

		if w.Code != http.StatusUnauthorized {
			t.Errorf("Expected status code %d, got %d", http.StatusUnauthorized, w.Code)
		}
	}

	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: server.authMiddleware(server.healthCheckHandler),
		// checkResponse *func(*testing.T, *httptest.ResponseRecorder)
		checkResponse: &checkResponse,
		header: map[string]string{
			"Authorization": "Bearer " + "wrongtoken",
		},
	})
}
