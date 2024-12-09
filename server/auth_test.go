package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestService_AuthMiddleware(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	// check that health check works also with database closed
	service.Pool.Close()

	service.APIToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: service.AuthMiddleware(service.HealthCheckHandler),
		header: map[string]string{
			"Authorization": "Bearer " + service.APIToken,
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
		handler: service.AuthMiddleware(service.HealthCheckHandler),
		// checkResponse *func(*testing.T, *httptest.ResponseRecorder)
		checkResponse: &checkResponse,
		header: map[string]string{
			"Authorization": "Bearer " + "wrongtoken",
		},
	})
}
