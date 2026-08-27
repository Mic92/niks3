package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestService_healthCheckHandler(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	// check that health check works also with database closed
	service.Pool.Close()

	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: service.HealthCheckHandler,
	})
}

func TestService_readinessHandler(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/readyz",
		handler: service.ReadinessHandler,
	})

	service.Pool.Close()

	notReady := func(t *testing.T, rr *httptest.ResponseRecorder) {
		t.Helper()

		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("expected 503 with closed pool, got %d: %s", rr.Code, rr.Body.String())
		}
	}

	testRequest(t, &TestRequest{
		method:        "GET",
		path:          "/readyz",
		handler:       service.ReadinessHandler,
		checkResponse: &notReady,
	})
}
