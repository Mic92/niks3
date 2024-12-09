package server_test

import (
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
