package server

import (
	"testing"
)

func TestServer_healthCheckHandler(t *testing.T) {
	t.Parallel()

	server := createTestServer(t)
	defer server.Close()

	// check that health check works also with database closed
	server.pool.Close()

	testRequest(t, &TestRequest{
		method:  "GET",
		path:    "/health",
		handler: server.healthCheckHandler,
	})
}
